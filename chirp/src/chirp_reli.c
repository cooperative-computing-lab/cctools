/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"
#include "chirp_protocol.h"
#include "chirp_client.h"

#include "macros.h"
#include "debug.h"
#include "full_io.h"
#include "sleeptools.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "list.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

#define MIN_DELAY 1
#define MAX_DELAY 60

struct chirp_file {
	char host[CHIRP_LINE_MAX];
	char path[CHIRP_LINE_MAX];
	struct chirp_stat info;
	INT64_T fd;
	INT64_T flags;
	INT64_T mode;
	INT64_T serial;
	INT64_T stale;
	char *buffer;
	INT64_T buffer_valid;
	INT64_T buffer_offset;
	INT64_T buffer_dirty;
};

struct hash_table *table = 0;
static int chirp_reli_blocksize = 65536;
static int chirp_reli_default_nreps = 0;

INT64_T chirp_reli_blocksize_get()
{
	return chirp_reli_blocksize;
}

void    chirp_reli_blocksize_set( INT64_T bs )
{
	chirp_reli_blocksize = bs;
}

static struct chirp_client * connect_to_host( const char *host, time_t stoptime )
{
	struct chirp_client *c;

	if(!table) {
		table = hash_table_create(0,0);
		if(!table) return 0;
	}

	c = hash_table_lookup(table,host);
	if(c) return c;

	if(!strncmp(host,"CONDOR",6)) {
		c = chirp_client_connect_condor(stoptime);
	} else {
		c = chirp_client_connect(host,1,stoptime);
	}

	if(c) {
		/*
		If a default replication factor was set earlier, then
		it must be re-applied when re-connecting after a failure.
		*/
		if(chirp_reli_default_nreps>0) {
			chirp_client_setrep(c,"@@@",chirp_reli_default_nreps,stoptime);
		}
		hash_table_insert(table,host,c);
		return c;
	} else {
		return 0;
	}
}

static INT64_T connect_to_file( struct chirp_client *client, struct chirp_file *file, time_t stoptime )
{
	struct chirp_stat buf;

	if(file->stale) {
		errno = ESTALE;
		return -1;
	}

	if(chirp_client_serial(client)==file->serial) return 1;

	debug(D_CHIRP,"verifying: %s",file->path);
	file->fd = chirp_client_open(client,file->path,file->flags,file->mode,&buf,stoptime);
	file->serial = chirp_client_serial(client);
	if(file->fd>=0) {
		if(buf.cst_dev!=file->info.cst_dev) {
			debug(D_CHIRP,"stale: device changed: %s",file->path);
			file->stale = 1;
			errno = ESTALE;
			return 0;
		} else if(buf.cst_ino!=file->info.cst_ino) {
			debug(D_CHIRP,"stale: inode changed: %s",file->path);
			file->stale = 1;
			errno = ESTALE;
			return 0;
		} else if(buf.cst_rdev!=file->info.cst_rdev) {
			debug(D_CHIRP,"stale: rdev changed: %s",file->path);
			file->stale = 1;
			errno = ESTALE;
			return 0;
		} else {
			debug(D_CHIRP,"uptodate: %s",file->path);
			file->stale = 0;
			return 1;
		}
	} else {
		if(errno!=ECONNRESET) {
			debug(D_CHIRP,"stale: %s: %s",strerror(errno),file->path);
			file->stale = 1;
			errno = ESTALE;
			return 0;
		}
	}

	return 1;
}

void chirp_reli_disconnect( const char *host )
{
	struct chirp_client *c;
	c = hash_table_remove(table,host);
	if(c) chirp_client_disconnect(c);
}

struct chirp_file * chirp_reli_open( const char *host, const char *path, INT64_T flags, INT64_T mode, time_t stoptime )
{
	struct chirp_file *file;
	int     delay=0;
	time_t  nexttry;
	INT64_T result;
	struct chirp_stat buf;
	time_t current;

	while(1) {
		struct chirp_client *client = connect_to_host(host,stoptime);
		if(client) {
			result = chirp_client_open(client,path,flags,mode,&buf,stoptime);
			if(result>=0) {
				file = xxmalloc(sizeof(*file));
				strcpy(file->host,host);
				strcpy(file->path,path);
				memcpy(&file->info,&buf,sizeof(buf));
				file->fd = result;
				file->flags = flags & ~(O_CREAT|O_TRUNC);
				file->mode = mode;
				file->serial = chirp_client_serial(client);
				file->stale = 0;
				file->buffer = malloc(chirp_reli_blocksize);
				file->buffer_offset = 0;
				file->buffer_valid = 0;
				file->buffer_dirty = 0;
				return file;
			} else {
				if(errno!=ECONNRESET) return 0;
			}
			chirp_reli_disconnect(host);
		} else {
			if(errno==ENOENT) return 0;
		}
		if(time(0)>=stoptime) {
			errno = ECONNRESET;
			return 0;
		}
		if(delay>=2) debug(D_NOTICE,"couldn't connect to %s: still trying...\n",host);
		debug(D_CHIRP,"couldn't talk to %s: %s\n",host,strerror(errno));
		current = time(0);
		nexttry = MIN(stoptime,current+delay);
		debug(D_CHIRP,"try again in %d seconds\n",(int)(nexttry-current));
		sleep_until(nexttry);
		if(delay==0) {
			delay = 1;
		} else {
			delay = MIN(delay*2,MAX_DELAY);
		}
	}
}

INT64_T chirp_reli_close( struct chirp_file *file, time_t stoptime )
{
	struct chirp_client *client;
	if(chirp_reli_flush(file,stoptime) < 0)
		return -1;
	client = connect_to_host(file->host,stoptime);
	if(client) {
		if(chirp_client_serial(client)==file->serial) {
			chirp_client_close(client,file->fd,stoptime);
		}
	}
	free(file->buffer);
	free(file);
	return 0;
}

#define RETRY_FILE( ZZZ ) \
	int delay=0; \
	time_t nexttry; \
	INT64_T result; \
	time_t current; \
	while(1) { \
		struct chirp_client *client = connect_to_host(file->host,stoptime); \
		if(client) { \
			if(connect_to_file(client,file,stoptime)) { \
				ZZZ \
				if(result>=0 || errno!=ECONNRESET) return result; \
			} \
			if(errno==ESTALE) return -1; \
			chirp_reli_disconnect(file->host); \
		} else { \
			if(errno==ENOENT) return -1; \
			if(errno==EPERM) return -1; \
			if(errno==EACCES) return -1; \
		} \
		if(time(0)>=stoptime) { \
			errno = ECONNRESET; \
			return -1; \
		} \
		if(delay>=2) debug(D_NOTICE,"couldn't connect to %s: still trying...\n",file->host); \
		debug(D_CHIRP,"couldn't talk to %s: %s\n",file->host,strerror(errno)); \
		current = time(0); \
		nexttry = MIN(stoptime,current+delay); \
		debug(D_CHIRP,"try again in %d seconds\n",(int)(nexttry-current)); \
		sleep_until(nexttry); \
		if(delay==0) {\
			delay = 1;\
		} else {\
			delay = MIN(delay*2,MAX_DELAY); \
		}\
	}


INT64_T chirp_reli_pread_unbuffered( struct chirp_file *file, void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	RETRY_FILE( result = chirp_client_pread(client,file->fd,data,length,offset,stoptime); )
}

static INT64_T chirp_reli_pread_buffered( struct chirp_file *file, void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	if(file->buffer_valid) {
		if(offset >= file->buffer_offset && offset < (file->buffer_offset+file->buffer_valid) ) {
			INT64_T blength;
			blength = MIN(length,file->buffer_offset+file->buffer_valid-offset);
			memcpy(data,&file->buffer[offset-file->buffer_offset],blength);
			return blength;
		}
	}

	chirp_reli_flush(file,stoptime);

	if(length<=chirp_reli_blocksize) {
		INT64_T result = chirp_reli_pread_unbuffered(file,file->buffer,chirp_reli_blocksize,offset,stoptime);
		if(result<0) {
			file->buffer_offset = 0;
			file->buffer_valid = 0;
			file->buffer_dirty = 0;
			return result;
		} else {
			file->buffer_offset = offset;
			file->buffer_valid = result;
			file->buffer_dirty = 0;
			result = MIN(result,length);
			memcpy(data,file->buffer,result);
			return result;
		}
	} else {
		return chirp_reli_pread_unbuffered(file,data,length,offset,stoptime);
	}
}

INT64_T chirp_reli_pread( struct chirp_file *file, void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	char *cdata = data;
	INT64_T result = 0;
	INT64_T actual = 0;

	while(length>0) {
		actual = chirp_reli_pread_buffered(file,cdata,length,offset,stoptime);
		if(actual<=0) break;

		result += actual;
		cdata += actual;
		offset += actual;
		length -= actual;
	}

	if(result>0) {
		return result;
	} else {
		return actual;
	}
}

INT64_T chirp_reli_pwrite_unbuffered( struct chirp_file *file, const void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	RETRY_FILE( result = chirp_client_pwrite(client,file->fd,data,length,offset,stoptime); )
}

static INT64_T chirp_reli_pwrite_buffered( struct chirp_file *file, const void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	if(length>=chirp_reli_blocksize) {
		if(chirp_reli_flush(file,stoptime)<0) {
			return -1;
		} else {
			return chirp_reli_pwrite_unbuffered(file,data,length,offset,stoptime);
		}
	}

	if(file->buffer_valid>0) {
		if( (file->buffer_offset + file->buffer_valid) == offset ) {
			INT64_T blength = MIN(chirp_reli_blocksize-file->buffer_valid,length);
			memcpy(&file->buffer[file->buffer_valid],data,blength);
			file->buffer_valid += blength;
			file->buffer_dirty = 1;
			if(file->buffer_valid==chirp_reli_blocksize) {
				if(chirp_reli_flush(file,stoptime)<0) {
					return -1;
				}
			}
			return blength;
		} else {
			if(chirp_reli_flush(file,stoptime)<0) {
				return -1;
			} else {
				/* fall through */
			}
		}
	}

	/* if we got here, then the buffer is empty */

	file->buffer_offset = offset;
	file->buffer_valid = length;
	file->buffer_dirty = 1;
	memcpy(file->buffer,data,length);
	return length;
}

INT64_T chirp_reli_pwrite( struct chirp_file *file, const void *data, INT64_T length, INT64_T offset, time_t stoptime )
{
	const char *cdata = data;
	INT64_T result = 0;
	INT64_T actual = 0;

	while(length>0) {
		actual = chirp_reli_pwrite_buffered(file,cdata,length,offset,stoptime);
		if(actual<=0) break;

		result += actual;
		cdata += actual;
		offset += actual;
		length -= actual;
	}

	if(result>0) {
		return result;
	} else {
		return actual;
	}
}

INT64_T chirp_reli_sread( struct chirp_file *file, void *data, INT64_T length, INT64_T stride_length, INT64_T stride_offset, INT64_T offset, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_sread(client,file->fd,data,length,stride_length,stride_offset,offset,stoptime); )
}

INT64_T chirp_reli_swrite( struct chirp_file *file, const void *data, INT64_T length, INT64_T stride_length, INT64_T stride_offset, INT64_T offset, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_swrite(client,file->fd,data,length,stride_length,stride_offset,offset,stoptime); )
}

INT64_T chirp_reli_fstat( struct chirp_file *file, struct chirp_stat *buf, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fstat(client,file->fd,buf,stoptime); )
}

INT64_T chirp_reli_fstatfs( struct chirp_file *file, struct chirp_statfs *buf, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fstatfs(client,file->fd,buf,stoptime); )
}

INT64_T chirp_reli_fchown( struct chirp_file *file, INT64_T uid, INT64_T gid, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fchown(client,file->fd,uid,gid,stoptime); )
}

INT64_T chirp_reli_fchmod( struct chirp_file *file, INT64_T mode, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fchmod(client,file->fd,mode,stoptime); )
}

INT64_T chirp_reli_ftruncate( struct chirp_file *file, INT64_T length, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_ftruncate(client,file->fd,length,stoptime); )
}

INT64_T chirp_reli_flush( struct chirp_file *file, time_t stoptime )
{
	INT64_T result;

	if(file->buffer_valid && file->buffer_dirty) {
		result = chirp_reli_pwrite_unbuffered(file,file->buffer,file->buffer_valid,file->buffer_offset,stoptime);
	} else {
		result = 0;
	}

	file->buffer_valid = 0;
	file->buffer_dirty = 0;
	file->buffer_offset = 0;

	return result;
}

INT64_T chirp_reli_fsync( struct chirp_file *file, time_t stoptime )
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fsync(client,file->fd,stoptime); );
}

#define _RETRY_ATOMIC( _ZZZ, _NOEAGAIN ) \
	int delay=0; \
	time_t nexttry; \
	INT64_T result; \
	time_t current; \
	while(1) { \
		struct chirp_client *client = connect_to_host(host,stoptime); \
		if(client) { \
			_ZZZ \
			if(result>=0) { \
				return result; \
			} else if (errno == ECONNRESET) { \
				chirp_reli_disconnect(host); \
			} else if (errno == EAGAIN) { \
				if (_NOEAGAIN) \
					return result; \
				/* fall through */ \
			} else { \
				return result; \
			} \
		} else { \
			if(errno==ENOENT) return -1; \
			if(errno==EPERM) return -1; \
			if(errno==EACCES) return -1; \
		} \
		if(time(0)>=stoptime) { \
			errno = ECONNRESET; \
			return -1; \
		} \
		if(delay>=2) debug(D_NOTICE,"couldn't connect to %s: still trying...\n",host); \
		debug(D_CHIRP,"couldn't talk to %s: %s\n",host,strerror(errno)); \
		current = time(0); \
		nexttry = MIN(stoptime,current+delay); \
		debug(D_CHIRP,"try again in %d seconds\n",(int)(nexttry-current)); \
		sleep_until(nexttry); \
		if(delay==0) {\
			delay = 1;\
		} else {\
			delay = MIN(delay*2,MAX_DELAY); \
		}\
	}

#define RETRY_ATOMIC(_ZZZ) _RETRY_ATOMIC(_ZZZ, 0)
#define RETRY_ATOMIC_NOEAGAIN(_ZZZ) _RETRY_ATOMIC(_ZZZ, 1)

INT64_T chirp_reli_whoami( const char *host, char *buf, INT64_T length, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_whoami(client,buf,length,stoptime); );
}

INT64_T chirp_reli_whoareyou( const char *host, const char *rhost, char *buffer, INT64_T length, time_t stoptime  )
{
	RETRY_ATOMIC( result = chirp_client_whoareyou(client,rhost,buffer,length,stoptime); );
}

INT64_T chirp_reli_getfile( const char *host, const char *path, FILE *stream, time_t stoptime )
{
	INT64_T pos = ftell(stream);
	if (pos < 0) pos = 0;

	RETRY_ATOMIC(\
		fseek(stream,pos,SEEK_SET);\
		result = chirp_client_getfile(client,path,stream,stoptime);\
		if(result<0 && ferror(stream)) { errno=EIO; return -1; }\
	)
}

INT64_T chirp_reli_getfile_buffer( const char *host, const char *path, char **buffer, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_getfile_buffer(client,path,buffer,stoptime); )
}

INT64_T chirp_reli_putfile( const char *host, const char *path, FILE *stream, INT64_T mode, INT64_T length, time_t stoptime )
{
	RETRY_ATOMIC(
		fseek(stream,0,SEEK_SET);\
		result = chirp_client_putfile(client,path,stream,mode,length,stoptime);\
		if(result<0 && ferror(stream)) { errno=EIO; return -1; }\
	)
}

INT64_T chirp_reli_putfile_buffer( const char *host, const char *path, const void *buffer, INT64_T mode, size_t length, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_putfile_buffer(client,path,buffer,mode,length,stoptime); )
}

INT64_T chirp_reli_getlongdir( const char *host, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_getlongdir(client,path,callback,arg,stoptime); )
}

INT64_T chirp_reli_getdir( const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_getdir(client,path,callback,arg,stoptime); )
}

INT64_T chirp_reli_getacl( const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_getacl(client,path,callback,arg,stoptime); )
}

INT64_T chirp_reli_ticket_create( const char *host, char name[CHIRP_PATH_MAX], unsigned bits, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_create(client,name,bits,stoptime); )
}

INT64_T chirp_reli_ticket_register( const char *host, const char *name, const char *subject, time_t duration, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_register(client,name,subject,duration,stoptime); )
}

INT64_T chirp_reli_ticket_delete( const char *host, const char *name, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_delete(client,name,stoptime); )
}

INT64_T chirp_reli_ticket_list( const char *host, const char *subject, char ***list, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_list(client,subject,list,stoptime); )
}

INT64_T chirp_reli_ticket_get( const char *host, const char *name, char **subject, char **ticket, time_t *duration, char ***rights, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_get(client,name,subject,ticket,duration,rights,stoptime); )
}

INT64_T chirp_reli_ticket_modify( const char *host, const char *name, const char *path, const char *aclmask, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_ticket_modify(client,name,path,aclmask,stoptime); )
}

INT64_T chirp_reli_setacl( const char *host, const char *path, const char *subject, const char *rights, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_setacl(client,path,subject,rights,stoptime); )
}

INT64_T chirp_reli_resetacl( const char *host, const char *path, const char *rights, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_resetacl(client,path,rights,stoptime); )
}

INT64_T chirp_reli_locate( const char *host, const char *path, chirp_loc_t callback, void *arg, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_locate(client,path,callback,arg,stoptime); )
}

INT64_T chirp_reli_unlink( const char *host, const char *path, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_unlink(client,path,stoptime); )
}

INT64_T chirp_reli_rename( const char *host, const char *path, const char *newpath, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_rename(client,path,newpath,stoptime); )
}

INT64_T chirp_reli_link( const char *host, const char *path, const char *newpath, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_link(client,path,newpath,stoptime); )
}

INT64_T chirp_reli_symlink( const char *host, const char *path, const char *newpath, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_symlink(client,path,newpath,stoptime); )
}

INT64_T chirp_reli_readlink( const char *host, const char *path, char *buf, INT64_T length, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_readlink(client,path,buf,length,stoptime); )
}

INT64_T chirp_reli_mkdir( const char *host, const char *path, INT64_T mode, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_mkdir(client,path,mode,stoptime); )
}

INT64_T chirp_reli_mkdir_recursive( const char *host, const char *path, INT64_T mode, time_t stoptime )
{
	char mypath[CHIRP_PATH_MAX];
	strcpy(mypath,path);

	char *n = strchr(&mypath[1],'/');
	while(n) {
		*n = 0;
		/* ignore the result here, because there are many reasons we might not have permission to make or view directories above. */
		chirp_reli_mkdir(host,mypath,mode,stoptime);
		*n = '/';
		n = strchr(n+1,'/');
	}

	/* this is the error that really counts */
	return chirp_reli_mkdir(host,path,mode,stoptime);
}

INT64_T chirp_reli_rmdir( const char *host, const char *path, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_rmdir(client,path,stoptime); )
}

INT64_T chirp_reli_rmall( const char *host, const char *path, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_rmall(client,path,stoptime); )
}

INT64_T chirp_reli_stat( const char *host, const char *path, struct chirp_stat *buf, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_stat(client,path,buf,stoptime); )
}

INT64_T chirp_reli_lstat( const char *host, const char *path, struct chirp_stat *buf, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_lstat(client,path,buf,stoptime); )
}

INT64_T chirp_reli_statfs( const char *host, const char *path, struct chirp_statfs *buf, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_statfs(client,path,buf,stoptime); )
}

INT64_T chirp_reli_access( const char *host, const char *path, INT64_T mode, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_access(client,path,mode,stoptime); )
}

INT64_T chirp_reli_chmod( const char *host, const char *path, INT64_T mode, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_chmod(client,path,mode,stoptime); )
}

INT64_T chirp_reli_chown( const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_chown(client,path,uid,gid,stoptime); )
}

INT64_T chirp_reli_lchown( const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_lchown(client,path,uid,gid,stoptime); )
}

INT64_T chirp_reli_truncate( const char *host, const char *path, INT64_T length, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_truncate(client,path,length,stoptime); )
}

INT64_T chirp_reli_utime( const char *host, const char *path, time_t actime, time_t modtime, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_utime(client,path,actime,modtime,stoptime); )
}

INT64_T chirp_reli_hash( const char *host, const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_hash(client,path,algorithm,digest,stoptime); )
}

INT64_T chirp_reli_md5( const char *host, const char *path, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_md5(client,path,digest,stoptime); )
}

INT64_T chirp_reli_setrep(const char *host, const char *path, int nreps, time_t stoptime )
{
	/*
	If setting a default replication factor for the session,
	then save it in a global so that it can be re-applied on a re-connection.
	*/

	if(!strcmp(path,"@@@")) chirp_reli_default_nreps = nreps;
	RETRY_ATOMIC( result = chirp_client_setrep(client,path,nreps,stoptime); )
}

INT64_T chirp_reli_getxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_getxattr(client,path,name,data,size,stoptime); )
}

INT64_T chirp_reli_fgetxattr(struct chirp_file *file, const char *name, void *data, size_t size, time_t stoptime)
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fgetxattr(client,file->fd,name,data,size,stoptime); )
}

INT64_T chirp_reli_lgetxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_lgetxattr(client,path,name,data,size,stoptime); )
}

INT64_T chirp_reli_listxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_listxattr(client,path,list,size,stoptime); )
}

INT64_T chirp_reli_flistxattr(struct chirp_file *file, char *list, size_t size, time_t stoptime)
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_flistxattr(client,file->fd,list,size,stoptime); )
}

INT64_T chirp_reli_llistxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_llistxattr(client,path,list,size,stoptime); )
}

INT64_T chirp_reli_setxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_setxattr(client,path,name,data,size,flags,stoptime); )
}

INT64_T chirp_reli_fsetxattr(struct chirp_file *file, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fsetxattr(client,file->fd,name,data,size,flags,stoptime); )
}

INT64_T chirp_reli_lsetxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_lsetxattr(client,path,name,data,size,flags,stoptime); )
}

INT64_T chirp_reli_removexattr(const char *host, const char *path, const char *name, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_removexattr(client,path,name,stoptime); )
}

INT64_T chirp_reli_fremovexattr(struct chirp_file *file, const char *name, time_t stoptime)
{
	chirp_reli_flush(file,stoptime);
	RETRY_FILE( result = chirp_client_fremovexattr(client,file->fd,name,stoptime); )
}

INT64_T chirp_reli_lremovexattr(const char *host, const char *path, const char *name, time_t stoptime)
{
	RETRY_ATOMIC( result = chirp_client_lremovexattr(client,path,name,stoptime); )
}

INT64_T chirp_reli_job_create (const char *host, const char *json, chirp_jobid_t *id, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_create(client,json,id,stoptime); )
}

INT64_T chirp_reli_job_commit (const char *host, const char *json, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_commit(client,json,stoptime); )
}

INT64_T chirp_reli_job_kill (const char *host, const char *json, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_kill(client,json,stoptime); )
}

INT64_T chirp_reli_job_status (const char *host, const char *json, char **status, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_status(client,json,status,stoptime); )
}

INT64_T chirp_reli_job_wait (const char *host, chirp_jobid_t id, INT64_T timeout, char **status, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_wait(client,id,timeout,status,stoptime); )
}

INT64_T chirp_reli_job_reap (const char *host, const char *json, time_t stoptime)
{
	RETRY_ATOMIC_NOEAGAIN( result = chirp_client_job_reap(client,json,stoptime); )
}

INT64_T chirp_reli_remote_debug( const char *host, const char *flag, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_remote_debug(client,flag,stoptime); )
}

INT64_T chirp_reli_localpath( const char *host, const char *path, char *localpath, int length, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_localpath(client,path,localpath,length,stoptime); )
}

INT64_T chirp_reli_audit( const char *host, const char *path, struct chirp_audit **list, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_audit(client,path,list,stoptime); )
}

INT64_T chirp_reli_thirdput( const char *host, const char *path, const char *thirdhost, const char *thirdpath, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_thirdput( client, path, thirdhost, thirdpath, stoptime ); )
}

INT64_T chirp_reli_mkalloc( const char *host, const char *path, INT64_T size, INT64_T mode, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_mkalloc(client,path,size,mode,stoptime); )
}

INT64_T chirp_reli_lsalloc( const char *host, const char *path, char *allocpath, INT64_T *total, INT64_T *inuse, time_t stoptime )
{
	RETRY_ATOMIC( result = chirp_client_lsalloc(client,path,allocpath,total,inuse,stoptime); )
}

CHIRP_SEARCH *chirp_reli_opensearch( const char *host, const char *paths, const char *pattern, int flags, time_t stoptime )
{
	struct chirp_client *client = connect_to_host(host, stoptime);
	if (client)
		return chirp_client_opensearch(client, paths, pattern, flags, stoptime);
	else
		return NULL;
}

struct chirp_dir {
	   struct chirp_dirent *head;
	   struct chirp_dirent *current;
};

static void opendir_callback( const char *path, struct chirp_stat *info, void *vdir )
{
	struct chirp_dir *dir = vdir;
	struct chirp_dirent *d;

	d = malloc(sizeof(*d));
	d->name = strdup(path);
	d->info = *info;
	d->next = 0;

	if(!dir->head) {
		dir->head = d;
		dir->current = d;
	} else {
		dir->current->next = d;
		dir->current = d;
	}
}

struct chirp_dir * chirp_reli_opendir( const char *host, const char *path, time_t stoptime )
{
	struct chirp_dir *dir = malloc(sizeof(*dir));
	INT64_T result;

	dir->head = dir->current = 0;

	result = chirp_reli_getlongdir(host,path,opendir_callback,dir,stoptime);
	if(result<0) {
		chirp_reli_closedir(dir);
		return 0;
	}

	dir->current = dir->head;

	return dir;
}

struct chirp_dirent * chirp_reli_readdir( struct chirp_dir *dir )
{
	struct chirp_dirent *d;

	if(!dir) return 0;

	d = dir->current;
	if(d) dir->current = dir->current->next;

	return d;
}

void chirp_reli_closedir( struct chirp_dir *dir )
{
	struct chirp_dirent *next;

	if(!dir) return;

	while(dir->head) {
		next = dir->head->next;
		free(dir->head->name);
		free(dir->head);
		dir->head = next;
	}
	free(dir);
}

static INT64_T chirp_reli_bulkio_once( struct chirp_bulkio *v, int count, time_t stoptime )
{
	int i;
	INT64_T result;

	for(i=0;i<count;i++) {
		struct chirp_bulkio *b = &v[i];
		struct chirp_client *client;

		client = connect_to_host(b->file->host,stoptime);
		if(!client) goto failure;

		if(connect_to_file(client,b->file,stoptime)<0) goto failure;

		if(b->type==CHIRP_BULKIO_PREAD) {
			result = chirp_client_pread_begin(client,b->file->fd,b->buffer,b->length,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_PWRITE) {
			result = chirp_client_pwrite_begin(client,b->file->fd,b->buffer,b->length,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_SREAD) {
			result = chirp_client_sread_begin(client,b->file->fd,b->buffer,b->length,b->stride_length,b->stride_skip,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_SWRITE) {
			result = chirp_client_swrite_begin(client,b->file->fd,b->buffer,b->length,b->stride_length,b->stride_skip,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_FSTAT) {
			result = chirp_client_fstat_begin(client,b->file->fd,b->info,stoptime);
		} else if(b->type==CHIRP_BULKIO_FSYNC) {
			result = chirp_client_fsync_begin(client,b->file->fd,stoptime);
		} else {
			result = -1;
			errno = EINVAL;
		}

		if(result<0 && errno==ECONNRESET) goto failure;
	}

	for(i=0;i<count;i++) {
		struct chirp_bulkio *b = &v[i];
		struct chirp_client *client;

		client = connect_to_host(b->file->host,stoptime);
		if(!client) goto failure;

		if(b->type==CHIRP_BULKIO_PREAD) {
			result = chirp_client_pread_finish(client,b->file->fd,b->buffer,b->length,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_PWRITE) {
			result = chirp_client_pwrite_finish(client,b->file->fd,b->buffer,b->length,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_SREAD) {
			result = chirp_client_sread_finish(client,b->file->fd,b->buffer,b->length,b->stride_length,b->stride_skip,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_SWRITE) {
			result = chirp_client_swrite_finish(client,b->file->fd,b->buffer,b->length,b->stride_length,b->stride_skip,b->offset,stoptime);
		} else if(b->type==CHIRP_BULKIO_FSTAT) {
			result = chirp_client_fstat_finish(client,b->file->fd,b->info,stoptime);
		} else if(b->type==CHIRP_BULKIO_FSYNC) {
			result = chirp_client_fsync_finish(client,b->file->fd,stoptime);
		} else {
			result = -1;
			errno = EINVAL;
		}

		if(result<0 && errno==ECONNRESET) goto failure;

		b->result = result;
		b->errnum = errno;
	}

	return count;

	failure:
	for(i=0;i<count;i++) {
		struct chirp_bulkio *b = &v[i];
		chirp_reli_disconnect(b->file->host);
	}
	errno = ECONNRESET;
	return -1;
}

INT64_T chirp_reli_bulkio( struct chirp_bulkio *v, int count, time_t stoptime )
{
	int delay=0;
	time_t nexttry;
	INT64_T result;
	time_t current;

	while(1) {
		result = chirp_reli_bulkio_once(v,count,stoptime);

		if(result>=0 || errno!=ECONNRESET) return result;

		if(time(0)>=stoptime) {
			errno = ECONNRESET;
			return -1;
		}
		if(delay>=2) debug(D_NOTICE,"couldn't connect: still trying...\n");
		current = time(0);
		nexttry = MIN(stoptime,current+delay);
		debug(D_CHIRP,"try again in %d seconds\n",(int)(nexttry-current));
		sleep_until(nexttry);
		if(delay==0) {
			delay = 1;
		} else {
			delay = MIN(delay*2,MAX_DELAY);
		}
	}
}

void chirp_reli_cleanup_before_fork()
{
	char *host;
	char *value;

	if(!table) return;

	hash_table_firstkey(table);
	while(hash_table_nextkey(table,&host,(void**)&value)) {
		chirp_reli_disconnect(host);
	}
}

/* vim: set noexpandtab tabstop=8: */
