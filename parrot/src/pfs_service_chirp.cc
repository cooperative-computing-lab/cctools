/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_table.h"
#include "pfs_service.h"
#include "pfs_location.h"

extern "C" {
#include "chirp_global.h"
#include "chirp_types.h"
#include "chirp_reli.h"
#include "chirp_client.h"
#include "stringtools.h"
#include "debug.h"
#include "xxmalloc.h"
#include "macros.h"
#include "hash_table.h"
}

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <sys/statfs.h>


extern int pfs_master_timeout;
extern int pfs_enable_small_file_optimizations;

char chirp_rootpath[] = "/";

static struct hash_table * chirp_dircache = 0;
static char * chirp_dircache_path = 0;

static void chirp_dircache_invalidate()
{
	char *key;
	void *value;

	if(chirp_dircache) {
		hash_table_firstkey(chirp_dircache);
		while(hash_table_nextkey(chirp_dircache,&key,&value)) {
			hash_table_remove(chirp_dircache,key);
			free(value);
		}
	}

	if(chirp_dircache_path) {
		free(chirp_dircache_path);
		chirp_dircache_path = 0;
	}
}

static void chirp_dircache_begin( const char *path )
{
	chirp_dircache_invalidate();
	chirp_dircache_path = xxstrdup(path);
}

static void chirp_dircache_insert( const char *name, struct chirp_stat *info, void *arg )
{
	char path[CHIRP_PATH_MAX];

	if(!chirp_dircache) chirp_dircache = hash_table_create(0,0);

	pfs_dir *dir = (pfs_dir *)arg;
	dir->append(name);

	struct chirp_stat *copy_info = (struct chirp_stat *)malloc(sizeof(*info));
	*copy_info = *info;

	sprintf(path,"%s/%s",chirp_dircache_path,name);

	hash_table_insert(chirp_dircache,path,copy_info);
}

static int chirp_dircache_lookup( const char *path, struct chirp_stat *info )
{
	struct chirp_stat *value;

	if(!chirp_dircache) chirp_dircache = hash_table_create(0,0);

	value = (struct chirp_stat*) hash_table_lookup(chirp_dircache,path);
	if(value) {
		*info = *value;
		hash_table_remove(chirp_dircache,path);
		free(value);
		return 1;
	} else {
		return 0;
	}
}

static void add_to_dir( const char *name, void *arg )
{
	pfs_dir *dir = (pfs_dir *)arg;
	dir->append(name);
}

class pfs_file_chirp : public pfs_file
{
private:
	struct chirp_file *file;

public:
	pfs_file_chirp( pfs_name *name, struct chirp_file *f ) : pfs_file(name) {
		file = f;
	}

	virtual int close() {
		return chirp_global_close(file,time(0)+pfs_master_timeout);
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		return chirp_global_pread(file,data,length,offset,time(0)+pfs_master_timeout);
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		chirp_dircache_invalidate();
		return chirp_global_pwrite(file,data,length,offset,time(0)+pfs_master_timeout);
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct chirp_stat cbuf;
		result = chirp_global_fstat(file,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int fstatfs( struct pfs_statfs *buf ) {
		int result;
		struct chirp_statfs cbuf;
		result = chirp_global_fstatfs(file,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_STATFS(cbuf,*buf);
		}
		return result;
	}

	virtual int ftruncate( pfs_size_t length ) {
		chirp_dircache_invalidate();
		return chirp_global_ftruncate(file,length,time(0)+pfs_master_timeout);
	}

	virtual int fchmod( mode_t mode ) {
		chirp_dircache_invalidate();
		return chirp_global_fchmod(file,mode,time(0)+pfs_master_timeout);
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		chirp_dircache_invalidate();
		return chirp_global_fchown(file,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual ssize_t fgetxattr( const char *name, void *data, size_t size ) {
		return chirp_global_fgetxattr(file,name,data,size,time(0)+pfs_master_timeout);
	}

	virtual ssize_t flistxattr( char *list, size_t size ) {
		return chirp_global_flistxattr(file,list,size,time(0)+pfs_master_timeout);
	}

	virtual int fsetxattr( const char *name, const void *data, size_t size, int flags ) {
		return chirp_global_fsetxattr(file,name,data,size,flags,time(0)+pfs_master_timeout);
	}

	virtual int fremovexattr( const char *name ) {
		return chirp_global_fremovexattr(file,name,time(0)+pfs_master_timeout);
	}

	virtual int fsync() {
		chirp_dircache_invalidate();
		return chirp_global_flush(file,time(0)+pfs_master_timeout)>=0 ? 0 : -1;
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat buf;
		if(this->fstat(&buf)==0) {
			return buf.st_size;
		} else {
			return -1;
		}
	}
};

static void add_to_acl( const char *entry, void *vbuf )
{
	char *buf = (char*)vbuf;
	strcat(buf,entry);
	strcat(buf,"\n");
}

class pfs_service_chirp : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		struct chirp_file *file;
		chirp_dircache_invalidate();
		file = chirp_global_open(name->hostport,name->rest,flags,mode,time(0)+pfs_master_timeout);
		if(file) {
			return new pfs_file_chirp(name,file);
		} else {
			return 0;
		}
	}

	static int search_chirp_stat_pack( struct chirp_stat c_info, char *buffer, size_t *i, size_t buffer_length ) {
		struct stat info;
		COPY_CSTAT(c_info, info);
		size_t n = snprintf(
			buffer + *i,
			buffer_length - *i,
			"|%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld",
			(long)info.st_dev,
			(long)info.st_ino,
			(long)info.st_mode,
			(long)info.st_nlink,
			(long)info.st_uid,
			(long)info.st_gid,
			(long)info.st_rdev,
			(long)info.st_size,
			(long)info.st_atime,
			(long)info.st_mtime,
			(long)info.st_ctime,
			(long)info.st_blksize,
			(long)info.st_blocks
		);

		if (n>=buffer_length-*i) {
			return -1;
		} else {
			*i += n;
			return 0;
		}
	}

	virtual int search( pfs_name *name, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i )
	{
		if (strlen(name->rest)==0) {
			sprintf(name->rest, "/");
		}

		CHIRP_SEARCH *s = chirp_reli_opensearch(name->hostport, name->rest, pattern, flags, time(0)+pfs_master_timeout);
		if (!s)
			return -1;

		struct chirp_searchent *res;
		int n = 0;
		while ((res = chirp_client_readsearch(s)) != NULL) {
			size_t l;

			n += 1;

			if (res->err)
				l = snprintf(buffer+*i, buffer_length-*i,  "%s%d|%d|%s", *i==0 ? "" : "|", res->err, res->errsource, res->path);
			else
				l = snprintf(buffer+*i, buffer_length-*i, "%s0|%s", *i==0 ? "" : "|", res->path);

			if (l >= buffer_length-*i) {
				errno = ERANGE;
				return -1;
			}

			*i += l;

			if (res->err == 0) {
				if (flags & PFS_SEARCH_METADATA) {
					if (search_chirp_stat_pack(res->info, buffer, i, buffer_length) == -1) {
						errno = ERANGE;
						return -1;
					}
				} else {
					if ((size_t)snprintf(buffer+*i, buffer_length-*i, "|") >= buffer_length-*i) {
						errno = ERANGE;
						return -1;
					}
					(*i)++;
				}
			}
		}

		chirp_client_closesearch(s);
		return n;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		int result=-1;
		pfs_dir *dir = new pfs_dir(name);

		if(pfs_enable_small_file_optimizations) {
			chirp_dircache_begin(name->path);
			result = chirp_global_getlongdir(name->hostport,name->rest,chirp_dircache_insert,dir,time(0)+pfs_master_timeout);
		} else {
			result = -1;
			errno = EINVAL;
		}

		if(result<0 && (errno==EINVAL||errno==ENOSYS)) {
			chirp_dircache_invalidate();
			result = chirp_global_getdir(name->hostport,name->rest,add_to_dir,dir,time(0)+pfs_master_timeout);
		}

		if(result>=0) {
			return dir;
		} else {
			delete dir;
			return 0;
		}
	}

	virtual int statfs( pfs_name *name, struct pfs_statfs *buf ) {
		int result;
		struct chirp_statfs cbuf;
		result = chirp_global_statfs(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_STATFS(cbuf,*buf);
		}
		return result;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		struct chirp_stat cbuf;
		int result;
		if(chirp_dircache_lookup(name->path,&cbuf)) {
			if(!S_ISLNK(cbuf.cst_mode)) {
				COPY_CSTAT(cbuf,*buf);
				return 0;
			}
		}
		result = chirp_global_stat(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout); /* BUG: was _lstat */
		if(result==0){
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		struct chirp_stat cbuf;
		int result;
		if(chirp_dircache_lookup(name->path,&cbuf)) {
			COPY_CSTAT(cbuf,*buf);
			return 0;
		}
		result = chirp_global_lstat(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout);
		if(result==0){
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int unlink( pfs_name *name ) {
		int result;
		chirp_dircache_invalidate();
		if(pfs_enable_small_file_optimizations) {
			result = chirp_global_rmall(name->hostport,name->rest,time(0)+pfs_master_timeout);
			if(result<0 && errno==ENOSYS) {
				/* fall through */
			} else {
				return result;
			}
		}
		return chirp_global_unlink(name->hostport,name->rest,time(0)+pfs_master_timeout);
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		struct chirp_stat info;
		return chirp_global_stat(name->hostport,name->rest,&info,time(0)+pfs_master_timeout);
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		chirp_dircache_invalidate();
		return chirp_global_chmod(name->hostport,name->rest,mode,time(0)+pfs_master_timeout);
	}

	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		chirp_dircache_invalidate();
		return chirp_global_chown(name->hostport,name->rest,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		chirp_dircache_invalidate();
		return chirp_global_lchown(name->hostport,name->rest,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		chirp_dircache_invalidate();
		return chirp_global_truncate(name->hostport,name->rest,length,time(0)+pfs_master_timeout);
	}

	virtual int utime( pfs_name *name, struct utimbuf *t ) {
		return chirp_global_utime(name->hostport,name->rest,t->actime,t->modtime,time(0)+pfs_master_timeout);
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		INT64_T result;
		time_t stoptime = time(0) + pfs_master_timeout;

		chirp_dircache_invalidate();

		if(!strcmp(name->hostport,newname->hostport)) {
			result = chirp_global_rename(name->hostport,name->rest,newname->rest,stoptime);
		} else {
			result = chirp_global_thirdput(name->hostport,name->rest,newname->hostport,newname->rest,stoptime);
			if(result>=0) {
				result = 0;
				chirp_global_rmall(name->hostport,name->rest,stoptime);
			} else {
				errno = EXDEV;
			}
		}
		return result;

	}

	virtual ssize_t getxattr ( pfs_name *name, const char *attrname, void *value, size_t size )
	{
		return chirp_global_getxattr(name->hostport,name->rest,attrname,value,size,time(0)+pfs_master_timeout);
	}

	virtual ssize_t lgetxattr ( pfs_name *name, const char *attrname, void *value, size_t size )
	{
		return chirp_global_lgetxattr(name->hostport,name->rest,attrname,value,size,time(0)+pfs_master_timeout);
	}

	virtual ssize_t listxattr ( pfs_name *name, char *attrlist, size_t size )
	{
		return chirp_global_listxattr(name->hostport,name->rest,attrlist,size,time(0)+pfs_master_timeout);
	}

	virtual ssize_t llistxattr ( pfs_name *name, char *attrlist, size_t size )
	{
		return chirp_global_llistxattr(name->hostport,name->rest,attrlist,size,time(0)+pfs_master_timeout);
	}

	virtual int setxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags )
	{
		return chirp_global_setxattr(name->hostport,name->rest,attrname,value,size,flags,time(0)+pfs_master_timeout);
	}

	virtual int lsetxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags )
	{
		return chirp_global_lsetxattr(name->hostport,name->rest,attrname,value,size,flags,time(0)+pfs_master_timeout);
	}

	virtual int removexattr ( pfs_name *name, const char *attrname )
	{
		return chirp_global_removexattr(name->hostport,name->rest,attrname,time(0)+pfs_master_timeout);
	}

	virtual int lremovexattr ( pfs_name *name, const char *attrname )
	{
		return chirp_global_lremovexattr(name->hostport,name->rest,attrname,time(0)+pfs_master_timeout);
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result=-1;
		struct pfs_stat info;
		if(this->stat(name,&info)>=0) {
			if(S_ISDIR(info.st_mode)) {
				sprintf(newname,"/%s/%s:%d%s",name->service_name,name->host,name->port,name->rest);
				result = 0;
			} else {
				errno = ENOTDIR;
				result = -1;
			}
		}
		return result;
	}

	virtual int link( pfs_name *name, pfs_name *newname ) {
		chirp_dircache_invalidate();
		return chirp_global_link(name->hostport,name->rest,newname->rest,time(0)+pfs_master_timeout);
	}

	virtual int symlink( const char *linkname, pfs_name *newname ) {
		chirp_dircache_invalidate();
		return chirp_global_symlink(newname->hostport,linkname,newname->rest,time(0)+pfs_master_timeout);
	}

	virtual int readlink( pfs_name *name, char *buf, pfs_size_t length ) {
		return chirp_global_readlink(name->hostport,name->rest,buf,length,time(0)+pfs_master_timeout);
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		chirp_dircache_invalidate();
		return chirp_global_mkdir(name->hostport,name->rest,mode,time(0)+pfs_master_timeout);
	}

	virtual int rmdir( pfs_name *name ) {
		int result;
		chirp_dircache_invalidate();
		if(pfs_enable_small_file_optimizations) {
			result = chirp_global_rmall(name->hostport,name->rest,time(0)+pfs_master_timeout);
			if(result<0 && errno==ENOSYS) {
				/* fall through */
			} else {
				return result;
			}
		}
		return chirp_global_rmdir(name->hostport,name->rest,time(0)+pfs_master_timeout);
	}

	virtual int mkalloc( pfs_name *name, pfs_ssize_t size, mode_t mode ) {
		chirp_dircache_invalidate();
		return chirp_global_mkalloc(name->hostport,name->rest,size,mode,time(0)+pfs_master_timeout);
	}

	virtual int lsalloc( pfs_name *name, char *alloc_name, pfs_ssize_t *size, pfs_ssize_t *inuse ) {
		chirp_dircache_invalidate();
		return chirp_global_lsalloc(name->hostport,name->rest,alloc_name,size,inuse,time(0)+pfs_master_timeout);
	}

	virtual pfs_ssize_t putfile( pfs_name *source, pfs_name *target )
	{
		struct stat64 info;
		FILE *sourcefile;
		pfs_ssize_t result;

		chirp_dircache_invalidate();

		sourcefile = fopen(source->logical_name,"r");
		if(!sourcefile) return -1;

		fstat64(fileno(sourcefile),&info);
		if(S_ISDIR(info.st_mode)) {
			fclose(sourcefile);
			errno = EISDIR;
			return -1;
		}

		result = chirp_global_putfile(target->hostport,target->rest,sourcefile,info.st_mode&0777,info.st_size,time(0)+pfs_master_timeout);

		fclose(sourcefile);

		return result;
	}

	virtual pfs_ssize_t getfile( pfs_name *source, pfs_name *target )
	{
		FILE *targetfile;
		pfs_ssize_t result;
		int save_errno;

		chirp_dircache_invalidate();

		targetfile = fopen(target->logical_name,"w");
		if(!targetfile) return -1;

		result = chirp_global_getfile(source->hostport,source->rest,targetfile,time(0)+pfs_master_timeout);
		save_errno = errno;

		fclose(targetfile);
		if(result<0) ::unlink(target->logical_name);

		errno = save_errno;
		return result;
	}

	virtual pfs_ssize_t thirdput( pfs_name *source, pfs_name *target )
	{
		pfs_ssize_t result;

		chirp_dircache_invalidate();

		result = chirp_global_thirdput(source->hostport,source->rest,target->hostport,target->rest,time(0)+pfs_master_timeout);
		if(result>=0) {
			return 0;
		} else {
			return -1;
		}
	}

	virtual int md5( pfs_name *path, unsigned char *digest )
	{
		chirp_dircache_invalidate();
		return chirp_global_md5(path->hostport,path->rest,digest,time(0)+pfs_master_timeout);
	}

	virtual int whoami( pfs_name *name, char *buf, int size ) {
		chirp_dircache_invalidate();
		return chirp_global_whoami(name->hostport,name->rest,buf,size,time(0)+pfs_master_timeout);
	}

	virtual int getacl( pfs_name *name, char *buf, int size ) {
		int result;
		buf[0] = 0;
		chirp_dircache_invalidate();
		result = chirp_global_getacl(name->hostport,name->rest,add_to_acl,buf,time(0)+pfs_master_timeout);
		if(result==0) result = strlen(buf);
		return result;
	}

	virtual int setacl( pfs_name *name, const char *subject, const char *rights ) {
		chirp_dircache_invalidate();
		return chirp_global_setacl(name->hostport,name->rest,subject,rights,time(0)+pfs_master_timeout);
	}

	virtual pfs_location* locate( pfs_name *name ) {
		int result = -1;
		pfs_location *loc = new pfs_location();

		result = chirp_global_locate(name->host,name->path,add_to_loc,(void*)loc,time(0)+pfs_master_timeout);

		if(result>=0) {
			return loc;
		} else {
			delete loc;
			return 0;
		}
	}

	virtual int get_default_port() {
		return 9094;
	}

	virtual int is_seekable() {
		return 1;
	}

};

static pfs_service_chirp pfs_service_chirp_instance;
pfs_service *pfs_service_chirp = &pfs_service_chirp_instance;

/* vim: set noexpandtab tabstop=4: */
