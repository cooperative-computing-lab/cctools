/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "macros.h"
#include "chirp_alloc.h"
#include "chirp_protocol.h"
#include "chirp_local.h"

#include "itable.h"
#include "hash_table.h"
#include "xmalloc.h"
#include "int_sizes.h"
#include "stringtools.h"
#include "full_io.h"
#include "delete_dir.h"
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

static struct hash_table * alloc_table = 0;
static struct hash_table * root_table = 0;
static struct itable * fd_table = 0;
static int recovery_in_progress = 0;
static int alloc_enabled = 0;

struct alloc_state {
	FILE *file;
	INT64_T size;
	INT64_T inuse;
	INT64_T avail;
	INT64_T dirty;
};

/*
Note that the space consumed by a file is not the same
as the filesize.  This function computes the space consumed
by a file of a given size.  Currently, it rounds up to
the next blocksize.  A more exact function might take into
account indirect blocks allocated within the filesystem.
*/

static INT64_T space_consumed( INT64_T filesize )
{
	INT64_T block_size = 4096;
	INT64_T blocks = filesize/block_size;
	if(filesize%block_size) blocks++;
	return blocks*block_size;
}

static void alloc_state_update( struct alloc_state *a, INT64_T change )
{
	if(change!=0) {
		a->inuse += change;
		if(a->inuse<0) a->inuse=0;
		a->avail = a->size-a->inuse;
		a->dirty = 1;	
	}
}

static struct alloc_state * alloc_state_load( const char *path )
{
	struct alloc_state *s = xxmalloc(sizeof(*s));
	char statename[CHIRP_PATH_MAX];

	debug(D_ALLOC,"locking %s",path);

	sprintf(statename,"%s/.__alloc",path);

	s->file = fopen(statename,"r+");
	if(!s->file) {
		free(s);
		return 0;
	}

	if(lockf(fileno(s->file),F_TLOCK,0)) {
		debug(D_ALLOC,"lock of %s blocked; flushing outstanding locks",path);
		chirp_alloc_flush();
		debug(D_ALLOC,"locking %s (retry)",path);

		if(lockf(fileno(s->file),F_LOCK,0)) {
			debug(D_ALLOC,"lock of %s failed: %s",path,strerror(errno));
			fclose(s->file);
			free(s);
			return 0;
		}
	}

	fscanf(s->file,"%lld %lld",&s->size,&s->inuse);

	s->dirty = 0;

	if(recovery_in_progress) {
		s->inuse = 0;
		s->dirty = 1;
	}

	s->avail = s->size - s->inuse;

	return s;
}

static void alloc_state_save( const char *path, struct alloc_state *s )
{
	if(s->dirty) {
		debug(D_ALLOC,"storing %s",path);
	} else {
		debug(D_ALLOC,"freeing %s",path);
	}
		
	if(s->dirty) {
		ftruncate(fileno(s->file),0);
		fseek(s->file,0,SEEK_SET);
		fprintf(s->file,"%lld\n%lld\n",s->size,s->inuse);
	}
	fclose(s->file);
	free(s);
}

static int alloc_state_create( const char *path, INT64_T size )
{
	char statepath[CHIRP_PATH_MAX];
	FILE *file;
	sprintf(statepath,"%s/.__alloc",path);
	file = fopen(statepath,"w");
	if(file) {
		fprintf(file,"%lld 0\n",size);
		fclose(file);
		return 1;
	} else {
		return 0;
	}
}

static char * alloc_state_root( const char *path )
{
	char dirname[CHIRP_PATH_MAX];
	char statename[CHIRP_PATH_MAX];
	char *s;

	strcpy(dirname,path);

	while(1) {
		sprintf(statename,"%s/.__alloc",dirname);
		if(chirp_local_file_size(statename)>=0) {
			return xstrdup(dirname);
		}
		s = strrchr(dirname,'/');
		if(!s) return 0;
		*s = 0;
	}

	return 0;
}

static char * alloc_state_root_cached( const char *path )
{
	char *result;

	if(!root_table) root_table = hash_table_create(0,0);

	result = hash_table_lookup(root_table,path);
	if(result) return result;

	result = alloc_state_root(path);
	if(!result) return 0;

	hash_table_insert(root_table,path,result);

	return result;
}

static struct alloc_state * alloc_state_cache_exact( const char *path )
{
	struct alloc_state *a;
	char *d;
	char dirname[CHIRP_PATH_MAX];
	char statename[CHIRP_PATH_MAX];

	d = alloc_state_root_cached(path);
	if(!d) return 0;

	/*
	Save a copy of dirname, because the following
	alloc_table_load_cached may result in a flush of the alloc table root.
	*/

	strcpy(dirname,d);

	sprintf(statename,"%s/.__alloc",dirname);

	if(!alloc_table) alloc_table = hash_table_create(0,0);

	a = hash_table_lookup(alloc_table,dirname);
	if(a) return a;

	a = alloc_state_load(dirname);
	if(!a) return a;
		
	hash_table_insert(alloc_table,dirname,a);

	return a;
}

static struct alloc_state * alloc_state_cache( const char *path )
{
	char dirname[CHIRP_PATH_MAX];
	string_dirname(path,dirname);
	return alloc_state_cache_exact(dirname);
}

static void recover( const char *path )
{
	char newpath[CHIRP_PATH_MAX];
	struct chirp_stat info;
	struct alloc_state *a, *b;
	int result;
	void *dir;
	char *d;

	a = alloc_state_cache_exact(path);
	if(!a) fatal("couldn't open alloc state in %s: %s",path,strerror(errno));

	dir = chirp_local_opendir(path);
	if(!dir) fatal("couldn't open %s: %s\n",path,strerror(errno));

	while((d=chirp_local_readdir(dir))) {
		if(!strcmp(d,".")) continue;
		if(!strcmp(d,"..")) continue;
		if(!strncmp(d,".__",3)) continue;

		sprintf(newpath,"%s/%s",path,d);

		result = chirp_local_lstat(newpath,&info);
		if(result!=0) fatal("couldn't stat %s: %s\n",path,strerror(errno));

		if(S_ISDIR(info.cst_mode)) {
			recover(newpath);
			b = alloc_state_cache_exact(newpath);
			if(a!=b) alloc_state_update(a,b->size);
		} else if(S_ISREG(info.cst_mode)) {
			alloc_state_update(a,space_consumed(info.cst_size));
		} else {
			debug(D_ALLOC,"warning: unknown file type: %s\n",newpath);
		}
	}

	chirp_local_closedir(dir);

	debug(D_ALLOC,"%s (%sB)",path,string_metric(a->inuse,-1,0));
}

void chirp_alloc_init( const char *rootpath, INT64_T size )
{
	struct alloc_state *a;
	time_t start, stop;
	INT64_T inuse, avail;

#ifdef CCTOOLS_OPSYS_CYGWIN
	fatal("sorry, CYGWIN cannot employ space allocation because it does not support file locking.");
#endif

	alloc_enabled = 1;
	recovery_in_progress = 1;

	debug(D_ALLOC,"### begin allocation recovery scan ###");

	if(!alloc_state_create(rootpath,size))
		fatal("couldn't create allocation in %s: %s\n",rootpath,strerror(errno));

	a = alloc_state_cache_exact(rootpath);
	if(!a) fatal("couldn't find allocation in %s: %s\n",rootpath,strerror(errno));


	start = time(0);
	recover(rootpath);
	size = a->size;
	inuse = a->inuse;
	avail = a->avail;
	chirp_alloc_flush();
	stop = time(0);

	debug(D_ALLOC,"### allocation recovery took %d seconds ###",stop-start);

	debug(D_ALLOC,"%sB total",string_metric(size,-1,0));
	debug(D_ALLOC,"%sB in use",string_metric(inuse,-1,0));
	debug(D_ALLOC,"%sB available",string_metric(avail,-1,0));

	recovery_in_progress = 0;
}

static time_t last_flush_time = 0;

void chirp_alloc_flush()
{
	char *path, *root;
	struct alloc_state *s;

	if(!alloc_enabled) return;

	debug(D_ALLOC,"flushing allocation states...");

	if(!alloc_table) alloc_table = hash_table_create(0,0);

	hash_table_firstkey(alloc_table);
	while(hash_table_nextkey(alloc_table,&path,(void**)&s)) {
		alloc_state_save(path,s);
		hash_table_remove(alloc_table,path);
	}

	if(!root_table) root_table = hash_table_create(0,0);

	hash_table_firstkey(root_table);
	while(hash_table_nextkey(root_table,&path,(void**)&root)) {
		free(root);
		hash_table_remove(root_table,path);
	}

	last_flush_time = time(0);
}

int chirp_alloc_flush_needed()
{
	if(!alloc_enabled) return 0;
	return hash_table_size(alloc_table);
}

time_t chirp_alloc_last_flush_time()
{
	return last_flush_time;
}

INT64_T chirp_alloc_open( const char *path, INT64_T flags, INT64_T mode )
{
	struct alloc_state *a;
	int fd = -1;

	if(!alloc_enabled) return chirp_local_open(path,flags,mode);

	a = alloc_state_cache(path);
	if(a) {
		INT64_T filesize = chirp_local_file_size(path);
		if(filesize<0) filesize = 0;

		fd = chirp_local_open(path,flags,mode);
		if(fd>=0) {
			if(!fd_table) fd_table = itable_create(0);
			itable_insert(fd_table,fd,xstrdup(path));
			if(flags&O_TRUNC) {
				alloc_state_update(a,-space_consumed(filesize));
			}
		}
	} else {
		fd = -1;
	}
	return fd;
}

INT64_T chirp_alloc_close( int fd )
{
	if(!alloc_enabled) return chirp_local_close(fd);

	if(!fd_table) fd_table = itable_create(0);
	char *path = itable_remove(fd_table,fd);
	if(path) free(path);
	chirp_local_close(fd);
	return 0;
}

INT64_T chirp_alloc_pread( int fd, void *buffer, INT64_T length, INT64_T offset )
{
	return chirp_local_pread(fd,buffer,length,offset);
}

INT64_T chirp_alloc_pwrite( int fd, const void *data, INT64_T length, INT64_T offset )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_pwrite(fd,data,length,offset);

	if(!fd_table) fd_table = itable_create(0);

	a  = alloc_state_cache(itable_lookup(fd_table,fd));
	if(a) {
		INT64_T filesize = chirp_local_fd_size(fd);
		if(filesize>=0) {
			INT64_T newfilesize = MAX(length+offset,filesize);
			INT64_T alloc_change = space_consumed(newfilesize) - space_consumed(filesize);
			if(a->avail>=alloc_change) {
				result = chirp_local_pwrite(fd,data,length,offset);
				if(result>0) alloc_state_update(a,alloc_change);
			} else {
				errno = ENOSPC;
				result = -1;
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_sread( int fd, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	return chirp_local_sread(fd,buffer,length,stride_length,stride_skip,offset);
}

INT64_T chirp_alloc_swrite( int fd, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	return chirp_local_swrite(fd,buffer,length,stride_length,stride_skip,offset);
}

INT64_T chirp_alloc_fstat( int fd, struct chirp_stat *buf )
{
	return chirp_local_fstat(fd,buf);
}

INT64_T chirp_alloc_fstatfs( int fd, struct chirp_statfs *info )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_fstatfs(fd,info);

	if(!fd_table) fd_table = itable_create(0);

	a = alloc_state_cache(itable_lookup(fd_table,fd));
	if(a) {
		result = chirp_local_fstatfs(fd,info);
		if(result==0) {
			info->f_blocks = a->size/info->f_bsize;
			info->f_bfree = a->avail/info->f_bsize;
			info->f_bavail = a->avail/info->f_bsize;
		}
	} else {
		result = -1;
	}

	return result;
}


INT64_T chirp_alloc_fchown( int fd, INT64_T uid, INT64_T gid )
{
	return chirp_local_fchown(fd,uid,gid);
}

INT64_T chirp_alloc_fchmod( int fd, INT64_T mode )
{
	return chirp_local_fchmod(fd,mode);
}

INT64_T chirp_alloc_ftruncate( int fd, INT64_T length )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_ftruncate(fd,length);

	if(!fd_table) fd_table = itable_create(0);

	a = alloc_state_cache(itable_lookup(fd_table,fd));
	if(a) {
		INT64_T filesize = chirp_local_fd_size(fd);
		if(filesize>=0) {
			INT64_T alloc_change = space_consumed(length)-space_consumed(filesize);
			if(a->avail>=alloc_change) {
				result = chirp_local_ftruncate(fd,length);
				if(result==0) alloc_state_update(a,alloc_change);
			} else {
				errno = ENOSPC;
				result = -1;
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_fsync( int fd )
{
	return chirp_local_fsync(fd);
}

void *  chirp_alloc_opendir( const char *path )
{
	return chirp_local_opendir(path);
}

char *  chirp_alloc_readdir( void *dir )
{
	return chirp_local_readdir(dir);
}

void    chirp_alloc_closedir( void *dir )
{
	return chirp_local_closedir(dir);
}

INT64_T chirp_alloc_getfile( const char *path, struct link *link, time_t stoptime )
{
	return chirp_local_getfile(path,link,stoptime);
}

INT64_T chirp_alloc_getstream( const char *path, struct link *l, time_t stoptime )
{
	INT64_T fd, result, actual, total = 0;
	int buffer_size = 65536;
	char *buffer;

	fd = chirp_alloc_open(path,O_RDONLY,0700);
	if(fd<0) return fd;

	link_write(l,"0\n",2,stoptime);

	buffer = malloc(buffer_size);

	while(1) {
		result = chirp_alloc_pread(fd,buffer,buffer_size,total);
		if(result<=0) break;

		actual = link_write(l,buffer,result,stoptime);
		if(actual!=result) break;

		total += actual;
	}

	free(buffer);

	chirp_alloc_close(fd);

	return total;
}

/*
Note that putfile is given in advance the size of a file.
It checks the space available, and then guarantees that
the file will either be delivered whole or not at all.
*/

INT64_T chirp_alloc_putfile( const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_putfile(path,link,length,mode,stoptime);

	result = chirp_alloc_unlink(path);
	if(result<0 && errno!=ENOENT) return result;

	a = alloc_state_cache(path);
	if(a) {
		if(a->avail>length) {
			result = chirp_local_putfile(path,link,length,mode,stoptime);
			if(result>0) {
				alloc_state_update(a,space_consumed(result));
			} else {
				chirp_local_unlink(path);
			}
		} else {
			errno = ENOSPC;
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

/*
In contrast, putstream does not know the size of the output in advance,
and simply writes piece by piece, updating the allocation state as it goes.
*/

INT64_T chirp_alloc_putstream( const char *path, struct link *l, time_t stoptime )
{
	INT64_T fd, result, actual, total = 0;
	int buffer_size = 65536;
	char *buffer;

	fd = chirp_alloc_open(path,O_CREAT|O_TRUNC|O_WRONLY,0700);
	if(fd<0) return fd;

	link_write(l,"0\n",2,stoptime);

	buffer = malloc(buffer_size);

	while(1) {
		result = link_read(l,buffer,buffer_size,stoptime);
		if(result<=0) break;

		actual = chirp_alloc_pwrite(fd,buffer,result,total);
		if(actual!=result) break;

		total += actual;
	}

	free(buffer);

	chirp_alloc_close(fd);

	return total;
}

INT64_T chirp_alloc_unlink( const char *path )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_unlink(path);

	a = alloc_state_cache(path);
	if(a) {
		INT64_T filesize = chirp_local_file_size(path);
		if(filesize>=0) {
			result = chirp_local_unlink(path);
			if(result==0) alloc_state_update(a,-space_consumed(filesize));
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_rename( const char *oldpath, const char *newpath )
{
	struct alloc_state *a, *b;
	int result = -1;

	if(!alloc_enabled) return chirp_local_rename(oldpath,newpath);

	a = alloc_state_cache(oldpath);
	if(a) {
		b = alloc_state_cache(newpath);
		if(b) {
			if(a==b) {
				result = rename(oldpath,newpath);
			} else {
				INT64_T filesize = chirp_local_file_size(oldpath);
				if(filesize>=0) {
					if(b->avail>=filesize) {
						result = chirp_local_rename(oldpath,newpath);
						if(result==0) {
							alloc_state_update(a,-space_consumed(filesize));
							alloc_state_update(b,space_consumed(filesize));
						}
						chirp_alloc_flush();
					} else {
						errno = ENOSPC;
						result = -1;
					}
				} else {
					result = -1;
				}
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_link( const char *path, const char *newpath )
{
	if(!alloc_enabled) return chirp_local_link(path,newpath);
	errno = EPERM;
	return -1;
}

INT64_T chirp_alloc_symlink( const char *path, const char *newpath )
{
	return chirp_local_symlink(path,newpath);
}

INT64_T chirp_alloc_readlink( const char *path, char *buf, INT64_T length )
{
	return chirp_local_readlink(path,buf,length);
}

INT64_T chirp_alloc_mkdir( const char *path, INT64_T mode )
{
	return chirp_local_mkdir(path,mode);
}

INT64_T chirp_alloc_rmall( const char *path )
{
	int result;

	result = chirp_alloc_unlink(path);
	if(result==0) {
		return 0;
	} else if(errno!=EISDIR) {
		return -1;
	} else {
		void *dir;
		char *d;
		char subpath[CHIRP_PATH_MAX];

		dir = chirp_alloc_opendir(path);
		if(!dir) return -1;

		result = 0;

		while((d=chirp_alloc_readdir(dir))) {
			if(!strcmp(d,".")) continue;
			if(!strcmp(d,"..")) continue;
			if(!strncmp(d,".__	",3)) continue;
			sprintf(subpath,"%s/%s",path,d);
			result = chirp_alloc_rmall(subpath);
			if(result!=0) break;
		}

		chirp_alloc_closedir(dir);

		if(result==0) {
			return chirp_alloc_rmdir(path);
		} else {
			return result;
		}
	}
}

INT64_T chirp_alloc_rmdir( const char *path )
{
	struct alloc_state *a, *d;
	int result=-1;

	if(!alloc_enabled) return chirp_local_rmdir(path);

	d = alloc_state_cache_exact(path);
	if(d) {
		a = alloc_state_cache(path);
		if(a) {
			if(chirp_local_rmdir(path)==0) {
				if(d!=a) {
					alloc_state_update(a,-d->size);
					debug(D_ALLOC,"rmalloc %s %lld",path,d->size);
				}
				chirp_alloc_flush();
				result = 0;
			} else {
				result = -1;
			}
		}
	}
	return result;
}

INT64_T chirp_alloc_stat( const char *path, struct chirp_stat *buf )
{
	return chirp_local_stat(path,buf);
}

INT64_T chirp_alloc_lstat( const char *path, struct chirp_stat *buf )
{
	return chirp_local_lstat(path,buf);
}

INT64_T chirp_alloc_statfs( const char *path, struct chirp_statfs *info )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_statfs(path,info);

	a = alloc_state_cache(path);
	if(a) {
		result = chirp_local_statfs(path,info);
		if(result==0) {
			info->f_blocks = a->size/info->f_bsize;
			info->f_bavail = a->avail/info->f_bsize;
			info->f_bfree = a->avail/info->f_bsize;
			if(a->avail<0) {
				info->f_bavail = 0;
				info->f_bfree = 0;
			}
		}
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_alloc_mkfifo( const char *path )
{
        return chirp_local_mkfifo(path);
}

INT64_T chirp_alloc_access( const char *path, INT64_T mode )
{
	return chirp_local_access(path,mode);
}

INT64_T chirp_alloc_chmod( const char *path, INT64_T mode )
{
	return chirp_local_chmod(path,mode);
}

INT64_T chirp_alloc_chown( const char *path, INT64_T uid, INT64_T gid )
{
	return chirp_local_chown(path,uid,gid);
}

INT64_T chirp_alloc_lchown( const char *path, INT64_T uid, INT64_T gid )
{
	return chirp_local_lchown(path,uid,gid);
}

INT64_T chirp_alloc_truncate( const char *path, INT64_T newsize )
{
	struct alloc_state *a;
	int result;

	if(!alloc_enabled) return chirp_local_truncate(path,newsize);

	a = alloc_state_cache(path);
	if(a) {
		INT64_T filesize = chirp_local_file_size(path);
		if(filesize>=0) {
			INT64_T alloc_change = space_consumed(newsize)-space_consumed(filesize);
			if(a->avail>=alloc_change) {
				result = chirp_local_truncate(path,newsize);
				if(result==0) alloc_state_update(a,alloc_change);
			} else {
				errno = ENOSPC;
				result = -1;
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_utime( const char *path, time_t actime, time_t modtime )
{
	return chirp_local_utime(path,actime,modtime);
}

INT64_T chirp_alloc_md5( const char *path, unsigned char digest[16] )
{
	return chirp_local_md5(path,digest);
}

INT64_T chirp_alloc_lsalloc( const char *path, char *alloc_path, INT64_T *total, INT64_T *inuse )
{
	char *name;
	struct alloc_state *a;
	int result=-1;

	if(!alloc_enabled) {
		errno = ENOSYS;
		return -1;
	}

	name = alloc_state_root_cached(path);
	if(name) {
		a = alloc_state_cache_exact(name);
		if(a) {
			strcpy(alloc_path,name);
			*total = a->size;
			*inuse = a->inuse;
			result = 0;
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_alloc_mkalloc( const char *path, INT64_T size, INT64_T mode )
{
	struct alloc_state *a;
	int result=-1;

	if(!alloc_enabled) {
		errno = ENOSYS;
		return -1;
	}

	a = alloc_state_cache(path);
	if(a) {
		if(a->avail>size) {
			result = chirp_local_mkdir(path,mode);
			if(result==0) {
				if(alloc_state_create(path,size)) {
					alloc_state_update(a,size);
					debug(D_ALLOC,"mkalloc %s %lld",path,size);
					chirp_alloc_flush();
				} else {
					result = -1;
				}
			}
		} else {
			errno = ENOSPC;
			return -1;
		}
	} else {
		return -1;
	}

	return result;
}

INT64_T chirp_alloc_file_size( const char *path )
{
	return chirp_local_file_size(path);
}

INT64_T chirp_alloc_fd_size( int fd )
{
	return chirp_local_fd_size(fd);
}
