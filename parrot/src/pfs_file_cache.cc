/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_file.h"
#include "pfs_file_cache.h"
#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
#include "sleeptools.h"
#include "file_cache.h"
#include "full_io.h"
#include "hash_table.h"
}

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <errno.h>
#include <stdlib.h>
#include <utime.h>
#include <time.h>

extern struct file_cache *pfs_file_cache;
extern int pfs_session_cache;
extern int pfs_master_timeout;

static struct hash_table * not_found_table = 0;

#define BUFFER_SIZE 65536

static pfs_ssize_t copy_fd_to_file( int fd, pfs_file *file )
{
	pfs_ssize_t ractual, wactual, offset = 0;
	char buffer[BUFFER_SIZE];

	while(1) {
		ractual = full_pread64(fd,buffer,sizeof(buffer),offset);
		if(ractual<=0) return ractual;

		wactual = file->write(buffer,ractual,offset);
		if(wactual>=0) {
			offset += wactual;
		} else {
			return -1;
		}
	}
}

static pfs_ssize_t copy_file_to_fd( pfs_file *file, int fd )
{
	pfs_ssize_t ractual, wactual, offset = 0;
	char buffer[BUFFER_SIZE];

	while(1) {
		ractual = file->read(buffer,sizeof(buffer),offset);
		if(ractual<=0) return ractual;

		wactual = full_pwrite64(fd,buffer,ractual,offset);
		if(wactual>=0) {
			offset += wactual;
		} else {
			return -1;
		}
	}
}

class pfs_file_cached : public pfs_file
{
private:
	int fd;
	int mode;
	int changed;
	time_t ctime;
	ino_t inode;

public:
	pfs_file_cached( pfs_name *n, int f, int m, time_t c, ino_t i ) : pfs_file(n) {
		fd = f;
		mode = m;
		changed = 0;
		ctime = c;
		inode = i;
	}

	virtual int close() {
		int result = -1;
		if(changed) {
			debug(D_CACHE,"storing %s",name.path);
			pfs_file *wfile = name.service->open(&name,O_WRONLY|O_CREAT|O_TRUNC,mode);
			if(wfile) {
				if(copy_fd_to_file(fd,wfile)==0) {
					result = 0;
				} else {
					result = -1;
				}
				wfile->close();
				delete wfile;
			} else {
				result = -1;
			}
		} else {
			result = 0;
		}
		/* give it a dummy truncate to update the mtime */
		/* this will prevent a later fetch of the same file */
		//::ftruncate64(fd,this->get_size());
		::close(fd);
		return result;
	}

	virtual pfs_ssize_t read( void *d, pfs_size_t length, pfs_off_t offset ) {
		return ::full_pread64(fd,d,length,offset);
	}

	virtual pfs_ssize_t write( const void *d, pfs_size_t length, pfs_off_t offset ) {
		changed = 1;
		return ::full_pwrite64(fd,d,length,offset);
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct stat64 lbuf;
		result = ::fstat64(fd,&lbuf);
		if(result>=0) {
			COPY_STAT(lbuf,*buf);
			buf->st_ctime = ctime;
			buf->st_ino = inode;
		}
		return result;
	}

	virtual int fstatfs( struct pfs_statfs *buf ) {
		struct statfs64 lbuf;
		int result = ::fstatfs64(fd,&lbuf);
		if(result>=0){
				COPY_STATFS(lbuf,*buf);
		}
		return result;
	}

	virtual int ftruncate( pfs_size_t length ) {
		changed = 1;
		return ::ftruncate64(fd,length);
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat buf;
		if(this->fstat(&buf)==0) {
			return buf.st_size;
		} else {
			return 0;
		}
	}

	virtual int get_local_name( char *n ) {
		return file_cache_contains(pfs_file_cache,name.path,n);
	}

	virtual int is_seekable() {
		return 1;
	}
};

pfs_file * pfs_cache_open( pfs_name *name, int flags, mode_t mode )
{
	struct pfs_stat buf;
	char txn[PFS_PATH_MAX];
	int fd, ok_to_fail;
	struct pfs_file *rfile, *result = NULL;
	struct utimbuf ut;
	int sleep_time = 1;

	retry:

	buf.st_ctime = time(0);
	buf.st_size = 0;
	buf.st_ino = hash_string(name->rest);

	if(pfs_session_cache) {
		if(!not_found_table) not_found_table = hash_table_create(0,0);

		if(!(flags&O_CREAT)) {
			if(hash_table_lookup(not_found_table,name->path)) {
				errno = ENOENT;
				return 0;
			}
		}
	} else {
		if(name->service->stat(name,&buf)!=0) {
			if(flags&O_CREAT && errno==ENOENT) {
				buf.st_mtime = 0;
				buf.st_size = 0;
			} else {
				return 0;
			}
		}
	}


	fd = file_cache_open(pfs_file_cache,name->path,flags,txn,buf.st_size,0);
	if(fd>=0) {
		if(flags&O_TRUNC) ftruncate(fd,0);
		return new pfs_file_cached(name,fd,mode,buf.st_ctime,buf.st_ino);
	} else {
		debug(D_DEBUG, "file cache lookup failed: %s", strerror(errno));
	}

	debug(D_CACHE,"loading %s",name->path);

	fd = file_cache_begin(pfs_file_cache,name->path,txn);
	if(fd<0) return 0;

	if(flags&O_TRUNC) {
		rfile = 0;
		ok_to_fail = 1;
	} else if(flags&O_CREAT) {
		rfile = name->service->open(name,O_RDONLY,0);
		ok_to_fail = 1;
	} else {
		rfile = name->service->open(name,O_RDONLY,0);
		ok_to_fail = 0;
	}

	if(rfile) {
		if(copy_file_to_fd(rfile,fd)==0) {
			if(rfile->close()<0) {
				file_cache_abort(pfs_file_cache,name->path,txn);
				if(sleep_time<pfs_master_timeout) {
					debug(D_CACHE,"filesystem inconsistent, retrying in %d seconds\n",sleep_time);
					sleep_for(sleep_time);
					sleep_time *= 2;
					goto retry;
				} else {
					fatal("filesystem inconsistent after retrying for %d seconds\n",pfs_master_timeout);
				}
			} else {
				/* Otherwise, update the local modification time, */
				/* and commit the cache store operation. */
				ut.actime = buf.st_atime;
				ut.modtime = buf.st_mtime;
				::utime(txn,&ut);
				if(file_cache_commit(pfs_file_cache,name->path,txn)==0) {
					result = new pfs_file_cached(name,fd,mode,buf.st_ctime,buf.st_ino);
				} else {
					result = 0;
				}
			}
		} else {
			rfile->close();
			result = 0;
		}

		int save_errno = errno;
		delete rfile;
		errno = save_errno;
	} else if(ok_to_fail) {
		result = name->service->open(name,flags,mode);
		if(result) {
			result->close();
			result = new pfs_file_cached(name,fd,mode,buf.st_ctime,buf.st_ino);
			if(result) result->ftruncate(0);
		}
	} else {
		result = 0;
	}

	if(result) {
		return result;
	} else {
		close(fd);
		file_cache_abort(pfs_file_cache,name->path,txn);
		if(pfs_session_cache && errno==ENOENT) {
			hash_table_insert(not_found_table,name->path,(void*)1);
		}
		return 0;
	}
}

int pfs_cache_invalidate( pfs_name *name )
{
	if(!name->is_local) {
		if(pfs_session_cache) {
			if(!not_found_table) not_found_table = hash_table_create(0,0);
			hash_table_remove(not_found_table,name->path);
		}
		return file_cache_delete(pfs_file_cache,name->path);
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=4: */
