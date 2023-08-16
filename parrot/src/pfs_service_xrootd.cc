/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Important: This define must come before all include files,
in order to indicate that we are working with the 64-bit definition
of file sizes in struct stat and other places as needed by xrootd.
Note that we do not define it globally, so that we are sure not to break other
people's include files.  Note also that we are careful to use
struct pfs_stat in our own headers, so that our own code isn't
sensitive to this setting.
*/

#define _FILE_OFFSET_BITS 64

#ifdef HAS_XROOTD

#include "pfs_service.h"
#include "pfs_types.h"

extern "C" {
#include "debug.h"
#include "domain_name.h"
#include "file_cache.h"
#include "full_io.h"
#include "http_query.h"
#include "link.h"
#include "stringtools.h"
}

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <string.h>
#include <XrdPosix/XrdPosixExtern.hh>

#define XROOTD_FILE_MODE (S_IFREG | 0555)
#define XROOTD_DEFAULT_PORT 1094

static char *translate_file_to_xrootd(pfs_name * name)
{
	char file_buf[PFS_PATH_MAX];
	int port_number = XROOTD_DEFAULT_PORT;

	if(name->port != 0) {
		port_number = name->port;
	}

	sprintf(file_buf, "root://%s:%i/%s", name->host, port_number, name->rest);

	return strdup(file_buf);
}


class pfs_file_xrootd:public pfs_file {
private:
	int file_handle;

public:
	pfs_file_xrootd(pfs_name * n, int file_handle):pfs_file(n) {
		this->file_handle = file_handle;
	}

	virtual int close() {
		debug(D_XROOTD, "close %i", this->file_handle);
		return XrdPosix_Close(this->file_handle);
	}

	virtual pfs_ssize_t read(void *d, pfs_size_t length, pfs_off_t offset) {
		debug(D_XROOTD, "pread %d %" PRId64 " %" PRId64,this->file_handle,length,offset);
		return XrdPosix_Pread(this->file_handle,d,length,offset);
	}

	virtual pfs_ssize_t write(const void *d, pfs_size_t length, pfs_off_t offset) {
		debug(D_XROOTD, "pwrite %d %" PRId64 " %" PRId64,this->file_handle,length,offset);
		return XrdPosix_Pwrite(this->file_handle,d,length,offset);
	}

	virtual int fstat(struct pfs_stat *buf) {
		debug(D_XROOTD, "fstat %d",this->file_handle);
		struct stat lbuf;
		int result = XrdPosix_Fstat(this->file_handle, &lbuf);
		if(result == 0) COPY_STAT(lbuf, *buf);
		return result;
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat buf;
		if(this->fstat(&buf) == 0) {
			return buf.st_size;
		} else {
			return -1;
		}
	}


};

class pfs_service_xrootd:public pfs_service {
public:
	virtual pfs_file * open(pfs_name * name, int flags, mode_t mode) {
		int file_handle;
		char *file_url = NULL;

		debug(D_XROOTD, "Opening file: %s", name->rest);

		if((flags & O_ACCMODE) != O_RDONLY) {
			errno = EROFS;
			return 0;
		}

		file_url = translate_file_to_xrootd(name);
		file_handle = XrdPosix_Open(file_url, flags, mode);
		free(file_url);

		if(file_handle>=0) {
			return new pfs_file_xrootd(name, file_handle);
		} else {
			return 0;
		}

	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		DIR *dir;
		struct dirent *d;

		char *file_url = NULL;
		file_url = translate_file_to_xrootd(name);

		debug(D_XROOTD,"getdir %s",file_url);

		dir = XrdPosix_Opendir(file_url);
		if(!dir) {
			free(file_url);
			return 0;
		}

		pfs_dir *pdir = new pfs_dir(name);

		while((d=XrdPosix_Readdir(dir))) {
			pdir->append(d->d_name);
		}
		XrdPosix_Closedir(dir);

		free(file_url);

		return pdir;
	}

	virtual int statfs(pfs_name * name, struct pfs_statfs *buf) {
		struct statfs lbuf;
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "statfs %s",file_url);
		int result = XrdPosix_Statfs((const char *) file_url, &lbuf);
		free(file_url);
		COPY_STATFS(lbuf, *buf);
		return result;
	}

	virtual int stat(pfs_name * name, struct pfs_stat *buf) {
		struct stat lbuf;
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "stat %s",file_url);
		int result = XrdPosix_Stat((const char *) file_url, &lbuf);
		free(file_url);
		COPY_STAT(lbuf, *buf);
		return result;
	}

	virtual int lstat(pfs_name * name, struct pfs_stat *buf) {
		return this->stat(name, buf);
	}

	virtual int unlink( pfs_name *name ) {
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "unlink %s",file_url);
		int result = XrdPosix_Unlink((const char *) file_url);
		free(file_url);
		return result;
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "access %s %o",file_url,mode);
		int result = XrdPosix_Access((const char *) file_url,mode);
		free(file_url);
		return result;
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "truncate %s %lld",file_url,(long long)length);
		int result = XrdPosix_Truncate(file_url,length);
		free(file_url);
		return result;
	}

	virtual int chdir( pfs_name *name, char *newpath ) {
		struct pfs_stat info;
		if(this->stat(name,&info)>=0) {
			if(S_ISDIR(info.st_mode)) {
				sprintf(newpath,"/%s/%s:%d%s",name->service_name,name->host,name->port,name->rest);
				return 0;
			} else {
				errno = ENOTDIR;
				return -1;
			}
		} else {
			return -1;
		}
	}

	virtual int rename( pfs_name *oldname, pfs_name *newname ) {
		char *old_url = translate_file_to_xrootd(oldname);
		char *new_url = translate_file_to_xrootd(newname);

		debug(D_XROOTD,"rename %s %s",old_url,new_url);
		int result = XrdPosix_Rename(old_url,new_url);

		free(old_url);
		free(new_url);

		return result;
	}

	virtual int mkdir( pfs_name *name, mode_t mode) {
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "mkdir %s %o",file_url,mode);
		int result = XrdPosix_Mkdir((const char *) file_url,mode);
		free(file_url);
		return result;
	}

	virtual int rmdir( pfs_name *name ) {
		char *file_url = translate_file_to_xrootd(name);
		debug(D_XROOTD, "rmdir %s",file_url);
		int result = XrdPosix_Rmdir((const char *) file_url);
		free(file_url);
		return result;
	}

	virtual int is_seekable() {
		return 1;
	}


};

static pfs_service_xrootd pfs_service_xrootd_instance;
pfs_service *pfs_service_xrootd = &pfs_service_xrootd_instance;

#endif

/* vim: set noexpandtab tabstop=8: */
