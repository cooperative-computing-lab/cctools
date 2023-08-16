/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
/*
Thanks to Ulrich Schwickeratch (ulrich.schwickerath@web.de)
for contributing this RFIO driver!
*/

/*
Note that this driver is deprecated in favor of
pfs_service_gfal, which implements rfio and several
other protocols using the egee software stack.
*/


#ifdef HAS_RFIO

extern "C" {
#include "rfio_api.h"
#include "debug.h"
}

#include "pfs_service.h"
#include "pfs_file.h"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>

class pfs_file_rfio : public pfs_file
{
private:
	int fd;
	int anyseek;
	pfs_off_t remote_offset;

public:
	pfs_file_rfio( pfs_name *n, int f ) : pfs_file(n) {
		fd = f;
		remote_offset = 0;
		anyseek = 0;
	}

	virtual int close() {
		int result;
		debug(D_RFIO,"close %d",fd);
		result = ::rfio_close(fd);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}

	pfs_off_t setpos( pfs_off_t offset ) {
		if(anyseek || remote_offset!=offset) {
			anyseek = 1;
			debug(D_RFIO,"lseek %d %d %d",fd,offset,SEEK_SET);
			int result =::rfio_lseek(fd,offset,SEEK_SET);
			debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
			if(result>=0) {
				remote_offset = offset;
				return remote_offset;
			} else {
				return -1;
			}
		} else {
			return remote_offset;
		}
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		int result;
		if(setpos(offset)<0) return -1;
		debug(D_RFIO,"read %d %d",fd,length);
		result =::rfio_read(fd,(char*)data,length);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>0) remote_offset += result;
		return result;
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		int result;
		if(setpos(offset)<0) return -1;
		debug(D_RFIO,"write %d %d",fd,length);
		result =::rfio_write(fd,(char*)data,length);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>0) remote_offset += result;
		return result;
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct stat lbuf;
		debug(D_RFIO,"fstat %d",fd);
		result =::rfio_fstat(fd,&lbuf);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>=0) COPY_STAT(lbuf,*buf);
		return result;
	}

	virtual int fchmod( mode_t mode ) {
		int result;
		debug(D_RFIO,"fchmod %d %d",fd,mode);
		result =::rfio_fchmod(fd,mode);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		int result;
		debug(D_RFIO,"fchown %d %d %d",fd,uid,gid);
		result =::rfio_fchown(fd,uid,gid);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}

	virtual pfs_ssize_t get_size() {
		struct stat s;
		int result;
		result =::rfio_fstat(fd,&s);
		if(result<0) {
			return 0;
		} else {
			return s.st_size;
		}
	}
};

class pfs_service_rfio : public pfs_service {
private:
	char path[PFS_PATH_MAX];
	void convert_name( pfs_name *name, char *path ) {
		if (!strcmp(name->service_name, "castor")) {
			sprintf(path, "/castor/%s/%s", name->host, name->rest);
		} else {
			if(name->host[0]) {
				sprintf(path,"%s:%s",name->host,name->rest);
			} else {
				strcpy(path,"/");
			}
		}
	}

public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		int result;
		char path[PFS_PATH_MAX];
		convert_name(name,path);
		debug(D_RFIO,"open %s %d %d",path,flags,mode);
		result =::rfio_open(path,flags,mode);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>=0) {
			return new pfs_file_rfio(name,result);
		} else {
			return 0;
		}
	}

	virtual pfs_dir * getdir( pfs_name *name )
	{
		char path[PFS_PATH_MAX];
		convert_name(name,path);
		pfs_dir *dirob;
		struct dirent *d;
		DIR *dir;

		debug(D_RFIO,"opendir %s",path);

		dir = rfio_opendir(path);
		if(dir) {
			dirob = new pfs_dir(name);
			debug(D_RFIO,"readdir");
			while((d=rfio_readdir(dir))) {
				debug(D_RFIO,"= %s",d->d_name);
				dirob->append(d->d_name);
				debug(D_RFIO,"readdir");
			}
			debug(D_RFIO,"= 0");
			rfio_closedir(dir);
		} else {
			debug(D_RFIO,"= %s",strerror(errno));
			dirob = 0;
		}
		return dirob;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat lbuf;
		convert_name(name,path);
		debug(D_RFIO,"stat %s",path);
		result =::rfio_stat(path,&lbuf);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>=0) COPY_STAT(lbuf,*buf);
		return result;
	}
	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat lbuf;
		convert_name(name,path);
		debug(D_RFIO,"lstat %s",path);
		result =::rfio_lstat(path,&lbuf);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		if(result>=0) COPY_STAT(lbuf,*buf);
		return result;
	}
	virtual int access( pfs_name *name, mode_t mode ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"access %s %d",path,mode);
		result =::rfio_access(path,mode);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int chmod( pfs_name *name, mode_t mode ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"chmod %s %d",path,mode);
		result =::rfio_chmod(path,mode);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int readlink( pfs_name *name, char *buf, pfs_size_t size ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"readlink %s %d",path,size);
		result =::rfio_readlink(path,buf,size);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"mkdir %s %d",path,mode);
		result =::rfio_mkdir(path,mode);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int rmdir( pfs_name *name ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"rmdir %s",path);
		result =::rfio_rmdir(path);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int unlink( pfs_name *name ) {
		int result;
		convert_name(name,path);
		debug(D_RFIO,"unlink %s",path);
		result =::rfio_unlink(path);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}
	virtual int rename( pfs_name *name, pfs_name *newname ) {
		int result;
		char newpath[PFS_PATH_MAX];
		convert_name(name,path);
		convert_name(newname,newpath);
		debug(D_RFIO,"rename %s %s",path,newpath);
		result =::rfio_rename(path,newpath);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}

	/*
	Surprise, rfio_chdir and rfio_getcwd do not have remote
	counterparts, only local and HSM.  So, instead, we just
	stat to see if it is a directory that we can pass through.
	*/

	virtual int chdir( pfs_name *name, char *newpath ) {
		int result;
		struct pfs_stat buf;
		result = this->stat(name,&buf);
		if(result>=0) {
			if(S_ISDIR(buf.st_mode)) {
				result = this->access(name,X_OK);
				if(result>=0) {
					strcpy(newpath,name->path);
				} else {
					result = -1;
					errno = EACCES;
				}
			} else {
				result = -1;
				errno = ENOTDIR;
			}
		}
		return result;
	}
	virtual int symlink( const char *linkname, pfs_name *newname ) {
		int result;
		char newpath[PFS_PATH_MAX];
		convert_name(newname,newpath);
		debug(D_RFIO,"symlink %s %s",linkname,newpath);
		result =::rfio_symlink((char*)linkname,newpath);
		debug(D_RFIO,"= %d %s",result,result>=0?"":strerror(errno));
		return result;
	}

	virtual int is_seekable() {
		return 1;
	}
};

static pfs_service_rfio pfs_service_rfio_instance;
pfs_service *pfs_service_rfio = &pfs_service_rfio_instance;

#endif

/* vim: set noexpandtab tabstop=8: */
