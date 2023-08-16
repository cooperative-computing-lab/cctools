/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_IRODS

extern "C" {
#include "irods_reli.h"
#include "debug.h"
}

#include "pfs_table.h"
#include "pfs_service.h"

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <sys/statfs.h>

class pfs_file_irods : public pfs_file
{
private:
	struct irods_file *ifile;

public:
	pfs_file_irods( pfs_name *name, irods_file *i ) : pfs_file(name) {
		ifile = i;
	}

	virtual int close() {
		return irods_reli_close(ifile);
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		return irods_reli_pread(ifile,(char*)data,length,offset);
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		return irods_reli_pwrite(ifile,(char*)data,length,offset);
	}

	virtual int fstat( struct pfs_stat *info ) {
		return name.service->stat(&name,info);
	}

	virtual int fstatfs( struct pfs_statfs *info ) {
		return name.service->statfs(&name,info);
	}

	virtual int ftruncate( pfs_size_t length ) {
		return name.service->truncate(&name,length);
	}

	virtual int fchmod( mode_t mode ) {
		return name.service->chmod(&name,mode);
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		return 0;
	}

	virtual int fsync() {
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat info;
		if(this->fstat(&info)==0) {
			return info.st_size;
		} else {
			return -1;
		}
	}
};

static void add_one_dir( const char *path, void *arg )
{
	pfs_dir *dir = (pfs_dir*) arg;
	dir->append(path);
}

class pfs_service_irods : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {

		struct irods_file *ifile;

		ifile = irods_reli_open(name->hostport,name->rest,flags,mode);
		if(ifile) {
			return new pfs_file_irods(name,ifile);
		} else {
			return 0;
		}
	}

	virtual pfs_dir * getdir( pfs_name *name ) {

		pfs_dir *dir = new pfs_dir(name);
		int result;

		result = irods_reli_getdir(name->hostport,name->rest,add_one_dir,dir);

		if(result<0) {
			delete dir;
			return 0;
		} else {
			return dir;
		}

	}

	virtual int statfs( pfs_name *name, struct pfs_statfs *info ) {
		return irods_reli_statfs(name->hostport,name->rest,info);
	}

	virtual int stat( pfs_name *name, struct pfs_stat *info ) {
		return irods_reli_stat(name->hostport,name->rest,info);
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *info ) {
		return this->stat(name,info);
	}

	virtual int unlink( pfs_name *name ) {
		return irods_reli_unlink(name->hostport,name->rest);
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		struct pfs_stat info;
		return this->stat(name,&info);
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		return 0;
	}

	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		return 0;
	}

	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		return 0;
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		return irods_reli_truncate(name->hostport,name->rest,length);
	}

	virtual int utime( pfs_name *name, struct utiminfo *t ) {
		return 0;
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		if(strcmp(name->hostport,name->hostport)) {
			errno = EXDEV;
			return -1;
		}
		return irods_reli_rename(name->hostport,name->rest,newname->rest);
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
		errno = ENOSYS;
		return -1;
	}

	virtual int symlink( const char *linkname, pfs_name *newname ) {
		errno = ENOSYS;
		return -1;
	}

	virtual int readlink( pfs_name *name, char *info, pfs_size_t length ) {
		errno = EINVAL;
		return -1;
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		return irods_reli_mkdir(name->hostport,name->rest);
	}

	virtual int rmdir( pfs_name *name ) {
		return irods_reli_rmdir(name->hostport,name->rest);
	}

	virtual int md5( pfs_name *name, char *digest) {
		return irods_reli_md5(name->hostport,name->rest,digest);
	}

	virtual pfs_ssize_t putfile( pfs_name *source, pfs_name *target ) {
		return irods_reli_putfile(target->hostport,target->rest,source->path);
	}

	virtual pfs_ssize_t getfile( pfs_name *source, pfs_name *target ) {
		return irods_reli_getfile(source->hostport,source->rest,target->path);
	}

	virtual int get_default_port() {
		return 1247;
	}

	virtual int get_block_size() {
		return 1048576;
	}

	virtual int is_seekable() {
		return 1;
	}

};

static pfs_service_irods pfs_service_irods_instance;
pfs_service *pfs_service_irods = &pfs_service_irods_instance;

#endif

/* vim: set noexpandtab tabstop=8: */
