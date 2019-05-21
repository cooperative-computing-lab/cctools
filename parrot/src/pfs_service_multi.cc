/*
Copyright (C) 2004 Douglas Thain
This work is made available under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "chirp_multi.h"
#include "stringtools.h"
#include "debug.h"
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

class pfs_file_multi : public pfs_file
{
private:
	struct chirp_file *file;

public:
	pfs_file_multi( pfs_name *name, struct chirp_file *f ) : pfs_file(name) {
		file = f;
	}

	virtual int close() {
		return chirp_multi_close(file,time(0)+pfs_master_timeout);
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		return chirp_multi_pread(file,data,length,offset,time(0)+pfs_master_timeout);
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		return chirp_multi_pwrite(file,data,length,offset,time(0)+pfs_master_timeout);
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct chirp_stat cbuf;
		result = chirp_multi_fstat(file,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int fstatfs( struct pfs_statfs *buf ) {
		int result;
		struct chirp_statfs cbuf;
		result = chirp_multi_fstatfs(file,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_STATFS(cbuf,*buf);
		}
		return result;
	}

	virtual int ftruncate( pfs_size_t length ) {
		return chirp_multi_ftruncate(file,length,time(0)+pfs_master_timeout);
	}

	virtual int fchmod( mode_t mode ) {
		return chirp_multi_fchmod(file,mode,time(0)+pfs_master_timeout);
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		return chirp_multi_fchown(file,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual int fsync() {
		/* sync not needed because of synchronous writes */
		return 0;
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

static void add_to_dir( const char *path, void *arg )
{
	pfs_dir *dir = (pfs_dir *)arg;
	dir->append(path);
}

static void add_to_acl( const char *entry, void *vbuf )
{
	char *buf = (char*)vbuf;
	strcat(buf,entry);
	strcat(buf,"\n");
}

class pfs_service_multi : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		struct chirp_file *file;
		file = chirp_multi_open(name->hostport,name->rest,flags,mode,time(0)+pfs_master_timeout);
		if(file) {
			return new pfs_file_multi(name,file);
		} else {
			return 0;
		}
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		int result=-1;
		pfs_dir *dir = new pfs_dir(name);
		result = chirp_multi_getdir(name->hostport,name->rest,add_to_dir,dir,time(0)+pfs_master_timeout);
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
		result = chirp_multi_statfs(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_STATFS(cbuf,*buf);
		}
		return result;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct chirp_stat cbuf;
		result = chirp_multi_stat(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct chirp_stat cbuf;
		result = chirp_multi_lstat(name->hostport,name->rest,&cbuf,time(0)+pfs_master_timeout);
		if(result==0) {
				COPY_CSTAT(cbuf,*buf);
		}
		return result;
	}

	virtual int unlink( pfs_name *name ) {
		return chirp_multi_unlink(name->hostport,name->rest,time(0)+pfs_master_timeout);
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		return chirp_multi_access(name->hostport,name->rest,mode,time(0)+pfs_master_timeout);
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		return chirp_multi_chmod(name->hostport,name->rest,mode,time(0)+pfs_master_timeout);
	}

	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		return chirp_multi_chown(name->hostport,name->rest,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		return chirp_multi_lchown(name->hostport,name->rest,uid,gid,time(0)+pfs_master_timeout);
	}

	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		return chirp_multi_truncate(name->hostport,name->rest,length,time(0)+pfs_master_timeout);
	}

	virtual int utime( pfs_name *name, struct utimbuf *t ) {
		return chirp_multi_utime(name->hostport,name->rest,t->actime,t->modtime,time(0)+pfs_master_timeout);
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		return chirp_multi_rename(name->hostport,name->rest,newname->rest,time(0)+pfs_master_timeout);
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result=-1;
		struct pfs_stat statbuf;
		if(this->stat(name,&statbuf)>=0) {
			if(S_ISDIR(statbuf.st_mode)) {
				sprintf(newname,"/%s/%s:%d%s",name->service_name,name->hostport,name->port,name->rest);
				result = 0;
			} else {
				errno = ENOTDIR;
				result = -1;
			}
		}
		return result;
	}

	virtual int link( pfs_name *name, pfs_name *newname ) {
		return chirp_multi_link(name->hostport,name->rest,newname->rest,time(0)+pfs_master_timeout);
	}

	virtual int symlink( const char *linkname, pfs_name *newname ) {
		return chirp_multi_symlink(newname->hostport,linkname,newname->rest,time(0)+pfs_master_timeout);
	}

	virtual int readlink( pfs_name *name, char *buf, pfs_size_t length ) {
		int result=-1;
		result = chirp_multi_readlink(name->hostport,name->rest,buf,length,time(0)+pfs_master_timeout);
		/* Fix up relative-rooted paths to match our view of things */
		if(result>0 && buf[0]=='/') {
			char tmp[PFS_PATH_MAX];
			buf[result] = 0;
			strcpy(tmp,buf);
			sprintf(buf,"/%s/%s%s",name->service_name,name->hostport,tmp);
			result = strlen(buf);
		}
		return result;
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		return chirp_multi_mkdir(name->hostport,name->rest,mode,time(0)+pfs_master_timeout);
	}

	virtual int rmdir( pfs_name *name ) {
		return chirp_multi_rmdir(name->hostport,name->rest,time(0)+pfs_master_timeout);
	}

	virtual int whoami( pfs_name *name, char *buf, int size ) {
		return chirp_multi_whoami(name->hostport,buf,size,time(0)+pfs_master_timeout);
	}

	virtual int getacl( pfs_name *name, char *buf, int size ) {
		int result;
		buf[0] = 0;
		result = chirp_multi_getacl(name->hostport,name->rest,add_to_acl,buf,time(0)+pfs_master_timeout);
		if(result==0) result = strlen(buf);
		return result;
	}

	virtual int setacl( pfs_name *name, const char *subject, const char *rights ) {
		return chirp_multi_setacl(name->hostport,name->rest,subject,rights,time(0)+pfs_master_timeout);
	}

	virtual pfs_location* locate( pfs_name *name) {
		int result = -1;
		pfs_location *loc = new pfs_location();

		result = chirp_multi_locate(name->hostport,name->rest,add_to_loc,(void*)loc,time(0)+pfs_master_timeout);

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

static pfs_service_multi pfs_service_multi_instance;
pfs_service *pfs_service_multi = &pfs_service_multi_instance;

/* vim: set noexpandtab tabstop=4: */
