/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Note that this module supports a variety of URLs,
all implemented by the EGEE GFAL library.
*/

#ifdef HAS_EGEE

#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "gfal_api.h"
#include "stringtools.h"
#include "debug.h"
#include "xxmalloc.h"
#include "macros.h"
}

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <sys/statfs.h>

void format_name( pfs_name *name, char *path )
{
	if(!strcmp(name->service_name,"lfn")) {
		sprintf(path,"lfn:/%s%s",name->host,name->rest);
	} else if(!strcmp(name->service_name,"guid")) {
		sprintf(path,"guid:%s",name->host);
	} else if(!strcmp(name->service_name,"gfal")) {
		sprintf(path,"%s",name->path+6);
	} else {
		/* note double slashes needed for srm, dcap, and rfio */
		/* name->rest always begins with a slash */
		sprintf(path,"%s://%s/%s",
			name->service_name,
			name->port==0 ? name->host : name->hostport,
			name->rest);
	}
}


class pfs_file_gfal : public pfs_file
{
private:
	int gfd;
	char *buffer;
	pfs_off_t current_offset;

public:
	pfs_file_gfal( pfs_name *name, int f ) : pfs_file(name) {
		gfd = f;
		buffer = 0;
		current_offset = 0;
	}

	virtual int close() {
		int result;
		debug(D_GFAL,"close %d",gfd);
		result = gfal_close(gfd);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;
		debug(D_GFAL,"read %d %x %d %d",gfd,data,length,offset);
		if(offset!=current_offset) gfal_lseek(gfd,offset,SEEK_SET);
		result = gfal_read(gfd,data,length);
		if(result>0) current_offset+=result;
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;
		debug(D_GFAL,"write %d %x %d %d",gfd,data,length,offset);
		if(offset!=current_offset) gfal_lseek(gfd,offset,SEEK_SET);
		result = gfal_write(gfd,data,length);
		if(result>0) current_offset+=result;
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int fstat( struct pfs_stat *buf ) {
		pfs_service_emulate_stat(&name,buf);
		buf->st_size = this->get_size();
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		current_offset = gfal_lseek(gfd,0,SEEK_END);
		return current_offset;
	}
};

class pfs_service_gfal : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		int gfd;

		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"open %s %d %d",gfalname,flags,mode);
		gfd = gfal_open(gfalname,flags,mode);
		debug(D_GFAL,"= %d",gfd);
		if(gfd>=0) {
			return new pfs_file_gfal(name,gfd);
		} else {
			return 0;
		}
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		DIR *gfaldir;
		struct dirent *d;

		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"getdir %s",gfalname);

		gfaldir = gfal_opendir(gfalname);
		if(gfaldir) {
			pfs_dir *dir = new pfs_dir(name);
			while(((d=gfal_readdir(gfaldir)))) {
				dir->append(d->d_name);
			}
			return dir;
		} else {
			return 0;
		}
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat gbuf;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"stat %s",gfalname);
		result = gfal_stat(gfalname,&gbuf);
		if(result==0) COPY_STAT(gbuf,*buf);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat gbuf;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"lstat %s",gfalname);
		result = gfal_lstat(gfalname,&gbuf);
		if(result==0) COPY_STAT(gbuf,*buf);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int unlink( pfs_name *name ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"unlink %s",gfalname);
		result = gfal_unlink(gfalname);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"access %s %d",gfalname,mode);
		result = gfal_access(gfalname,mode);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int chmod( pfs_name *name, mode_t mode ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"chmod %s %d",gfalname,mode);
		result = gfal_chmod(gfalname,mode);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		char newgfalname[PFS_PATH_MAX];
		format_name(name,gfalname);
		format_name(newname,newgfalname);

		debug(D_GFAL,"rename %s %s",gfalname,newgfalname);
		result = gfal_rename(gfalname,newgfalname);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int chdir( pfs_name *name, char *newname ) {

		struct pfs_stat buf;
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		result = this->stat(name,&buf);
		if(result<0) return result;

		if(S_ISDIR(buf.st_mode)) {
			strcpy(newname,name->path);
			return 0;
		} else {
			errno = ENOTDIR;
			return -1;
		}
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"mkdir %s %d",gfalname,mode);
		result = gfal_mkdir(gfalname,mode);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int rmdir( pfs_name *name ) {
		int result;
		char gfalname[PFS_PATH_MAX];
		format_name(name,gfalname);

		debug(D_GFAL,"rmdir %s",gfalname);
		result = gfal_rmdir(gfalname);
		debug(D_GFAL,"= %d",result);
		return result;
	}

	virtual int is_local() {
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}

};

static pfs_service_gfal pfs_service_gfal_instance;
pfs_service *pfs_service_gfal = &pfs_service_gfal_instance;

#endif

/* vim: set noexpandtab tabstop=8: */
