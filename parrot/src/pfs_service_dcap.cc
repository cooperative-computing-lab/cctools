/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

/*
Note that this driver is deprecated in favor of
pfs_service_gfal, which implements rfio and several
other protocols using the egee software stack.
*/

#ifdef HAS_DCAP

#include "pfs_service.h"
#include "pfs_file.h"

/*
Ugly little note here:
Both dcap.h and debug.h define a symbol "debug".
However, debug.h uses some macros to define debug as cctools_debug,
so the references below pick up the cctools version, not the dcap version.
*/

#include "dcap.h"

extern "C" {
#include "debug.h"
}

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/ioctl.h>

class pfs_file_dcap : public pfs_file
{
private:
	int fd;
	pfs_off_t remote_offset;

public:
	pfs_file_dcap( pfs_name *n, int f ) : pfs_file(n) {
		fd = f;
		remote_offset = 0;
	}
	virtual int close() {
		int result;
		debug(D_DCAP,"close %d",fd);
		result = dc_close(fd);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;
		return result;
	}
	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		int result;
		debug(D_DCAP,"pread %d %d %d",fd,length,offset);
		result = dc_pread(fd,data,length,offset);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;
		return result;
	}
	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		int result;
		debug(D_DCAP,"pwrite %d %d %d",fd,length,offset);
		result = dc_pwrite(fd,data,length,offset);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;
		return result;
	}

	virtual pfs_ssize_t get_size() {
		pfs_ssize_t result;
		result = ::dc_lseek(fd,0,SEEK_END);
		if(result>=0) {
			remote_offset = result;
			return result;
		} else {
			return 0;
		}
	}
};

class pfs_service_dcap : public pfs_service {
public:
	virtual int get_default_port() {
		return 22125;
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		int result;
		char url[PFS_PATH_MAX];
		sprintf(url,"dcap://%s:%d/%s",name->host,name->port,name->rest);
		debug(D_DCAP,"open %s %d %d",url,flags,mode);
		result = dc_open(url,flags,mode);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;

		if(result>=0) {
			dc_unsafeWrite(result);
			dc_noBuffering(result);
			return new pfs_file_dcap(name,result);
		} else {
			return 0;
		}
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf )
	{
		int result;
		char url[PFS_PATH_MAX];
		struct stat lbuf;
		sprintf(url,"dcap://%s:%d/%s",name->host,name->port,name->rest);
		debug(D_DCAP,"stat %s",url);
		result = dc_stat(url,&lbuf);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;
		if(result>=0) COPY_STAT(lbuf,*buf);
		return result;
	}

	virtual int lstat( pfs_name *name, struct stat64 *buf )
	{
		int result;
		char url[PFS_PATH_MAX];
		struct stat lbuf;
		sprintf(url,"dcap://%s:%d/%s",name->host,name->port,name->rest);
		debug(D_DCAP,"lstat %s",url);
		result = dc_lstat(url,&lbuf);
		debug(D_DCAP,"= %d %s",result,result>=0?"":strerror(errno));
		if(result<0 && errno==EINTR) errno = ETIMEDOUT;
		if(result>=0) COPY_STAT(lbuf,*buf);
		return result;
	}


	virtual int is_seekable() {
		return 1;
	}
};

static pfs_service_dcap pfs_service_dcap_instance;
pfs_service *pfs_service_dcap = &pfs_service_dcap_instance;

#endif
