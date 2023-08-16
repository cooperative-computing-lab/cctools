/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
#include "domain_name.h"
#include "link.h"
#include "file_cache.h"
#include "full_io.h"
#include "http_query.h"
}

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statfs.h>

#define HTTP_LINE_MAX 4096
#define HTTP_PORT 80
#define HTTP_FILE_MODE (S_IFREG | 0555)

extern int pfs_main_timeout;

static struct link * http_fetch( pfs_name *name, const char *action, INT64_T *size )
{
	char url[HTTP_LINE_MAX];

	if(!name->host[0]) {
		errno = ENOENT;
		return 0;
	}

	sprintf(url,"http://%s:%d%s",name->host,name->port,name->rest);
	return http_query_size(url,action,size,time(0)+pfs_main_timeout,0);
}

class pfs_file_http : public pfs_file
{
private:
	struct link *link;
	INT64_T size;

public:
	pfs_file_http( pfs_name *n, struct link *l, INT64_T s ) : pfs_file(n) {
		link = l;
		size = s;
	}

	virtual int close() {
		link_close(link);
		return 0;
	}

	virtual pfs_ssize_t read( void *d, pfs_size_t length, pfs_off_t offset ) {
		return link_read(link,(char*)d,length,LINK_FOREVER);
	}

	virtual int fstat( struct pfs_stat *buf ) {
		pfs_service_emulate_stat(&name,buf);
		buf->st_mode = HTTP_FILE_MODE;
		buf->st_size = size;
		return 0;
	}

	virtual pfs_ssize_t get_size() {
		return size;
	}

};

class pfs_service_http : public pfs_service {
public:
	virtual int get_default_port() {
		return HTTP_PORT;
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		struct link *link;
		INT64_T size;

		if((flags&O_ACCMODE)!=O_RDONLY) {
			errno = EROFS;
			return 0;
		}

		link = http_fetch(name,"GET",&size);
		if(link) {
			return new pfs_file_http(name,link,size);
		} else {
			return 0;
		}
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		struct link *link;
		INT64_T size;

		link = http_fetch(name,"HEAD",&size);
		if(link) {
			link_close(link);
			pfs_service_emulate_stat(name,buf);
			buf->st_mode = HTTP_FILE_MODE;
			buf->st_size = size;
			return 0;
		} else {
			return -1;
		}
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		return this->stat(name,buf);
	}

	virtual int is_seekable (void) {
		return 0;
	}
};

static pfs_service_http pfs_service_http_instance;
pfs_service *pfs_service_http = &pfs_service_http_instance;

/* vim: set noexpandtab tabstop=8: */
