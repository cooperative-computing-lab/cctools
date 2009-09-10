
/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "pfs_service.h"
#include "pfs_dir.h"
#include "pfs_process.h"

extern "C" {
#include "chirp_reli.h"
#include "hash_table.h"
}

#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

pfs_service::pfs_service()
{
}

pfs_service::~pfs_service()
{
}

void * pfs_service::connect( pfs_name *name )
{
	errno = ENOSYS;
	return 0;
}

void pfs_service::disconnect( pfs_name *name, void *cxn )
{
}

int pfs_service::get_default_port()
{
	return 0;
}

int pfs_service::tilde_is_special()
{
	return 0;
}

int pfs_service::is_seekable()
{
	return 0;
}

int pfs_service::is_local()
{
	return 0;
}

pfs_file * pfs_service::open( pfs_name *name, int flags, mode_t mode )
{
	errno = ENOENT;
	return 0;
}

pfs_dir * pfs_service::getdir( pfs_name *name )
{
	errno = ENOTDIR;
	return 0;
}

int pfs_service::stat( pfs_name *name, struct pfs_stat *buf )
{
	pfs_service_emulate_stat(name,buf);
	return 0;
}

int pfs_service::statfs( pfs_name *name, struct pfs_statfs *buf )
{
	pfs_service_emulate_statfs(buf);
	return 0;
}

int pfs_service::lstat( pfs_name *name, struct pfs_stat *buf )
{
	pfs_service_emulate_stat(name,buf);
	return 0;
}

int pfs_service::access( pfs_name *name, mode_t mode ) 
{
	if( mode&X_OK ) {
		errno = EACCES;
		return -1;
	} else if( mode&W_OK ) {
		errno = EACCES;
		return -1;
	} else {
		return 0;
	}
}

/*
Strictly speaking, this should fail, but users get confused
about error messages from tools such as cp innocently
trying to set the right mode.  Same comments apply to
utime and such.
*/

int pfs_service::chmod( pfs_name *name, mode_t mode ) 
{
	return 0;
}

int pfs_service::chown( pfs_name *name, uid_t uid, gid_t gid )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::lchown( pfs_name *name, uid_t uid, gid_t gid )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::truncate( pfs_name *name, pfs_off_t length )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::utime( pfs_name *name, struct utimbuf *buf )
{
	return 0;
}

int pfs_service::unlink( pfs_name *name )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::rename( pfs_name *old_name, pfs_name *new_name )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::chdir( pfs_name *name, char *newpath )
{
	strcpy(newpath,name->path);
	return 0;
}

int pfs_service::link( pfs_name *old_name, pfs_name *new_name )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::symlink( const char *linkname, pfs_name *new_name )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::readlink( pfs_name *name, char *buf, pfs_size_t size )
{
	errno = EINVAL;
	return -1;
}

int pfs_service::mknod( pfs_name *name, mode_t mode, dev_t dev )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::mkdir( pfs_name *name, mode_t mode )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::rmdir( pfs_name *name )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::mkalloc( pfs_name *name, pfs_ssize_t size, mode_t mode )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::lsalloc( pfs_name *name, char *alloc_name, pfs_ssize_t *size, pfs_ssize_t *inuse )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::whoami( pfs_name *name, char *buf, int size )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::getacl( pfs_name *name, char *buf, int size )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::setacl( pfs_name *name, const char *subject, const char *rights )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::putfile( pfs_name *source, pfs_name *target )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::getfile( pfs_name *source, pfs_name *target )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::thirdput( pfs_name *source, pfs_name *target )
{
	errno = ENOSYS;
	return -1;
}

int pfs_service::md5( pfs_name *source, unsigned char *digest )
{
	errno = ENOSYS;
	return -1;
}

pfs_service * pfs_service_lookup( const char *name )
{
	if(!strcmp(name,"chirp")) {
		extern pfs_service *pfs_service_chirp;
		return pfs_service_chirp;
	} else if(!strcmp(name,"multi")) {
		extern pfs_service *pfs_service_multi;
		return pfs_service_multi;
	} else if(!strcmp(name,"anonftp")) {
		extern pfs_service *pfs_service_anonftp;
		return pfs_service_anonftp;
	} else if(!strcmp(name,"ftp")) {
		extern pfs_service *pfs_service_ftp;
		return pfs_service_ftp;
	} else if(!strcmp(name,"http")) {
		extern pfs_service *pfs_service_http;
		return pfs_service_http;
	} else if(!strcmp(name,"grow")) {
		extern pfs_service *pfs_service_grow;
		return pfs_service_grow;
#ifdef HAS_GLOBUS_GSS
	} else if(!strcmp(name,"gsiftp") || !strcmp(name,"gridftp") ) {
		extern pfs_service *pfs_service_gsiftp;
		return pfs_service_gsiftp;
#endif
#ifdef HAS_NEST
	} else if(!strcmp(name,"nest")) {
		extern pfs_service *pfs_service_nest;
		return pfs_service_nest;
#endif
#ifdef HAS_EGEE
	} else if(!strcmp(name,"gfal") || !strcmp(name,"lfn") || !strcmp(name,"guid") || !strcmp(name,"srm") || !strcmp(name,"rfio") ) {
		extern pfs_service *pfs_service_gfal;
		return pfs_service_gfal;
	} else if(!strcmp(name,"lfc")) {
		extern pfs_service *pfs_service_lfc;
		return pfs_service_lfc;
#endif
#ifdef HAS_RFIO
        } else if(!strcmp(name,"rfio")) {
                extern pfs_service *pfs_service_rfio;
		return pfs_service_rfio;
#endif
#ifdef HAS_DCAP
        } else if(!strcmp(name,"dcap")) {
                extern pfs_service *pfs_service_dcap;
		return pfs_service_dcap;
#endif
#ifdef HAS_IRODS
        } else if(!strcmp(name,"irods")) {
                extern pfs_service *pfs_service_irods;
		return pfs_service_irods;
#endif
#ifdef HAS_HDFS
        } else if(!strcmp(name,"hdfs")) {
                extern pfs_service *pfs_service_hdfs;
		return pfs_service_hdfs;
#endif
#ifdef HAS_BXGRID
        } else if(!strcmp(name,"bxgrid")) {
                extern pfs_service *pfs_service_bxgrid;
		return pfs_service_bxgrid;
#endif
	} else {
		return 0;
	}

}

pfs_service * pfs_service_lookup_default()
{
	extern pfs_service *pfs_service_local;
	return pfs_service_local;
}

void pfs_service_emulate_statfs( struct pfs_statfs *buf )
{
	memset(buf,0,sizeof(*buf));
}

static int default_block_size = 65336;

void pfs_service_set_block_size( int bs )
{
	default_block_size = bs;
	chirp_reli_blocksize_set(bs);
}

int  pfs_service_get_block_size()
{
	if(!strcmp(pfs_current->name,"ld")) {
		return 4096;
	} else {
		return default_block_size;
	}
}

void pfs_service_emulate_stat( pfs_name *name, struct pfs_stat *buf )
{
	static time_t start_time = 0;
	memset(buf,0,sizeof(*buf));
	buf->st_dev = (dev_t) -1;
	if(name) {
		buf->st_ino = hash_string(name->rest);
	} else {
		buf->st_ino = 0;
	}
	buf->st_mode = S_IFREG | S_IRWXU | S_IRWXG | S_IRWXO ;
	buf->st_uid = getuid();
	buf->st_gid = getgid();
	buf->st_nlink = 1;
	buf->st_size = 0;
	if(start_time==0) start_time = time(0);
	buf->st_ctime = buf->st_atime = buf->st_mtime = start_time;
	buf->st_blksize = default_block_size;
}

static struct hash_table *table = 0;

void * pfs_service_connect_cache( pfs_name *name )
{
	char key[PFS_PATH_MAX];
	void *cxn;

	if(!name->host[0]) {
		errno = ENOENT;
		return 0;
	}

	if(!table) table = hash_table_create(0,0);

	if(table) {
		sprintf(key,"/%s/%s:%d",name->service_name,name->host,name->port);
		cxn = hash_table_remove(table,key);
		if(cxn) return cxn;
	}

	return name->service->connect(name);
}

void pfs_service_disconnect_cache( pfs_name *name, void *cxn, int invalidate )
{
	char key[PFS_PATH_MAX];
	int save_errno = errno;

	if(!table) table = hash_table_create(0,0);

	if(table && !invalidate) {
		sprintf(key,"/%s/%s:%d",name->service_name,name->host,name->port);
		if(hash_table_lookup(table,key)) {
			name->service->disconnect(name,cxn);
		} else {
			hash_table_insert(table,key,cxn);
		}
	} else {
		name->service->disconnect(name,cxn);
	}

	errno = save_errno;
}

