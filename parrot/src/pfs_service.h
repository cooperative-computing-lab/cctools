/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SERVICE_H
#define PFS_SERVICE_H

#include "pfs_name.h"
#include "pfs_file.h"
#include "pfs_dir.h"
#include "pfs_location.h"

class pfs_service {
public:
	pfs_service();
	virtual ~pfs_service();

	virtual void * connect( pfs_name *name );
	virtual void disconnect( pfs_name *name, void *cxn );
	virtual int get_default_port();
	virtual int get_block_size();
	virtual int tilde_is_special();
	virtual int is_seekable();
	virtual int is_local();

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode );
	virtual pfs_dir * getdir( pfs_name *name );

	virtual int stat( pfs_name *name, struct pfs_stat *buf );
	virtual int statfs( pfs_name *name, struct pfs_statfs *buf );
	virtual int lstat( pfs_name *name, struct pfs_stat *buf );
	virtual int unlink( pfs_name *name );
	virtual int access( pfs_name *name, mode_t mode );
	virtual int chmod( pfs_name *name, mode_t mode );
	virtual int chown( pfs_name *name, uid_t uid, gid_t gid );
	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid );
	virtual int truncate( pfs_name *name, pfs_off_t length );
	virtual int utime( pfs_name *name, struct utimbuf *buf );
	virtual int rename( pfs_name *oldname, pfs_name *newname );
	virtual int chdir( pfs_name *name, char *newpath );
	virtual int link( pfs_name *oldname, pfs_name *newname );
	virtual int symlink( const char *linkname, pfs_name *newname );
	virtual int readlink( pfs_name *name, char *buf, pfs_size_t bufsiz );
	virtual int mknod( pfs_name *name, mode_t mode, dev_t dev );
	virtual int mkdir( pfs_name *name, mode_t mode );
	virtual int rmdir( pfs_name *name );

	virtual ssize_t getxattr ( pfs_name *name, const char *attrname, void *value, size_t size );
	virtual ssize_t lgetxattr ( pfs_name *name, const char *attrname, void *value, size_t size );
	virtual ssize_t listxattr ( pfs_name *name, char *attrlist, size_t size );
	virtual ssize_t llistxattr ( pfs_name *name, char *attrlist, size_t size );
	virtual int setxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags );
	virtual int lsetxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags );
	virtual int removexattr ( pfs_name *name, const char *attrname );
	virtual int lremovexattr ( pfs_name *name, const char *attrname );

	virtual int mkalloc( pfs_name *name, pfs_ssize_t size, mode_t mode );
	virtual int lsalloc( pfs_name *name, char *alloc_name, pfs_ssize_t *size, pfs_ssize_t *inuse );
	virtual int whoami( pfs_name *name, char *buf, int size );
	virtual int search( const char *path, const char *pattern, char *buffer, size_t len1, struct stat *stats, size_t len2 );
	virtual int getacl( pfs_name *name, char *buf, int size );
	virtual int setacl( pfs_name *name, const char *subject, const char *rights );
	virtual pfs_location* locate( pfs_name *name );

	virtual pfs_ssize_t putfile( pfs_name *source, pfs_name *target );
	virtual pfs_ssize_t getfile( pfs_name *source, pfs_name *target );
	virtual pfs_ssize_t thirdput( pfs_name *source, pfs_name *target );
	virtual int md5( pfs_name *source, unsigned char *digest );
};


pfs_service * pfs_service_lookup( const char *name );
pfs_service * pfs_service_lookup_default();

void pfs_service_print();

void pfs_service_emulate_statfs( struct pfs_statfs *buf );
void pfs_service_emulate_stat( pfs_name *name, struct pfs_stat *buf );

void pfs_service_set_block_size( int bs );
int  pfs_service_get_block_size();

void * pfs_service_connect_cache( pfs_name *name );
void pfs_service_disconnect_cache( pfs_name *name, void *cxn, int invalidate );

#endif
