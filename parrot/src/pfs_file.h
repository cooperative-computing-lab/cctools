/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_FILE_H
#define PFS_FILE_H

#include "pfs_name.h"
#include "pfs_poll.h"
#include "pfs_types.h"
#include "pfs_refcount.h"

class pfs_service;

class pfs_file : public pfs_refcount {
public:
	pfs_file( pfs_name *n );
	virtual ~pfs_file();

	virtual int close();
	virtual	pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset );
	virtual	pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset );
	virtual int fstat( struct pfs_stat *buf );
	virtual int fstatfs( struct pfs_statfs *buf );
	virtual	int ftruncate( pfs_size_t length );
	virtual	int fsync();
	virtual int fcntl( int cmd, void *arg );
	virtual int ioctl( int cmd, void *arg );
	virtual int fchmod( mode_t mode );
	virtual int fchown( uid_t uid, gid_t gid );
	virtual int flock( int op );
	virtual void * mmap( void *start, pfs_size_t length, int prot, int flags, pfs_off_t offset );
	virtual struct dirent * fdreaddir( pfs_off_t offset, pfs_off_t *next_offset );

	virtual ssize_t fgetxattr( const char *name, void *data, size_t size );
	virtual ssize_t flistxattr( char *list, size_t size );
	virtual int fsetxattr( const char *name, const void *data, size_t size, int flags );
	virtual int fremovexattr( const char *name );

	virtual pfs_ssize_t get_size();
	virtual pfs_name *get_name();
	virtual int get_real_fd();
	virtual int get_local_name( char *n );
	virtual int get_block_size();
	virtual int is_seekable();
	virtual pfs_off_t get_last_offset();
	virtual void set_last_offset( pfs_off_t offset );

	virtual void poll_register( int which );
	virtual int poll_ready();

protected:
	pfs_name name;
	pfs_off_t last_offset;
};

struct pfs_file * pfs_file_bootstrap( int fd, const char *name );

#endif
