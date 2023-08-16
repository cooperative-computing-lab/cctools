/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_file.h"
#include "pfs_service.h"

#include <errno.h>
#include <stdio.h>
#include <sys/mman.h>
#include <string.h>

pfs_file::pfs_file( pfs_name *n )
{
	memcpy(&name,n,sizeof(name));
	last_offset = 0;
}

pfs_file::~pfs_file()
{
}

int pfs_file::close()
{
	return 0;
}

pfs_ssize_t pfs_file::read( void *data, pfs_size_t length, pfs_off_t offset )
{
	errno = EINVAL;
	return -1;
}

pfs_ssize_t pfs_file::write( const void *data, pfs_size_t length, pfs_off_t offset )
{
	errno = EROFS;
	return -1;
}

int pfs_file::fstat( struct pfs_stat *buf )
{
	pfs_service_emulate_stat(&name,buf);
	return 0;
}

int pfs_file::fstatfs( struct pfs_statfs *buf )
{
	pfs_service_emulate_statfs(buf);
	return 0;
}

int pfs_file::ftruncate( pfs_size_t length )
{
	errno = EROFS;
	return -1;
}

int pfs_file::fsync()
{
	return 0;
}

int pfs_file::fcntl( int cmd, void *arg )
{
	errno = EINVAL;
	return -1;
}

int pfs_file::fchmod( mode_t mode )
{
	return 0;
}

int pfs_file::fchown( uid_t uid, gid_t gid )
{
	errno = EROFS;
	return -1;
}

ssize_t pfs_file::fgetxattr( const char *name, void *data, size_t size )
{
	/* Despite what `man getxattr` says, linux doesn't have an ENOTSUP errno.
	* GNU defines ENOTSUP as EOPNOTSUPP. We should mirror Linux in this case.
	* */
	errno = EOPNOTSUPP;
	return -1;
}

ssize_t pfs_file::flistxattr( char *list, size_t size )
{
	/* Despite what `man getxattr` says, linux doesn't have an ENOTSUP errno.
	* GNU defines ENOTSUP as EOPNOTSUPP. We should mirror Linux in this case.
	* */
	errno = EOPNOTSUPP;
	return -1;
}

int pfs_file::fsetxattr( const char *name, const void *data, size_t size, int flags )
{
	/* Despite what `man getxattr` says, linux doesn't have an ENOTSUP errno.
	* GNU defines ENOTSUP as EOPNOTSUPP. We should mirror Linux in this case.
	* */
	errno = EOPNOTSUPP;
	return -1;
}

int pfs_file::fremovexattr( const char *name )
{
	/* Despite what `man getxattr` says, linux doesn't have an ENOTSUP errno.
	* GNU defines ENOTSUP as EOPNOTSUPP. We should mirror Linux in this case.
	* */
	errno = EOPNOTSUPP;
	return -1;
}

int pfs_file::flock( int op )
{
	errno = ENOSYS;
	return -1;
}

void * pfs_file::mmap( void *start, pfs_size_t length, int prot, int flags, pfs_off_t offset )
{
	errno = EINVAL;
	return MAP_FAILED;
}

struct dirent * pfs_file::fdreaddir( pfs_off_t offset, pfs_off_t *next_offset )
{
	errno = ENOTDIR;
	return 0;
}

pfs_name * pfs_file::get_name()
{
	return &name;
}

pfs_ssize_t pfs_file::get_size()
{
	return -1;
}

int pfs_file::get_real_fd()
{
	return -1;
}

int pfs_file::get_local_name( char *n )
{
	return -1;
}

int pfs_file::get_block_size()
{
	return name.service->get_block_size();
}

pfs_off_t pfs_file::get_last_offset()
{
	return last_offset;
}

void pfs_file::set_last_offset( pfs_off_t o )
{
	last_offset = o;
}

int pfs_file::is_seekable()
{
	return name.service->is_seekable();
}

/* vim: set noexpandtab tabstop=8: */
