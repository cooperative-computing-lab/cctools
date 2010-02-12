/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SYS_H
#define PFS_SYS_H

#include "pfs_types.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/file.h>
#include <sys/poll.h>
#include <utime.h>

#ifdef __cplusplus
extern "C" {
#endif

struct pfs_name;

int		pfs_open( const char *path, int flags, mode_t mode );
int		pfs_pipe( int *fds );

int		pfs_close( int fd );
pfs_ssize_t	pfs_read( int fd, void *data, pfs_size_t length );
pfs_ssize_t	pfs_write( int fd, const void *data, pfs_size_t length );
pfs_ssize_t	pfs_pread( int fd, void *data, pfs_size_t length, pfs_off_t offset );
pfs_ssize_t	pfs_pwrite( int fd, const void *data, pfs_size_t length, pfs_off_t offset );
pfs_ssize_t	pfs_readv( int fd, const struct iovec *vector, int count );
pfs_ssize_t	pfs_writev( int fd, const struct iovec *vector, int count );
pfs_off_t	pfs_lseek( int fd, pfs_off_t offset, int whence );

int		pfs_ftruncate( int fd, pfs_off_t length );
int		pfs_fstat( int fd, struct pfs_stat *buf );
int		pfs_fstatfs( int fd, struct pfs_statfs *buf );
int		pfs_fsync( int fd );
int		pfs_fchdir( int fd );
int		pfs_fcntl( int fd, int cmd, void *arg );
int		pfs_ioctl( int fd, int cmd, void *arg );
int		pfs_fchmod( int fd, mode_t mode );
int		pfs_fchown( int fd, uid_t uid, gid_t gid );
int		pfs_flock( int fd, int op );

int		pfs_select( int n, fd_set *rfds, fd_set *wfds, fd_set *efds, struct timeval *timeout );
int		pfs_poll( struct pollfd *ufds, unsigned nfds, int timeout );
int		pfs_chdir( const char *path );
char *		pfs_getcwd( char *path, pfs_size_t size );
int		pfs_dup( int old );
int		pfs_dup2( int old, int nfd );

int		pfs_stat( const char *name, struct pfs_stat *buf );
int		pfs_statfs( const char *path, struct pfs_statfs *buf );
int		pfs_lstat( const char *name, struct pfs_stat *buf );
int		pfs_access( const char *name, mode_t mode );
int		pfs_chmod( const char *name, mode_t mode );
int		pfs_chown( const char *name, uid_t uid, gid_t gid );
int		pfs_lchown( const char *name, uid_t uid, gid_t gid );
int		pfs_truncate( const char *path, pfs_off_t length );
int		pfs_utime( const char *path, struct utimbuf *buf );
int		pfs_unlink( const char *name );
int		pfs_rename( const char *old_name, const char *new_name );
int		pfs_link( const char *oldpath, const char *newpath );
int		pfs_symlink( const char *oldpath, const char *newpath );
int		pfs_readlink( const char *path, char *buf, pfs_size_t size );
int		pfs_mknod( const char *path, mode_t mode, dev_t dev );
int		pfs_mkdir( const char *path, mode_t mode );
int		pfs_rmdir( const char *path );
struct dirent *	pfs_fdreaddir( int fd );

int		pfs_socket( int domain, int type, int protocol );
int		pfs_socketpair( int domain, int type, int protocol, int *fds );
int		pfs_accept( int fd, struct sockaddr *addr, int * addrlen );
int		pfs_bind( int fd, const struct sockaddr *addr, int addrlen );
int		pfs_connect( int fd, const struct sockaddr *addr, int addrlen );
int		pfs_getpeername( int fd, struct sockaddr *addr, int * addrlen );
int		pfs_getsockname( int fd, struct sockaddr *addr, int * addrlen );
int		pfs_getsockopt( int fd, int level, int option, void *value, int * length );
int		pfs_listen( int fd, int backlog );
int		pfs_recv( int fd, void *data, int length, int flags );
int		pfs_recvfrom( int fd, void *data, int length, int flags, struct sockaddr *addr, int * addrlength);
int		pfs_recvmsg( int fd,  struct msghdr *msg, int flags );
int		pfs_send( int fd, const void *data, int length, int flags );
int		pfs_sendmsg( int fd, const struct msghdr *msg, int flags );
int		pfs_sendto( int fd, const void *data, int length, int flags, const struct sockaddr *addr, int addrlength );
int		pfs_setsockopt( int fd, int level, int option, const void *value, int length );
int		pfs_shutdown( int fd, int how );

int		pfs_mkalloc( const char *path, pfs_ssize_t size, mode_t mode );
int		pfs_lsalloc( const char *path, char *alloc_path, pfs_ssize_t *avail, pfs_ssize_t *inuse );

int		pfs_whoami( const char *path, char *buf, int size );
int		pfs_getacl( const char *path, char *buf, int size );
int		pfs_setacl( const char *path, const char *subject, const char *rights );
int		pfs_locate( const char *path, char* buf, int size );
int		pfs_copyfile( const char *source, const char *target );
int		pfs_md5( const char *path, unsigned char *digest );
int		pfs_timeout( const char *str );

int		pfs_get_real_fd( int fd );
int		pfs_get_full_name( int fd, char *name );
int		pfs_get_local_name( const char *rpath, char *lpath, char *firstline, int length );
int		pfs_is_nonblocking( int fd );
int		pfs_resolve_name( const char *path, struct pfs_name *pname );
 
#ifdef __cplusplus
}
#endif

#endif
