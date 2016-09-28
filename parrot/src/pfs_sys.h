/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_SYS_H
#define PFS_SYS_H

#include "pfs_types.h"
#include "pfs_search.h"
extern "C" {
#include "pfs_resolve.h"
}

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/file.h>
#include <utime.h>

#ifdef __cplusplus
extern "C" {
#endif

struct pfs_name;

int		pfs_open( struct pfs_mount_entry *ns, const char *path, int flags, mode_t mode, char *native_path, size_t len );

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
int		pfs_fchmod( int fd, mode_t mode );
int		pfs_fchown( int fd, struct pfs_process *p, uid_t uid, gid_t gid );
int		pfs_flock( int fd, int op );

int		pfs_chdir( struct pfs_mount_entry *ns, const char *path );
char *		pfs_getcwd( char *path, pfs_size_t size );

int		pfs_stat( struct pfs_mount_entry *ns, const char *name, struct pfs_stat *buf );
int		pfs_statfs( struct pfs_mount_entry *ns, const char *path, struct pfs_statfs *buf );
int		pfs_lstat( struct pfs_mount_entry *ns, const char *name, struct pfs_stat *buf );
int		pfs_access( struct pfs_mount_entry *ns, const char *name, mode_t mode );
int		pfs_chmod( struct pfs_mount_entry *ns, const char *name, mode_t mode );
int		pfs_chown( struct pfs_mount_entry *ns, const char *name, struct pfs_process *p, uid_t uid, gid_t gid );
int		pfs_lchown( struct pfs_mount_entry *ns, const char *name, uid_t uid, gid_t gid );
int		pfs_truncate( struct pfs_mount_entry *ns, const char *path, pfs_off_t length );
int		pfs_utime( struct pfs_mount_entry *ns, const char *path, struct utimbuf *buf );
int		pfs_utimensat( struct pfs_mount_entry *ns, int dirfd, const char *pathname, const struct timespec times[2], int flags );
int		pfs_unlink( struct pfs_mount_entry *ns, const char *name );
int		pfs_rename( struct pfs_mount_entry *ns, const char *old_name, const char *new_name );
int		pfs_link( struct pfs_mount_entry *ns, const char *oldpath, const char *newpath );
int		pfs_symlink( struct pfs_mount_entry *ns, const char *target, const char *path );
int		pfs_readlink( struct pfs_mount_entry *ns, const char *path, char *buf, pfs_size_t size );
int		pfs_mknod( struct pfs_mount_entry *ns, const char *path, mode_t mode, dev_t dev );
int		pfs_mkdir( struct pfs_mount_entry *ns, const char *path, mode_t mode );
int		pfs_rmdir( struct pfs_mount_entry *ns, const char *path );
struct dirent *	pfs_fdreaddir( int fd );

int		pfs_mount( struct pfs_mount_entry **ns, const char *path, const char *device, const char *mode );
int		pfs_unmount( struct pfs_mount_entry **ns, const char *path );

int		pfs_openat( struct pfs_mount_entry *ns, int dirfd, const char *path, int flags, mode_t mode, char *native_path, size_t len );
int		pfs_mkdirat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode);
int		pfs_mknodat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode, dev_t dev );
int		pfs_fchownat( struct pfs_mount_entry *ns, int dirfd, const char *path, struct pfs_process *p, uid_t owner, gid_t group, int flags );
int		pfs_futimesat( struct pfs_mount_entry *ns, int dirfd, const char *path, const struct timeval times[2] );
int		pfs_fstatat( struct pfs_mount_entry *ns, int dirfd, const char *path, struct pfs_stat *buf, int flags );
int		pfs_unlinkat( struct pfs_mount_entry *ns, int dirfd, const char *path, int flags );
int		pfs_renameat( struct pfs_mount_entry *ns, int olddirfd, const char *oldpath, int newdirfd, const char *newpath );
int		pfs_linkat( struct pfs_mount_entry *ns, int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags );
int		pfs_symlinkat( struct pfs_mount_entry *ns, const char *oldpath, int newdirfd, const char *newpath );
int		pfs_readlinkat( struct pfs_mount_entry *ns, int dirfd, const char *path, char *buf, size_t bufsiz );
int		pfs_fchmodat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode, int flags );
int		pfs_faccessat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode );


ssize_t pfs_getxattr (struct pfs_mount_entry *ns, const char *path, const char *name, void *value, size_t size);
ssize_t pfs_lgetxattr (struct pfs_mount_entry *ns, const char *path, const char *name, void *value, size_t size);
ssize_t pfs_fgetxattr (int fd, const char *name, void *value, size_t size);
ssize_t pfs_listxattr (struct pfs_mount_entry *ns, const char *path, char *list, size_t size);
ssize_t pfs_llistxattr (struct pfs_mount_entry *ns, const char *path, char *list, size_t size);
ssize_t pfs_flistxattr (int fd, char *list, size_t size);
int pfs_setxattr (struct pfs_mount_entry *ns, const char *path, const char *name, const void *value, size_t size, int flags);
int pfs_lsetxattr (struct pfs_mount_entry *ns, const char *path, const char *name, const void *value, size_t size, int flags);
int pfs_fsetxattr (int fd, const char *name, const void *value, size_t size, int flags);
int pfs_removexattr (struct pfs_mount_entry *ns, const char *path, const char *name);
int pfs_lremovexattr (struct pfs_mount_entry *ns,const char *path, const char *name);
int pfs_fremovexattr (int fd, const char *name);

int		pfs_mkalloc( struct pfs_mount_entry *ns, const char *path, pfs_ssize_t size, mode_t mode );
int		pfs_lsalloc( struct pfs_mount_entry *ns, const char *path, char *alloc_path, pfs_ssize_t *avail, pfs_ssize_t *inuse );

int		pfs_whoami( struct pfs_mount_entry *ns, const char *path, char *buf, int size );
int		pfs_getacl( struct pfs_mount_entry *ns, const char *path, char *buf, int size );
int		pfs_setacl( struct pfs_mount_entry *ns, const char *path, const char *subject, const char *rights );
int		pfs_locate( struct pfs_mount_entry *ns, const char *path, char* buf, int size );
int		pfs_copyfile( struct pfs_mount_entry *ns, const char *source, const char *target );
int		pfs_fcopyfile( int srcfd, int dstfd );
int		pfs_md5(struct pfs_mount_entry *ns,  const char *path, unsigned char *digest );
int		pfs_timeout( const char *str );

int		pfs_get_real_fd( int fd );
int		pfs_get_full_name( int fd, char *name );
int		pfs_get_local_name( struct pfs_mount_entry *ns, const char *rpath, char *lpath, char *firstline, size_t length );

int		pfs_search( struct pfs_mount_entry *ns, const char *path, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i);

int	pfs_mmap_delete( uintptr_t logical_address, size_t length );
int	pfs_mmap_update( uintptr_t logical_address, pfs_size_t channel_address );
pfs_size_t pfs_mmap_create( int fd, pfs_size_t file_offset, size_t length, int prot, int flags );

#ifdef __cplusplus
}
#endif

#endif
