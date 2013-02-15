/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_TABLE_H
#define PFS_TABLE_H

#include "pfs_types.h"
#include "pfs_refcount.h"
#include "pfs_name.h"
#include "pfs_mmap.h"

class pfs_file;
class pfs_pointer;
class pfs_service;

#define PFS_MAX_RESOLVE_DEPTH	8

class pfs_table : public pfs_refcount {
public:
	pfs_table();
	~pfs_table();
	pfs_table * fork();
	void close_on_exec();

	/* file descriptor creation */
	int	open( const char *path, int flags, mode_t mode, int force_cache );
	int	pipe( int *fds );
	void	attach( int logical, int physical, int flags, mode_t mode, const char *name );

	/* operations on open files */
	int		close( int fd );
	pfs_ssize_t	read( int fd, void *data, pfs_size_t length );
	pfs_ssize_t	write( int fd, const void *data, pfs_size_t length );
	pfs_ssize_t	pread( int fd, void *data, pfs_size_t length, pfs_off_t offset );
	pfs_ssize_t	pwrite( int fd, const void *data, pfs_size_t length, pfs_off_t offset );
	pfs_ssize_t	readv( int fd, const struct iovec *vector, int count );
	pfs_ssize_t	writev( int fd, const struct iovec *vector, int count );
	pfs_off_t	lseek( int fd, pfs_off_t offset, int whence );

	int		ftruncate( int fd, pfs_off_t length );
	int		fstat( int fd, struct pfs_stat *buf );
	int		fstatfs( int fd, struct pfs_statfs *buf );
	int		fsync( int fd );
	int		fchdir( int fd );
	int		fcntl( int fd, int cmd, void *arg );
	int		ioctl( int fd, int cmd, void *arg );
	int		fchmod( int fd, mode_t mode );
	int		fchown( int fd, uid_t uid, gid_t gid );
	int		flock( int fd, int op );

	/* operations on the table itself */
	int     select( int n, fd_set *rfds, fd_set *wfds, fd_set *efds, struct timeval *timeout );
	int	poll( struct pollfd *ufds, unsigned int nfds, int timeout );  
	int	chdir( const char *path );
	char *	getcwd( char *path, pfs_size_t size );
	int	dup( int old );
	int	dup2( int old, int nfd );

	int	get_real_fd( int fd );
	int	get_full_name( int fd, char *name );
	int	get_local_name( int fd, char *name );

	/* operations on services */
	int	stat( const char *name, struct pfs_stat *buf );
	int	statfs( const char *path, struct pfs_statfs *buf );
	int	lstat( const char *name, struct pfs_stat *buf );
	int	access( const char *name, mode_t mode );
	int	chmod( const char *name, mode_t mode );
	int	chown( const char *name, uid_t uid, gid_t gid );
	int	lchown( const char *name, uid_t uid, gid_t gid );
	int	truncate( const char *path, pfs_off_t length );
	int	utime( const char *path, struct utimbuf *buf );
	int	unlink( const char *name );
	int	rename( const char *old_name, const char *new_name );
	int	link( const char *oldpath, const char *newpath );
	int	symlink( const char *oldpath, const char *newpath );
	int	readlink( const char *path, char *buf, pfs_size_t size );
	int	mknod( const char *path, mode_t mode, dev_t dev );
	int	mkdir( const char *path, mode_t mode );
	int	rmdir( const char *path );
	struct dirent * fdreaddir( int fd );

    /* extended attributes */
	ssize_t getxattr (const char *path, const char *name, void *value, size_t size);
	ssize_t lgetxattr (const char *path, const char *name, void *value, size_t size);
	ssize_t fgetxattr (int fd, const char *name, void *value, size_t size);
	ssize_t listxattr (const char *path, char *list, size_t size);
	ssize_t llistxattr (const char *path, char *list, size_t size);
	ssize_t flistxattr (int fd, char *list, size_t size);
	int setxattr (const char *path, const char *name, const void *value, size_t size, int flags);
	int lsetxattr (const char *path, const char *name, const void *value, size_t size, int flags);
	int fsetxattr (int fd, const char *name, const void *value, size_t size, int flags);
	int removexattr (const char *path, const char *name);
	int lremovexattr (const char *path, const char *name);
	int fremovexattr (int fd, const char *name);

	/* custom Parrot syscalls */
	int	mkalloc( const char *path, pfs_ssize_t size, mode_t mode );
	int	lsalloc( const char *path, char *alloc_path, pfs_ssize_t *total, pfs_ssize_t *inuse );
	int	whoami( const char *path, char *buf, int size );
	int	getacl( const char *path, char *buf, int size );
	int	setacl( const char *path, const char *subject, const char *rights );
	int	locate( const char *path, char *buf, int size );
	pfs_ssize_t copyfile( const char *source, const char *target );
	pfs_ssize_t copyfile_slow( const char *source, const char *target );
	int	md5( const char *path, unsigned char *digest );
	int	md5_slow( const char *path, unsigned char *digest );
	int 	search( const char *paths, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i);
	
	/* network operations */
	int	socket( int domain, int type, int protocol );
	int	socketpair( int domain, int type, int protocol, int *fds );
	int	accept( int fd, struct sockaddr *addr, int * addrlen );

	void	follow_symlink( struct pfs_name *pname, int depth = 0 );
	int	resolve_name( const char *cname, pfs_name *pname, bool do_follow_symlink = true, int depth = 0 );

	/* mmap operations */
	pfs_size_t mmap_create_object( pfs_file *file, pfs_size_t file_offset, pfs_size_t length, int prot, int flags );
	pfs_size_t mmap_create( int fd, pfs_size_t file_offset, pfs_size_t length, int prot, int flags );
	int	   mmap_update( pfs_size_t logical_address, pfs_size_t channel_address );
	int	   mmap_delete( pfs_size_t logical_address, pfs_size_t length );
	void       mmap_print();

	pfs_file * open_object( const char *path, int flags, mode_t mode, int force_cache );

	int find_empty( int lowest );
	void complete_at_path( int dirfd, const char *short_path, char *long_path );
private:
	int search_dup2( int ofd, int search );

	int count_pointer_uses( pfs_pointer *p );
	int count_file_uses( pfs_file *f );

	void collapse_path( const char *short_path, char *long_path, int remove_dotdot );
	void complete_path( const char *short_path, char *long_path );

	int         pointer_count;
	pfs_pointer **pointers;
	int         *fd_flags;
	char        working_dir[PFS_PATH_MAX];
	pfs_mmap    *mmap_list;

};

#endif



