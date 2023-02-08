/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_TABLE_H
#define PFS_TABLE_H

extern "C" {
#include "buffer.h"
}

#include "pfs_mmap.h"
#include "pfs_name.h"
#include "pfs_refcount.h"
#include "pfs_types.h"

class pfs_file;
class pfs_dir;
class pfs_pointer;
class pfs_service;

#define PFS_MAX_RESOLVE_DEPTH	8

class pfs_table : public pfs_refcount {
public:
	pfs_table();
	~pfs_table();
	pfs_table * fork();
	void close_on_exec();

	int isvalid( int fd );
	int isnative( int fd );
	int isparrot( int fd );
	int isspecial( int fd );
	void recvfd( pid_t pid, int fd );
	void sendfd( int fd, int errored );

	/* file descriptor creation */
	int open( const char *path, int flags, mode_t mode, int force_cache, char *native_path, size_t len );
	void attach( int logical, int physical, int flags, mode_t mode, const char *name, struct stat *buf );
	void setnative( int fd, int fdflags );
	void setspecial( int fd );
	void setparrot(int fd, int rfd, struct stat *buf);

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
	int		fchmod( int fd, mode_t mode );
	int		fchown( int fd, struct pfs_process *p, uid_t uid, gid_t gid );
	int		flock( int fd, int op );
	int		bind( int fd, char *lpath, size_t len );

	/* operations on the table itself */
	int	chdir( const char *path );
	char *	getcwd( char *path, pfs_size_t size );
	int	dup2( int old, int nfd, int flags );

	int	get_real_fd( int fd );
	int	get_full_name( int fd, char *name );
	int	get_local_name( int fd, char *name );

	/* operations on services */
	int	stat( const char *name, struct pfs_stat *buf );
	int	statx( const char *pathname, int flags, unsigned int mask, struct pfs_statx *buf );
	int	statfs( const char *path, struct pfs_statfs *buf );
	int	lstat( const char *name, struct pfs_stat *buf );
	int	access( const char *name, mode_t mode );
	int	chmod( const char *name, mode_t mode );
	int	chown( const char *name, struct pfs_process *p, uid_t uid, gid_t gid );
	int	lchown( const char *name, uid_t uid, gid_t gid );
	int	truncate( const char *path, pfs_off_t length );
	int	utime( const char *path, struct utimbuf *buf );
	int	utimens( const char *path, const struct timespec times[2] );
	int	lutimens( const char *path, const struct timespec times[2] );
	int	unlink( const char *name );
	int	rename( const char *old_name, const char *new_name );
	int	link( const char *oldpath, const char *newpath );
	int	symlink( const char *target, const char *path );
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
	pfs_ssize_t fcopyfile(int sourcefd, int targetfd);
	pfs_ssize_t copyfile_slow( pfs_file *sourcefile, pfs_file *targetfile );
	int	md5( const char *path, unsigned char *digest );
	int	md5_slow( const char *path, unsigned char *digest );
	int 	search( const char *paths, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i);

	void	follow_symlink( struct pfs_name *pname, mode_t amode, int depth = 0 );
	int	resolve_name( int is_special_syscall, const char *cname, pfs_name *pname, mode_t mode, bool do_follow_symlink = true, int depth = 0, const char *parent_dir = NULL );

	/* mmap operations */
	pfs_size_t  mmap_create( int fd, pfs_size_t file_offset, size_t length, int prot, int flags );
	int         mmap_update( uintptr_t logical_address, size_t channel_address );
	int         mmap_delete( uintptr_t logical_address, size_t length );
	void        mmap_print();
	static void mmap_proc(pid_t pid, buffer_t *B);

	pfs_file * open_object( const char *path, int flags, mode_t mode, int force_cache );
	pfs_dir * open_directory(pfs_name *pname, int flags);

	int find_empty( int lowest );
	int complete_at_path( int dirfd, const char *short_path, char *long_path );
private:
	int count_pointer_uses( pfs_pointer *p );
	int count_file_uses( pfs_file *f );
	static pfs_pointer *getopenfile( pid_t pid, int fd );

	void complete_path( const char *short_path, const char *parent_dir, char *long_path );

	pfs_size_t mmap_create_object( pfs_file *file, pfs_size_t channel_offset, pfs_size_t map_length, pfs_size_t file_offset, int prot, int flags );

	int         pointer_count;
	pfs_pointer **pointers;
	int         *fd_flags;
	char        working_dir[PFS_PATH_MAX];
	pfs_mmap    *mmap_list;

};

#endif

/* vim: set noexpandtab tabstop=4: */
