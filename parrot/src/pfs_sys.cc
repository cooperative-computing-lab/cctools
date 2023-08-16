/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_sys.h"
#include "pfs_types.h"
#include "pfs_sysdeps.h"
#include "pfs_table.h"
#include "pfs_process.h"
#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "full_io.h"
#include "file_cache.h"
#include "pfs_mountfile.h"
#include "pfs_resolve.h"
#include "stringtools.h"
}

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/un.h>
#include <time.h>

extern struct file_cache *pfs_file_cache;
extern int pfs_force_cache;

/*
Notice that we make the check for EINTR in here,
rather than in pfs_dispatch.  In here, an EINTR
is clearly ourselves getting interrupted while
we do some work, while in pfs_dispatch, it might
indicate that we are to return control to a
recently-signalled child process.
*/

#define BEGIN \
	pfs_ssize_t result;\
	retry:

#define END \
	if (result >= 0)\
		debug(D_LIBCALL, "= %d [%s]",(int)result,__func__);\
	else\
		debug(D_LIBCALL, "= %d %s [%s]",(int)result,strerror(errno),__func__);\
	if(result<0 && errno==EINTR)\
		goto retry;\
	if(result<0 && errno==0) {\
		debug(D_DEBUG,"whoops, converting errno=0 to ENOENT");\
		errno = ENOENT;\
	}\
	return result;

int pfs_open( const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(path,flags,mode,pfs_force_cache,native_path,len);
	END
}

int pfs_open_cached( const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(path,flags,mode,1,native_path,len);
	END
}

int pfs_close( int fd )
{
	BEGIN
	debug(D_LIBCALL,"close %d",fd);
	result = pfs_current->table->close(fd);
	END
}

pfs_ssize_t pfs_read( int fd, void *data, pfs_size_t length )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"read %d %p %lld",fd,data,(long long) length);
	result = pfs_current->table->read(fd,data,length);
	END
}

pfs_ssize_t pfs_write( int fd, const void *data, pfs_size_t length )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"write %d %p %lld",fd,data,(long long) length);
	result = pfs_current->table->write(fd,data,length);
	END
}

pfs_ssize_t pfs_pread( int fd, void *data, pfs_size_t length, pfs_off_t offset )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"pread %d %p %lld",fd,data,(long long)length);
	result = pfs_current->table->pread(fd,data,length,offset);
	END
}

pfs_ssize_t pfs_pwrite( int fd, const void *data, pfs_size_t length, pfs_off_t offset )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"pwrite %d %p %lld",fd,data,(long long)length);
	result = pfs_current->table->pwrite(fd,data,length,offset);
	END
}

pfs_ssize_t pfs_readv( int fd, const struct iovec *vector, int count )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"readv %d %p %d",fd,vector,count);
	result = pfs_current->table->readv(fd,vector,count);
	END
}

pfs_ssize_t pfs_writev( int fd, const struct iovec *vector, int count )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"writev %d %p %d",fd,vector,count);
	result = pfs_current->table->writev(fd,vector,count);
	END
}

pfs_off_t pfs_lseek( int fd, pfs_off_t offset, int whence )
{
	pfs_off_t result;
	retry:
	debug(D_LIBCALL,"lseek %d %lld %d",fd,(long long)offset,whence);
	result = pfs_current->table->lseek(fd,offset,whence);
	END
}

int pfs_ftruncate( int fd, pfs_off_t length )
{
	BEGIN
	  debug(D_LIBCALL,"ftruncate %d %lld",fd,(long long)length);
	result = pfs_current->table->ftruncate(fd,length);
	END
}

int pfs_fstat( int fd, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"fstat %d %p",fd,buf);
	result = pfs_current->table->fstat(fd,buf);
	END
}

int pfs_fstatfs( int fd, struct pfs_statfs *buf )
{
	BEGIN
	debug(D_LIBCALL,"fstatfs %d %p",fd,buf);
	result = pfs_current->table->fstatfs(fd,buf);
	END
}

int pfs_fsync( int fd )
{
	BEGIN
	debug(D_LIBCALL,"fsync %d",fd);
	result = pfs_current->table->fsync(fd);
	END
}

int pfs_fchdir( int fd )
{
	BEGIN
	debug(D_LIBCALL,"fchdir %d",fd);
	result = pfs_current->table->fchdir(fd);
	END
}

int pfs_fcntl( int fd, int cmd, void *arg )
{
	BEGIN
	debug(D_LIBCALL,"fcntl %d %d %p",fd,cmd,arg);
	result = pfs_current->table->fcntl(fd,cmd,arg);
	END
}

int pfs_fchmod( int fd, mode_t mode )
{
	BEGIN
	result = pfs_current->table->fchmod(fd,mode);
	debug(D_LIBCALL,"fchmod %d %d",fd,mode);
	END
}

int pfs_fchown( int fd, struct pfs_process *p, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"fchown %d %d %d",fd,uid,gid);
	result = pfs_current->table->fchown(fd,p,uid,gid);
	END
}

int pfs_flock( int fd, int op )
{
	BEGIN
	debug(D_LIBCALL,"flock %d %d",fd,op);
	result = pfs_current->table->flock(fd,op);
	END
}

int pfs_chdir( const char *path )
{
	BEGIN
	debug(D_LIBCALL,"chdir %s",path);
	result = pfs_current->table->chdir(path);
	END
}

char * pfs_getcwd( char *path, pfs_size_t size )
{
	char *result;
	debug(D_LIBCALL,"getcwd %p %d",path,(int)size);
	result = pfs_current->table->getcwd(path,size);
	debug(D_LIBCALL,"= %s",result ? result : "(null)");
	return result;
}

int pfs_mount( const char *path, const char *device, const char *mode )
{
	BEGIN
	debug(D_LIBCALL,"mount %s %s %s",path,device,mode);

	if(!path && !device) {
		pfs_resolve_seal_ns();
		result = 0;
	} else {
		if(path[0]!='/') {
			result = -1;
			errno = EINVAL;
		} else {
			pfs_resolve_add_entry(path,device,pfs_mountfile_parse_mode(mode));
			result = 0;
		}
	}

	END
}

int pfs_unmount( const char *path )
{
	BEGIN
	debug(D_LIBCALL,"unmount %s",path);
	if(path[0]!='/') {
		result = -1;
		errno = EINVAL;
	} else if(pfs_resolve_remove_entry(path)) {
		result = 0;
	} else {
		result = -1;
		errno = EINVAL;
	}
	END
}

int pfs_stat( const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"stat %s %p",path,buf);
	result = pfs_current->table->stat(path,buf);
	END
}

int pfs_statfs( const char *path, struct pfs_statfs *buf )
{
	BEGIN
	debug(D_LIBCALL,"statfs %s %p",path,buf);
	result = pfs_current->table->statfs(path,buf);
	END
}

int pfs_lstat( const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"lstat %s %p",path,buf);
	result = pfs_current->table->lstat(path,buf);
	END
}

int pfs_statx( int dirfd, const char *pathname, int flags, unsigned int mask, struct pfs_statx *buf ) {
	BEGIN
	char newpath[PFS_PATH_MAX];

	//statx:
	//If pathname starts with a /, it is absolute and it is the path used.
	//Otherwise, if pathname is not NULL or an empty string, it is taken as a
	//relative path from dirfd. dirfd must be a file descriptor to an opened
	//directory, or AT_FDCWD. If AT_FDCWD, paths are relative to the current
	//workind directory.
	//Otherwise, if pathnames is NULL or the empty string, and flags has
	//AT_EMPTY_PATH, then the path is taken from the dirfd.
	//Further, if flags contains AT_SYMLINK_NOFOLLOW, then statx does not
	//dereference the path if it points to a symlink.

	const char *path_ptr = pathname;
	//when pathname is the empty string, or NULL, then statx behaves like stat*at functions.
	if(!pathname || strnlen(pathname, 1) == 0) {
		path_ptr = NULL;
	}

	if (pfs_current->table->complete_at_path(dirfd,path_ptr,newpath) == -1) return -1;
	debug(D_LIBCALL,"statx %s %p",newpath,buf);

	//newpath is an absolute path after complete_at_path, thus we can drop the dirfd argument to statx
	result = pfs_current->table->statx(newpath,flags,mask,buf);
	END
}

int pfs_access( const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"access %s %d",path,mode);
	result = pfs_current->table->access(path,mode);
	END
}

int pfs_chmod( const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"chmod %s %o",path,mode);
	result = pfs_current->table->chmod(path,mode);
	END
}

int pfs_chown( const char *path, struct pfs_process *p, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"chown %s %d %d",path,uid,gid);
	result = pfs_current->table->chown(path,p,uid,gid);
	END
}

int pfs_lchown( const char *path, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"lchown %s %d %d",path,uid,gid);
	result = pfs_current->table->lchown(path,uid,gid);
	END
}

int pfs_truncate( const char *path, pfs_off_t length )
{
	BEGIN
	debug(D_LIBCALL,"truncate %s %lld",path,(long long)length);
	result = pfs_current->table->truncate(path,length);
	END
}

int pfs_utime( const char *path, struct utimbuf *buf )
{
	BEGIN
	debug(D_LIBCALL,"utime %s %p",path,buf);
	result = pfs_current->table->utime(path,buf);
	END
}

int pfs_unlink( const char *path )
{
	BEGIN
	debug(D_LIBCALL,"unlink %s",path);
	result = pfs_current->table->unlink(path);
	END
}

int pfs_rename( const char *oldpath, const char *newpath )
{
	BEGIN
	debug(D_LIBCALL,"rename %s %s",oldpath,newpath);
	result = pfs_current->table->rename(oldpath,newpath);
	END
}

int pfs_link( const char *oldpath, const char *newpath )
{
	BEGIN
	debug(D_LIBCALL,"link %s %s",oldpath,newpath);
	result = pfs_current->table->link(oldpath,newpath);
	END
}

int pfs_symlink( const char *target, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"symlink %s %s",target,path);
	result = pfs_current->table->symlink(target,path);
	END
}

int pfs_readlink( const char *path, char *buf, pfs_size_t size )
{
	BEGIN
	  debug(D_LIBCALL,"readlink %s %p %lld",path,buf,(long long)size);
	result = pfs_current->table->readlink(path,buf,size);
	END
}

int pfs_mknod( const char *path, mode_t mode, dev_t dev )
{
	BEGIN
	  debug(D_LIBCALL,"mknod %s %d %d",path,mode,(int)dev);
	result = pfs_current->table->mknod(path,mode,dev);
	END
}

int pfs_mkdir( const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"mkdir %s %d",path,mode);
	result = pfs_current->table->mkdir(path,mode);
	END
}

int pfs_rmdir( const char *path )
{
	BEGIN
	debug(D_LIBCALL,"rmdir %s",path);
	result = pfs_current->table->rmdir(path);
	END
}

struct dirent * pfs_fdreaddir( int fd )
{
	struct dirent *result;
	debug(D_LIBCALL,"fdreaddir %d",fd);
	result = pfs_current->table->fdreaddir(fd);
	debug(D_LIBCALL,"= %s",result ? result->d_name : "null");
	return result;
}

extern int pfs_main_timeout;
int pfs_timeout( const char *str )
{
	BEGIN
	debug(D_LIBCALL, "timeout %s", str);
	if(str) pfs_main_timeout = string_time_parse(str);
	else if(isatty(0)) pfs_main_timeout = 300;
	else pfs_main_timeout = 3600;
	result = pfs_main_timeout;
	END
}

int pfs_mkalloc( const char *path, pfs_ssize_t size, mode_t mode )
{
	BEGIN
	  debug(D_LIBCALL,"mkalloc %s %lld %d",path,(long long)size,mode);
	result = pfs_current->table->mkalloc(path,size,mode);
	END
}

int pfs_lsalloc( const char *path, char *alloc_path, pfs_ssize_t *total, pfs_ssize_t *inuse )
{
	BEGIN
	debug(D_LIBCALL,"lsalloc %s",path);
	result = pfs_current->table->lsalloc(path,alloc_path,total,inuse);
	END
}

int pfs_whoami( const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL,"whoami %s %p %d",path,buf,size);
	result = pfs_current->table->whoami(path,buf,size);
	END
}

int pfs_search( const char *paths, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i)
{
	BEGIN
	debug(D_LIBCALL,"search %s %s %d %p %zu",paths,pattern,flags,buffer,buffer_length);
	result = pfs_current->table->search(paths,pattern,flags,buffer,buffer_length, i);
	END
}

int pfs_getacl( const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL,"getacl %s %p %d",path,buf,size);
	result = pfs_current->table->getacl(path,buf,size);
	END
}

int pfs_setacl( const char *path, const char *subject, const char *rights )
{
	BEGIN
	debug(D_LIBCALL,"setacl %s %s %s",path,subject,rights);
	result = pfs_current->table->setacl(path,subject,rights);
	END
}

int pfs_locate( const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL, "pfs_locate %s %p %d", path, buf, size);
	result = pfs_current->table->locate(path,buf,size);
	END
}

int pfs_copyfile( const char *source, const char *target )
{
	BEGIN
	debug(D_LIBCALL,"copyfile %s %s",source,target);
	result = pfs_current->table->copyfile(source,target);
	END
}

int pfs_fcopyfile( int srcfd, int dstfd )
{
	BEGIN
	debug(D_LIBCALL,"fcopyfile %d %d",srcfd,dstfd);
	result = pfs_current->table->fcopyfile(srcfd,dstfd);
	END
}

int pfs_md5( const char *path, unsigned char *digest )
{
	BEGIN
	debug(D_LIBCALL,"md5 %s",path);
	result = pfs_current->table->md5(path,digest);
	END
}

int pfs_get_real_fd( int fd )
{
	BEGIN
	debug(D_LIBCALL,"get_real_fd %d",fd);
	result = pfs_current->table->get_real_fd(fd);
	END
}

int pfs_get_full_name( int fd, char *name )
{
	BEGIN
	debug(D_LIBCALL,"get_full_name %d",fd);
	result = pfs_current->table->get_full_name(fd,name);
	END
}

pfs_size_t pfs_mmap_create( int fd, pfs_size_t file_offset, size_t length, int prot, int flags )
{
	BEGIN
	debug(D_LIBCALL,"mmap_create %d %llx %" PRIxPTR " %x %x",fd,(long long)file_offset,length,prot,flags);
	result = pfs_current->table->mmap_create(fd,file_offset,length,prot,flags);
	END
}

int	pfs_mmap_update( uintptr_t logical_address, pfs_size_t channel_address )
{
	BEGIN
	debug(D_LIBCALL,"mmap_update %016" PRIxPTR " %llx",logical_address,(long long)channel_address);
	result = pfs_current->table->mmap_update(logical_address,channel_address);
	END
}

int	pfs_mmap_delete( uintptr_t logical_address, size_t length )
{
	BEGIN
	debug(D_LIBCALL,"mmap_delete %016" PRIxPTR " %zu",logical_address,length);
	result = pfs_current->table->mmap_delete(logical_address,length);
	END
}

int pfs_get_local_name( const char *rpath, char *lpath, char *firstline, size_t length )
{
	int fd;
	int result;

	fd = pfs_open_cached(rpath,O_RDONLY,0,NULL,0);
	if(fd>=0) {
		if(firstline) {
			int actual = pfs_read(fd,firstline,length-1);
			if(actual>=0) {
				char *n;
				firstline[actual] = 0;
				n = strchr(firstline,'\n');
				if(n) {
					*n = 0;
				} else {
					firstline[0] = 0;
				}
			} else {
				firstline[0] = 0;
			}
		}
		result = pfs_current->table->get_local_name(fd,lpath);
		pfs_close(fd);
		return result;
	} else {
		return -1;
	}
}

/*
A proposed POSIX standard includes a number of new system calls
ending in -at, corresponding to traditional system calls.
Each one takes a directory fd and resolves relative paths in
relation to that fd.  This avoids some race-conditions (good idea)
and allows for per-thread working directories (bad idea).
Instead of propagating these new calls all the way down through Parrot,
we reduce them to traditional calls at this interface.
*/

int pfs_openat( int dirfd, const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_open(newpath,flags,mode,native_path,len);
}

int pfs_mkdirat( int dirfd, const char *path, mode_t mode)
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_mkdir(newpath,mode);
}

int pfs_mknodat( int dirfd, const char *path, mode_t mode, dev_t dev )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_mknod(newpath,mode,dev);
}

int pfs_fchownat( int dirfd, const char *path, struct pfs_process *p, uid_t owner, gid_t group, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_SYMLINK_NOFOLLOW
	if(flags&AT_SYMLINK_NOFOLLOW) {
		return pfs_lchown(newpath,owner,group);
	}
#endif
	return pfs_chown(newpath,p,owner,group);
}

int pfs_futimesat( int dirfd, const char *path, const struct timeval times[2] )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;

	struct utimbuf ut;
	if(times) {
		ut.actime = times[0].tv_sec;
		ut.modtime = times[1].tv_sec;
	} else {
		ut.actime = ut.modtime = time(0);
	}
	return pfs_utime(newpath,&ut);
}

static int pfs_utimens( const char *path, const struct timespec times[2] )
{
	BEGIN
	debug(D_LIBCALL,"utimens `%s' %p",path,times);
	result = pfs_current->table->utimens(path,times);
	END
}

static int pfs_lutimens( const char *path, const struct timespec times[2] )
{
	BEGIN
	debug(D_LIBCALL,"lutimens `%s' %p",path,times);
	result = pfs_current->table->lutimens(path,times);
	END
}

int pfs_utimensat( int dirfd, const char *path, const struct timespec times[2], int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;

	debug(D_LIBCALL,"utimensat %d `%s' %p %d",dirfd,path,times,flags);
#ifdef AT_SYMLINK_NOFOLLOW
	if (flags == AT_SYMLINK_NOFOLLOW)
		return pfs_lutimens(newpath,times);
	else
#endif
	return pfs_utimens(newpath,times);
}

int pfs_fstatat( int dirfd, const char *path, struct pfs_stat *buf, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_SYMLINK_NOFOLLOW
	if(flags&AT_SYMLINK_NOFOLLOW) {
		return pfs_lstat(newpath,buf);
	}
#endif
	return pfs_stat(newpath,buf);
}

int pfs_unlinkat( int dirfd, const char *path, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_REMOVEDIR
	if(flags&AT_REMOVEDIR) {
		return pfs_rmdir(newpath);
	}
#endif
	return pfs_unlink(newpath);
}

int pfs_renameat( int olddirfd, const char *oldpath, int newdirfd, const char *newpath )
{
	char newoldpath[PFS_PATH_MAX];
	char newnewpath[PFS_PATH_MAX];

	if (pfs_current->table->complete_at_path(olddirfd,oldpath,newoldpath) == -1) return -1;
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;

	return pfs_rename(newoldpath,newnewpath);
}

int pfs_linkat( int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags )
{
	char newoldpath[PFS_PATH_MAX];
	char newnewpath[PFS_PATH_MAX];

	if (pfs_current->table->complete_at_path(olddirfd,oldpath,newoldpath) == -1) return -1;
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;

	return pfs_link(newoldpath,newnewpath);
}


int pfs_symlinkat( const char *oldpath, int newdirfd, const char *newpath )
{
	char newnewpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;
	return pfs_symlink(oldpath,newnewpath);
}

int pfs_readlinkat( int dirfd, const char *path, char *buf, size_t bufsiz )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_readlink(newpath,buf,bufsiz);
}

int pfs_fchmodat( int dirfd, const char *path, mode_t mode, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_chmod(newpath,mode);
}

int pfs_faccessat( int dirfd, const char *path, mode_t mode )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_access(newpath,mode);
}

ssize_t pfs_getxattr (const char *path, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"getxattr %s %s",path,name);
	result = pfs_current->table->getxattr(path,name,value,size);
	END
}

ssize_t pfs_lgetxattr (const char *path, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"lgetxattr %s %s",path,name);
	result = pfs_current->table->lgetxattr(path,name,value,size);
	END
}

ssize_t pfs_fgetxattr (int fd, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"fgetxattr %d %s",fd,name);
	result = pfs_current->table->fgetxattr(fd,name,value,size);
	END
}

ssize_t pfs_listxattr (const char *path, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"listxattr %s",path);
	result = pfs_current->table->listxattr(path,list,size);
	END
}

ssize_t pfs_llistxattr (const char *path, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"llistxattr %s",path);
	result = pfs_current->table->llistxattr(path,list,size);
	END
}

ssize_t pfs_flistxattr (int fd, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"flistxattr %d",fd);
	result = pfs_current->table->flistxattr(fd,list,size);
	END
}

int pfs_setxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"setxattr %s %s <> %zu %d",path,name,size,flags);
	result = pfs_current->table->setxattr(path,name,value,size,flags);
	END
}

int pfs_lsetxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"lsetxattr %s %s <> %zu %d",path,name,size,flags);
	result = pfs_current->table->lsetxattr(path,name,value,size,flags);
	END
}

int pfs_fsetxattr (int fd, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"fsetxattr %d %s <> %zu %d",fd,name,size,flags);
	result = pfs_current->table->fsetxattr(fd,name,value,size,flags);
	END
}

int pfs_removexattr (const char *path, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"removexattr %s %s",path,name);
	result = pfs_current->table->removexattr(path,name);
	END
}

int pfs_lremovexattr (const char *path, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"lremovexattr %s %s",path,name);
	result = pfs_current->table->lremovexattr(path,name);
	END
}

int pfs_fremovexattr (int fd, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"fremovexattr %d %s",fd,name);
	result = pfs_current->table->fremovexattr(fd,name);
	END
}

/* vim: set noexpandtab tabstop=8: */
