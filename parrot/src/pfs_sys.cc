/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
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
#include "stringtools.h"
#include "pfs_resolve.h"
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
extern int pfs_allow_dynamic_mounts;

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

int pfs_open( struct pfs_mount_entry *ns, const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(ns,path,flags,mode,pfs_force_cache,native_path,len);
	END
}

int pfs_open_cached( struct pfs_mount_entry *ns, const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(ns,path,flags,mode,1,native_path,len);
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

int pfs_chdir( struct pfs_mount_entry *ns, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"chdir %s",path);
	result = pfs_current->table->chdir(ns,path);
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

int pfs_mount( struct pfs_mount_entry **ns, const char *path, const char *device, const char *mode )
{
	BEGIN
	debug(D_LIBCALL,"mount %s %s %s",path,device,mode);

	if(!path && !device) {
		pfs_allow_dynamic_mounts = 0;
		result = 0;
	} else if(pfs_allow_dynamic_mounts) {
		if(path[0]!='/') {
			result = -1;
			errno = EINVAL;
		} else {
			pfs_resolve_add_entry(ns,path,device,pfs_resolve_parse_mode(mode));
			result = 0;
		}
	} else {
		result = -1;
		errno = EPERM;
	}

	END
}

int pfs_unmount( struct pfs_mount_entry **ns, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"unmount %s",path);
	if(pfs_allow_dynamic_mounts) {
		if(path[0]!='/') {
			result = -1;
			errno = EINVAL;
		} else if(pfs_resolve_remove_entry(ns, path)) {
			result = 0;
		} else {
			result = -1;
			errno = EINVAL;
		}
	} else {
		result = -1;
		errno = EPERM;
	}
	END
}

int pfs_stat( struct pfs_mount_entry *ns, const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"stat %s %p",path,buf);
	result = pfs_current->table->stat(ns,path,buf);
	END
}

int pfs_statfs( struct pfs_mount_entry *ns, const char *path, struct pfs_statfs *buf )
{
	BEGIN
	debug(D_LIBCALL,"statfs %s %p",path,buf);
	result = pfs_current->table->statfs(ns,path,buf);
	END
}

int pfs_lstat( struct pfs_mount_entry *ns, const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"lstat %s %p",path,buf);
	result = pfs_current->table->lstat(ns,path,buf);
	END
}

int pfs_access( struct pfs_mount_entry *ns, const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"access %s %d",path,mode);
	result = pfs_current->table->access(ns,path,mode);
	END
}

int pfs_chmod( struct pfs_mount_entry *ns, const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"chmod %s %o",path,mode);
	result = pfs_current->table->chmod(ns,path,mode);
	END
}

int pfs_chown( struct pfs_mount_entry *ns, const char *path, struct pfs_process *p, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"chown %s %d %d",path,uid,gid);
	result = pfs_current->table->chown(ns,path,p,uid,gid);
	END
}

int pfs_lchown( struct pfs_mount_entry *ns, const char *path, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"lchown %s %d %d",path,uid,gid);
	result = pfs_current->table->lchown(ns,path,uid,gid);
	END
}

int pfs_truncate( struct pfs_mount_entry *ns, const char *path, pfs_off_t length )
{
	BEGIN
	debug(D_LIBCALL,"truncate %s %lld",path,(long long)length);
	result = pfs_current->table->truncate(ns,path,length);
	END
}

int pfs_utime( struct pfs_mount_entry *ns, const char *path, struct utimbuf *buf )
{
	BEGIN
	debug(D_LIBCALL,"utime %s %p",path,buf);
	result = pfs_current->table->utime(ns,path,buf);
	END
}

int pfs_unlink( struct pfs_mount_entry *ns, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"unlink %s",path);
	result = pfs_current->table->unlink(ns,path);
	END
}

int pfs_rename( struct pfs_mount_entry *ns, const char *oldpath, const char *newpath )
{
	BEGIN
	debug(D_LIBCALL,"rename %s %s",oldpath,newpath);
	result = pfs_current->table->rename(ns,oldpath,newpath);
	END
}

int pfs_link( struct pfs_mount_entry *ns, const char *oldpath, const char *newpath )
{
	BEGIN
	debug(D_LIBCALL,"link %s %s",oldpath,newpath);
	result = pfs_current->table->link(ns,oldpath,newpath);
	END
}

int pfs_symlink( struct pfs_mount_entry *ns, const char *target, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"symlink %s %s",target,path);
	result = pfs_current->table->symlink(ns,target,path);
	END
}

int pfs_readlink( struct pfs_mount_entry *ns, const char *path, char *buf, pfs_size_t size )
{
	BEGIN
	  debug(D_LIBCALL,"readlink %s %p %lld",path,buf,(long long)size);
	result = pfs_current->table->readlink(ns,path,buf,size);
	END
}

int pfs_mknod( struct pfs_mount_entry *ns, const char *path, mode_t mode, dev_t dev )
{
	BEGIN
	  debug(D_LIBCALL,"mknod %s %d %d",path,mode,(int)dev);
	result = pfs_current->table->mknod(ns,path,mode,dev);
	END
}

int pfs_mkdir( struct pfs_mount_entry *ns, const char *path, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"mkdir %s %d",path,mode);
	result = pfs_current->table->mkdir(ns,path,mode);
	END
}

int pfs_rmdir( struct pfs_mount_entry *ns, const char *path )
{
	BEGIN
	debug(D_LIBCALL,"rmdir %s",path);
	result = pfs_current->table->rmdir(ns,path);
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

extern int pfs_master_timeout;
int pfs_timeout( const char *str )
{
	BEGIN
	debug(D_LIBCALL, "timeout %s", str);
	if(str) pfs_master_timeout = string_time_parse(str);
	else if(isatty(0)) pfs_master_timeout = 300;
	else pfs_master_timeout = 3600;
	result = pfs_master_timeout;
	END
}

int pfs_mkalloc( struct pfs_mount_entry *ns, const char *path, pfs_ssize_t size, mode_t mode )
{
	BEGIN
	  debug(D_LIBCALL,"mkalloc %s %lld %d",path,(long long)size,mode);
	result = pfs_current->table->mkalloc(ns,path,size,mode);
	END
}

int pfs_lsalloc( struct pfs_mount_entry *ns, const char *path, char *alloc_path, pfs_ssize_t *total, pfs_ssize_t *inuse )
{
	BEGIN
	debug(D_LIBCALL,"lsalloc %s",path);
	result = pfs_current->table->lsalloc(ns,path,alloc_path,total,inuse);
	END
}

int pfs_whoami( struct pfs_mount_entry *ns, const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL,"whoami %s %p %d",path,buf,size);
	result = pfs_current->table->whoami(ns,path,buf,size);
	END
}

int pfs_search( struct pfs_mount_entry *ns, const char *paths, const char *pattern, int flags, char *buffer, size_t buffer_length, size_t *i)
{
	BEGIN
	debug(D_LIBCALL,"search %s %s %d %p %zu",paths,pattern,flags,buffer,buffer_length);
	result = pfs_current->table->search(ns,paths,pattern,flags,buffer,buffer_length, i);
	END
}

int pfs_getacl( struct pfs_mount_entry *ns, const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL,"getacl %s %p %d",path,buf,size);
	result = pfs_current->table->getacl(ns,path,buf,size);
	END
}

int pfs_setacl( struct pfs_mount_entry *ns, const char *path, const char *subject, const char *rights )
{
	BEGIN
	debug(D_LIBCALL,"setacl %s %s %s",path,subject,rights);
	result = pfs_current->table->setacl(ns,path,subject,rights);
	END
}

int pfs_locate( struct pfs_mount_entry *ns, const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL, "pfs_locate %s %p %d", path, buf, size);
	result = pfs_current->table->locate(ns,path,buf,size);
	END
}

int pfs_copyfile( struct pfs_mount_entry *ns, const char *source, const char *target )
{
	BEGIN
	debug(D_LIBCALL,"copyfile %s %s",source,target);
	result = pfs_current->table->copyfile(ns,source,target);
	END
}

int pfs_fcopyfile( int srcfd, int dstfd )
{
	BEGIN
	debug(D_LIBCALL,"fcopyfile %d %d",srcfd,dstfd);
	result = pfs_current->table->fcopyfile(srcfd,dstfd);
	END
}

int pfs_md5( struct pfs_mount_entry *ns, const char *path, unsigned char *digest )
{
	BEGIN
	debug(D_LIBCALL,"md5 %s",path);
	result = pfs_current->table->md5(ns,path,digest);
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

int pfs_get_local_name( struct pfs_mount_entry *ns, const char *rpath, char *lpath, char *firstline, size_t length )
{
	int fd;
	int result;

	fd = pfs_open_cached(ns,rpath,O_RDONLY,0,NULL,0);
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

int pfs_openat( struct pfs_mount_entry *ns, int dirfd, const char *path, int flags, mode_t mode, char *native_path, size_t len )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_open(ns,newpath,flags,mode,native_path,len);
}

int pfs_mkdirat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode)
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_mkdir(ns,newpath,mode);
}

int pfs_mknodat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode, dev_t dev )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_mknod(ns,newpath,mode,dev);
}

int pfs_fchownat( struct pfs_mount_entry *ns, int dirfd, const char *path, struct pfs_process *p, uid_t owner, gid_t group, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_SYMLINK_NOFOLLOW
	if(flags&AT_SYMLINK_NOFOLLOW) {
		return pfs_lchown(ns,newpath,owner,group);
	}
#endif
	return pfs_chown(ns,newpath,p,owner,group);
}

int pfs_futimesat( struct pfs_mount_entry *ns, int dirfd, const char *path, const struct timeval times[2] )
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
	return pfs_utime(ns,newpath,&ut);
}

static int pfs_utimens(struct pfs_mount_entry *ns,  const char *path, const struct timespec times[2] )
{
	BEGIN
	debug(D_LIBCALL,"utimens `%s' %p",path,times);
	result = pfs_current->table->utimens(ns,path,times);
	END
}

static int pfs_lutimens(struct pfs_mount_entry *ns,  const char *path, const struct timespec times[2] )
{
	BEGIN
	debug(D_LIBCALL,"lutimens `%s' %p",path,times);
	result = pfs_current->table->lutimens(ns,path,times);
	END
}

int pfs_utimensat( struct pfs_mount_entry *ns, int dirfd, const char *path, const struct timespec times[2], int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;

	debug(D_LIBCALL,"utimensat %d `%s' %p %d",dirfd,path,times,flags);
#ifdef AT_SYMLINK_NOFOLLOW
	if (flags == AT_SYMLINK_NOFOLLOW)
		return pfs_lutimens(ns,newpath,times);
	else
#endif
	return pfs_utimens(ns,newpath,times);
}

int pfs_fstatat( struct pfs_mount_entry *ns, int dirfd, const char *path, struct pfs_stat *buf, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_SYMLINK_NOFOLLOW
	if(flags&AT_SYMLINK_NOFOLLOW) {
		return pfs_lstat(ns,newpath,buf);
	}
#endif
	return pfs_stat(ns,newpath,buf);
}

int pfs_unlinkat( struct pfs_mount_entry *ns, int dirfd, const char *path, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
#ifdef AT_REMOVEDIR
	if(flags&AT_REMOVEDIR) {
		return pfs_rmdir(ns,newpath);
	}
#endif
	return pfs_unlink(ns,newpath);
}

int pfs_renameat( struct pfs_mount_entry *ns, int olddirfd, const char *oldpath, int newdirfd, const char *newpath )
{
	char newoldpath[PFS_PATH_MAX];
	char newnewpath[PFS_PATH_MAX];

	if (pfs_current->table->complete_at_path(olddirfd,oldpath,newoldpath) == -1) return -1;
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;

	return pfs_rename(ns,newoldpath,newnewpath);
}

int pfs_linkat( struct pfs_mount_entry *ns, int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags )
{
	char newoldpath[PFS_PATH_MAX];
	char newnewpath[PFS_PATH_MAX];

	if (pfs_current->table->complete_at_path(olddirfd,oldpath,newoldpath) == -1) return -1;
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;

	return pfs_link(ns,newoldpath,newnewpath);
}


int pfs_symlinkat( struct pfs_mount_entry *ns, const char *oldpath, int newdirfd, const char *newpath )
{
	char newnewpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(newdirfd,newpath,newnewpath) == -1) return -1;
	return pfs_symlink(ns,oldpath,newnewpath);
}

int pfs_readlinkat( struct pfs_mount_entry *ns, int dirfd, const char *path, char *buf, size_t bufsiz )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_readlink(ns,newpath,buf,bufsiz);
}

int pfs_fchmodat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode, int flags )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_chmod(ns,newpath,mode);
}

int pfs_faccessat( struct pfs_mount_entry *ns, int dirfd, const char *path, mode_t mode )
{
	char newpath[PFS_PATH_MAX];
	if (pfs_current->table->complete_at_path(dirfd,path,newpath) == -1) return -1;
	return pfs_access(ns,newpath,mode);
}

ssize_t pfs_getxattr (struct pfs_mount_entry *ns, const char *path, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"getxattr %s %s",path,name);
	result = pfs_current->table->getxattr(ns,path,name,value,size);
	END
}

ssize_t pfs_lgetxattr (struct pfs_mount_entry *ns, const char *path, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"lgetxattr %s %s",path,name);
	result = pfs_current->table->lgetxattr(ns,path,name,value,size);
	END
}

ssize_t pfs_fgetxattr (int fd, const char *name, void *value, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"fgetxattr %d %s",fd,name);
	result = pfs_current->table->fgetxattr(fd,name,value,size);
	END
}

ssize_t pfs_listxattr (struct pfs_mount_entry *ns, const char *path, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"listxattr %s",path);
	result = pfs_current->table->listxattr(ns,path,list,size);
	END
}

ssize_t pfs_llistxattr (struct pfs_mount_entry *ns, const char *path, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"llistxattr %s",path);
	result = pfs_current->table->llistxattr(ns,path,list,size);
	END
}

ssize_t pfs_flistxattr (int fd, char *list, size_t size)
{
	BEGIN
	debug(D_LIBCALL,"flistxattr %d",fd);
	result = pfs_current->table->flistxattr(fd,list,size);
	END
}

int pfs_setxattr (struct pfs_mount_entry *ns, const char *path, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"setxattr %s %s <> %zu %d",path,name,size,flags);
	result = pfs_current->table->setxattr(ns,path,name,value,size,flags);
	END
}

int pfs_lsetxattr (struct pfs_mount_entry *ns, const char *path, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"lsetxattr %s %s <> %zu %d",path,name,size,flags);
	result = pfs_current->table->lsetxattr(ns,path,name,value,size,flags);
	END
}

int pfs_fsetxattr (int fd, const char *name, const void *value, size_t size, int flags)
{
	BEGIN
	debug(D_LIBCALL,"fsetxattr %d %s <> %zu %d",fd,name,size,flags);
	result = pfs_current->table->fsetxattr(fd,name,value,size,flags);
	END
}

int pfs_removexattr (struct pfs_mount_entry *ns, const char *path, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"removexattr %s %s",path,name);
	result = pfs_current->table->removexattr(ns,path,name);
	END
}

int pfs_lremovexattr (struct pfs_mount_entry *ns, const char *path, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"lremovexattr %s %s",path,name);
	result = pfs_current->table->lremovexattr(ns,path,name);
	END
}

int pfs_fremovexattr (int fd, const char *name)
{
	BEGIN
	debug(D_LIBCALL,"fremovexattr %d %s",fd,name);
	result = pfs_current->table->fremovexattr(fd,name);
	END
}

/* vim: set noexpandtab tabstop=4: */
