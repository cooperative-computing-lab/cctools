/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "get_canonical_path.h"
#include "username.h"
#include "ibox_acl.h"
#include "stats.h"
#include "stringtools.h"
}

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <sys/file.h>
#include <sys/time.h>
#include <utime.h>
#include <dirent.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif
#ifndef ENOATTR
#define ENOATTR  EINVAL
#endif

#define END \
	if (result >= 0)\
		debug(D_LOCAL, "= %d [%s]",(int)result,__func__);\
	else\
		debug(D_LOCAL, "= %d %s [%s]",(int)result,strerror(errno),__func__);\
	return result;

extern "C" ssize_t pread(int fd, void *buf, size_t count, off_t  offset);
extern "C" ssize_t pwrite(int  fd,  const  void  *buf, size_t count, off_t offset);

extern const char * pfs_username;

static int check_implicit_acl( const char *path, int checkflags )
{
	int flags=0;
	struct stat64 info;

	if(stat64(path,&info)==0) {
		if(info.st_mode&S_IWOTH) flags |= IBOX_ACL_WRITE|IBOX_ACL_LIST;
		if(info.st_mode&S_IROTH) flags |= IBOX_ACL_READ|IBOX_ACL_LIST;
		if(info.st_mode&S_IXOTH) flags |= IBOX_ACL_EXECUTE;
		if((flags&checkflags)==checkflags) {
			return 1;
		} else {
			errno = EACCES;
			return 0;
		}
	} else {
		// most likely: file not found
		// errno set by stat
		return 0;
	}
}

static int pfs_acl_check( pfs_name *name, int flags )
{
	if(!pfs_username) return 1;
	if(ibox_acl_check(name->rest,pfs_username,flags)) return 1;
	return check_implicit_acl(name->rest,flags);
}

static int pfs_acl_check_dir( pfs_name *name, int flags )
{
	if(!pfs_username) return 1;
	if(ibox_acl_check_dir(name->rest,pfs_username,flags)) return 1;
	return check_implicit_acl(name->rest,flags);
}

class pfs_file_local : public pfs_file
{
private:
	int fd;
	pfs_off_t last_offset;

public:
	pfs_file_local( pfs_name *name, int f ) : pfs_file(name) {
		assert(f >= 0);
		fd = f;
		last_offset = 0;
	}

	virtual int canbenative (char *path, size_t len) {
		struct stat64 buf;
		if (::fstat64(fd, &buf) == 0 && (S_ISSOCK(buf.st_mode) || S_ISBLK(buf.st_mode) || S_ISCHR(buf.st_mode) || S_ISFIFO(buf.st_mode))) {
			snprintf(path, len, "%s", name.rest);
			return 1;
		}
		return 0;
	}

	virtual int close() {
		stats_inc("parrot.local.close", 1);
		int result;
		debug(D_LOCAL,"close %d",fd);
		result = ::close(fd);
		END
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		stats_inc("parrot.local.read", 1);
		stats_bin("parrot.local.read.requested", length);

		pfs_ssize_t result;

		debug(D_LOCAL,"read %d %p %lld %lld",fd,data,(long long)length,(long long)offset);

		if(offset!=last_offset) ::lseek64(fd,offset,SEEK_SET);
		result = ::read(fd,data,length);
		if(result>0) last_offset = offset+result;
		if (result >= 0) stats_bin("parrot.local.read.actual", result);

		END
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		stats_inc("parrot.local.write", 1);
		stats_bin("parrot.local.write.requested", length);

		pfs_ssize_t result;
		debug(D_LOCAL,"write %d %p %lld %lld",fd,data,(long long)length,(long long)offset);
		if(offset!=last_offset) ::lseek64(fd,offset,SEEK_SET);
		result = ::write(fd,data,length);
		if(result>0) last_offset = offset+result;
		if (result >= 0) stats_bin("parrot.local.write.actual", result);
		END
	}

	virtual int fstat( struct pfs_stat *buf ) {
		stats_inc("parrot.local.fstat", 1);
		int result;
		struct stat64 lbuf;
		debug(D_LOCAL,"fstat %d %p",fd,buf);
		result = ::fstat64(fd,&lbuf);
		if(result>=0) {
				COPY_STAT(lbuf,*buf);
		}
		END
	}

	virtual int fstatfs( struct pfs_statfs *buf ) {
		stats_inc("parrot.local.fstatfs", 1);
		int result;
		struct statfs64 lbuf;
		debug(D_LOCAL,"fstatfs %d %p",fd,buf);
		result = ::fstatfs64(fd,&lbuf);
		if(result>=0){
				COPY_STATFS(lbuf,*buf);
		}
		END
	}

	virtual int ftruncate( pfs_size_t length ) {
		stats_inc("parrot.local.ftruncate", 1);
		int result;
		debug(D_LOCAL,"truncate %d %lld",fd,(long long)length);
		result = ::ftruncate64(fd,length);
		END
	}

	virtual int fsync() {
		stats_inc("parrot.local.fsync", 1);
		int result;
		debug(D_LOCAL,"fsync %d",fd);
		result = ::fsync(fd);
		END
	}

	virtual int fcntl( int cmd, void *arg ) {
		stats_inc("parrot.local.fcntl", 1);
		int result;
		debug(D_LOCAL,"fcntl %d %d %p",fd,cmd,arg);
		if(cmd==F_SETFL) arg = (void*)(((PTRINT_T)arg)|O_NONBLOCK);
#if defined(CCTOOLS_OPSYS_LINUX) && defined(CCTOOLS_CPU_X86_64)
		if (cmd == PFS_GETLK64) cmd = F_GETLK;
		if (cmd == PFS_SETLK64) cmd = F_SETLK;
		if (cmd == PFS_SETLKW64) cmd = F_SETLKW;
#endif
		result = ::fcntl(fd,cmd,arg);
		END
	}

	virtual int fchmod( mode_t mode ) {
		stats_inc("parrot.local.fchmod", 1);
		int result;
		debug(D_LOCAL,"fchmod %d %d",fd,mode);
		result = ::fchmod(fd,mode);
		END
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		stats_inc("parrot.local.fchown", 1);
		int result;
		debug(D_LOCAL,"fchown %d %d %d",fd,uid,gid);
		result = ::fchown(fd,uid,gid);
		END
	}

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	virtual ssize_t fgetxattr( const char *name, void *data, size_t size ) {
		stats_inc("parrot.local.fgetxattr", 1);
		ssize_t result;
		debug(D_LOCAL,"fgetxattr %d %s",fd,name);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::fgetxattr(fd, name, data, size, 0, 0);
#else
		result = ::fgetxattr(fd, name, data, size);
#endif
		END
	}

	virtual ssize_t flistxattr( char *list, size_t size ) {
		stats_inc("parrot.local.flistxattr", 1);
		ssize_t result;
		debug(D_LOCAL,"flistxattr %d",fd);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::flistxattr(fd, list, size, 0);
#else
		result = ::flistxattr(fd, list, size);
#endif
		END
	}

	virtual int fsetxattr( const char *name, const void *data, size_t size, int flags ) {
		stats_inc("parrot.local.fsetxattr", 1);
		int result;
		debug(D_LOCAL,"fsetxattr %d %s <> %d",fd,name,flags);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::fsetxattr(fd, name, data, size, 0, flags);
#else
		result = ::fsetxattr(fd, name, data, size, flags);
#endif
		END
	}

	virtual int fremovexattr( const char *name ) {
		stats_inc("parrot.local.fremovexattr", 1);
		int result;
		debug(D_LOCAL,"fremovexattr %d %s",fd,name);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::fremovexattr(fd, name, 0);
#else
		result = ::fremovexattr(fd, name);
#endif
		END
	}

#endif

	virtual int flock( int op ) {
		stats_inc("parrot.local.flock", 1);
		int result;
		debug(D_LOCAL,"flock %d %d",fd,op);
		result = ::flock(fd,op);
		END
	}

	virtual void * mmap( void *start, pfs_size_t length, int prot, int flags, off_t offset ) {
		stats_inc("parrot.local.mmap", 1);
		void *result;
		result = ::mmap((caddr_t)start,length,prot,flags,fd,offset);
		debug(D_LOCAL,"= %p %s",result,(((PTRINT_T)result>=0) ? "" : strerror(errno)) );
		return result;
	}

	virtual pfs_ssize_t get_size() {
		struct pfs_stat s;
		int result;
		result = this->fstat(&s);
		if(result<0) {
			return 0;
		} else {
			return s.st_size;
		}
	}
	virtual int get_real_fd() {
		return fd;
	}

	virtual int get_local_name( char *n )
	{
		strcpy(n,name.rest);
		return 0;
	}

	virtual int is_seekable()
	{
		return 1;
	}
};

class pfs_service_local : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		stats_inc("parrot.local.open", 1);

		pfs_file *result;

		if(!pfs_acl_check(name,ibox_acl_from_open_flags(flags))) return 0;

		flags |= O_NONBLOCK;
		debug(D_LOCAL,"open %s %d %d",name->rest,flags,(flags&O_CREAT) ? mode : 0);
RETRY:
		int fd = ::open64(name->rest,flags|O_NOCTTY,mode);
		if(fd>=0) {
			struct stat info;
			if (::fstat(fd, &info) == 0 && S_ISDIR(info.st_mode)) {
				result = 0;
				errno = EISDIR;
				::close(fd);
			} else {
				result = new pfs_file_local(name,fd);
			}
		} else if (errno == ENXIO && (flags&(O_WRONLY|O_RDWR)) == O_WRONLY) {
			// see the section on ENXIO in open(2) and also fifo(7)
			debug(D_LOCAL, "failed on fifo with no readers, retrying O_RDWR");
			flags &= ~O_WRONLY;
			flags |= O_RDWR;
			goto RETRY;
		} else {
			result = 0;
		}
		if (result)
			debug(D_LOCAL, "= %d [%s]",fd,__func__);
		else
			debug(D_LOCAL, "= %d %s [%s]",errno,strerror(errno),__func__);
		return result;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		stats_inc("parrot.local.getdir", 1);

		struct dirent *d;
		DIR *dir;
		pfs_dir *result = 0;

		if(!pfs_acl_check_dir(name,IBOX_ACL_LIST)) return 0;

		size_t dirsize = 0;
		debug(D_LOCAL,"getdir %s",name->rest);
		dir = ::opendir(name->rest);
		if(dir) {
			result = new pfs_dir(name);
			while((d=::readdir(dir))) {
				if(!strcmp(d->d_name,IBOX_ACL_BASE_NAME)) continue;
				result->append(d);
				++dirsize;
			}
			closedir(dir);
		} else {
			result = 0;
		}
		if (result) {
			stats_bin("parrot.local.getdir.size", dirsize);
			debug(D_LOCAL, "= 0 [%s]",__func__);
		} else {
			debug(D_LOCAL, "= %d %s [%s]",errno,strerror(errno),__func__);
		}
		return result;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		stats_inc("parrot.local.stat", 1);
		int result;
		struct stat64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"stat %s %p",name->rest,buf);
		result = ::stat64(name->rest,&lbuf);
		if(result>=0){
				COPY_STAT(lbuf,*buf);
		}
		END
	}
	virtual int statfs( pfs_name *name, struct pfs_statfs *buf ) {
		stats_inc("parrot.local.statfs", 1);
		int result;
		struct statfs64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"statfs %s %p",name->rest,buf);
		result = ::statfs64(name->rest,&lbuf);
		if(result>=0){
				COPY_STATFS(lbuf,*buf);
		}
		END
	}
	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		stats_inc("parrot.local.lstat", 1);
		int result;
		struct stat64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"lstat %s %p",name->rest,buf);
		result = ::lstat64(name->rest,&lbuf);
		if(result>=0){
				COPY_STAT(lbuf,*buf);
		}
		END
	}
	virtual int access( pfs_name *name, mode_t mode ) {
		stats_inc("parrot.local.access", 1);
		int result;
		if(!pfs_acl_check(name,ibox_acl_from_access_flags(mode))) return -1;
		debug(D_LOCAL,"access %s %d",name->rest,mode);
		result = ::access(name->rest,mode);
		END
	}
	virtual int chmod( pfs_name *name, mode_t mode ) {
		stats_inc("parrot.local.chmod", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"chmod %s %d",name->rest,mode);
		result = ::chmod(name->rest,mode);
		END
	}
	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		stats_inc("parrot.local.chown", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"chown %s %d %d",name->rest,uid,gid);
		result = ::chown(name->rest,uid,gid);
		END
	}
	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		stats_inc("parrot.local.lchown", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lchown %s %d %d",name->rest,uid,gid);
		result = ::lchown(name->rest,uid,gid);
		END
	}
	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		stats_inc("parrot.local.truncate", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"truncate %s %lld",name->rest,(long long)length);
		result = ::truncate64(name->rest,length);
		END
	}
	virtual int utime( pfs_name *name, struct utimbuf *buf ) {
		stats_inc("parrot.local.utime", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"utime %s %p",name->rest,buf);
		result = ::utime(name->rest,buf);
		END
	}
	virtual int utimens( pfs_name *name, const struct timespec times[2] ) {
		stats_inc("parrot.local.utimens", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		assert(*name->rest == '/');
#if _XOPEN_SOURCE >= 700 || _POSIX_C_SOURCE >= 200809L
		debug(D_LOCAL,"utimens %s %p",name->rest,times);
		result = ::utimensat(AT_FDCWD,name->rest,times,0);
#else
		debug(D_LOCAL,"(fallback) utime %s %p",name->rest,times);
		{
			struct utimbuf buf;
			buf.actime = times[0].tv_sec;
			buf.modtime = times[1].tv_sec;
			result = ::utime(name->rest,&buf);
		}
#endif
		END
	}
	virtual int lutimens( pfs_name *name, const struct timespec times[2] ) {
		stats_inc("parrot.local.lutimens", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lutimens %s %p",name->rest,times);
		assert(*name->rest == '/');
#ifdef AT_SYMLINK_NOFOLLOW
#  if _XOPEN_SOURCE >= 700 || _POSIX_C_SOURCE >= 200809L
		debug(D_LOCAL,"utimens %s %p",name->rest,times);
		result = ::utimensat(AT_FDCWD,name->rest,times,AT_SYMLINK_NOFOLLOW);
#  else
		debug(D_LOCAL,"(fallback) utime %s %p",name->rest,times);
		{
			struct utimbuf buf;
			buf.actime = times[0].tv_sec;
			buf.modtime = times[1].tv_sec;
			result = ::utime(name->rest,&buf);
		}
#  endif
#else
		result = -1;
		errno = ENOSYS;
#endif
		END
	}
	virtual int unlink( pfs_name *name ) {
		stats_inc("parrot.local.unlink", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"unlink %s",name->rest);
		result = ::unlink(name->rest);
		END
	}
	virtual int rename( pfs_name *oldname, pfs_name *newname ) {
		stats_inc("parrot.local.rename", 1);
		int result;
		if(!pfs_acl_check(oldname,IBOX_ACL_READ)) return -1;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"rename %s %s",oldname->rest,newname->rest);
		result = ::rename(oldname->rest,newname->rest);
		END
	}

	virtual ssize_t getxattr ( pfs_name *name, const char *attrname, void *data, size_t size )
	{
		stats_inc("parrot.local.getxattr", 1);
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"getxattr %s %s",name->rest,attrname);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::getxattr(name->rest, attrname, data, size, 0, 0);
#else
		result = ::getxattr(name->rest, attrname, data, size);
#endif
		END
	}

	virtual ssize_t lgetxattr ( pfs_name *name, const char *attrname, void *data, size_t size )
	{
		stats_inc("parrot.local.lgetxattr", 1);
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"lgetxattr %s %s",name->rest,attrname);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::getxattr(name->rest, attrname, data, size, 0, XATTR_NOFOLLOW);
#else
		result = ::lgetxattr(name->rest, attrname, data, size);
#endif
		END
	}

	virtual ssize_t listxattr ( pfs_name *name, char *list, size_t size )
	{
		stats_inc("parrot.local.listxattr", 1);
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"listxattr %s",name->rest);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::listxattr(name->rest, list, size, 0);
#else
		result = ::listxattr(name->rest, list, size);
#endif
		END
	}

	virtual ssize_t llistxattr ( pfs_name *name, char *list, size_t size )
	{
		stats_inc("parrot.local.llistxattr", 1);
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"llistxattr %s",name->rest);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::listxattr(name->rest, list, size, XATTR_NOFOLLOW);
#else
		result = ::llistxattr(name->rest, list, size);
#endif
		END
	}

	virtual int setxattr ( pfs_name *name, const char *attrname, const void *data, size_t size, int flags )
	{
		stats_inc("parrot.local.setxattr", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"setxattr %s %s <> %d",name->rest,attrname,flags);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::setxattr(name->rest, attrname, data, size, 0, flags);
#else
		result = ::setxattr(name->rest, attrname, data, size, flags);
#endif
		END
	}

	virtual int lsetxattr ( pfs_name *name, const char *attrname, const void *data, size_t size, int flags )
	{
		stats_inc("parrot.local.lsetxattr", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lsetxattr %s %s <> %d",name->rest,attrname,flags);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::setxattr(name->rest, attrname, data, size, 0, XATTR_NOFOLLOW|flags);
#else
		result = ::lsetxattr(name->rest, attrname, data, size, flags);
#endif
		END
	}

	virtual int removexattr ( pfs_name *name, const char *attrname )
	{
		stats_inc("parrot.local.removexattr", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"removexattr %s %s",name->rest,attrname);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::removexattr(name->rest, attrname, 0);
#else
		result = ::removexattr(name->rest, attrname);
#endif
		END
	}

	virtual int lremovexattr ( pfs_name *name, const char *attrname )
	{
		stats_inc("parrot.local.lremovexattr", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lremovexattr %s %s",name->rest,attrname);
#ifdef CCTOOLS_OPSYS_DARWIN
		result = ::removexattr(name->rest, attrname, XATTR_NOFOLLOW);
#else
		result = ::lremovexattr(name->rest, attrname);
#endif
		END
	}

	/*
	We do not actually change to the new directory,
	because this is performed within the PFS master
	process, and we do not want to change the meaning
	of open() on filenames used for configuration,
	security and so forth.  We also do not change and then
	move back, because what will we do if the chdir
	back fails?
	*/
	virtual int chdir( pfs_name *name, char *newpath ) {
		stats_inc("parrot.local.chdir", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"canonicalize %s",name->rest);
		result = ::get_canonical_path(name->rest,newpath,PFS_PATH_MAX);
		END
	}

	virtual int link( pfs_name *oldname, pfs_name *newname ) {
		stats_inc("parrot.local.link", 1);
		int result;
		if(!pfs_acl_check(oldname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"link %s %s",oldname->rest,newname->rest);
		result = ::link(oldname->rest,newname->rest);
		END
	}
	virtual int symlink( const char *linkname, pfs_name *newname ) {
		stats_inc("parrot.local.symlink", 1);
		int result;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"symlink %s %s",linkname,newname->rest);
		result = ::symlink(linkname,newname->rest);
		END
	}
	virtual int readlink( pfs_name *name, char *buf, pfs_size_t size ) {
		stats_inc("parrot.local.readlink", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"readlink %s %p %d",name->rest,buf,(int)size);
		result = ::readlink(name->rest,buf,size);
		if (result >= 0) stats_bin("parrot.local.readlink.size", result);
		END
	}
	virtual int mknod( pfs_name *name, mode_t mode, dev_t dev ) {
		stats_inc("parrot.local.mknod", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"mknod %s %d %d",name->rest,(int)mode,(int)dev);
		result = ::mknod(name->rest,mode,dev);
		END
	}
	virtual int mkdir( pfs_name *name, mode_t mode ) {
		stats_inc("parrot.local.mkdir", 1);
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"mkdir %s %d",name->rest,mode);
		result = ::mkdir(name->rest,mode);
		if(result==0 && pfs_username) ibox_acl_init_copy(name->rest);
		END
	}
	virtual int rmdir( pfs_name *name ) {
		stats_inc("parrot.local.rmdir", 1);
		int result;
		if(!pfs_acl_check_dir(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"rmdir %s",name->rest);
		result = ::rmdir(name->rest);
		if(result == -1 && errno == ENOTEMPTY) {
			// If we failed to remove the directory because it contains
			// only an acl file, remove the acl and the directory.
			result = ibox_acl_rmdir(name->rest);
		}
		END
	}

	virtual int whoami( pfs_name *name, char *buf, int size ) {
		stats_inc("parrot.local.whoami", 1);
		int result;
		debug(D_LOCAL,"whoami %s",name->rest);
		if (pfs_username) {
		strncpy(buf, pfs_username, size);
		result = strlen(buf);
		} else {
		result = username_get(buf);
		result = strlen(buf);
		}
		END
	}

	virtual pfs_location* locate( pfs_name *name ) {
		stats_inc("parrot.local.locate", 1);
		int result;
		struct pfs_stat buf;
		char path[PFS_PATH_MAX];
		pfs_location *loc;
		if(!pfs_acl_check_dir(name,IBOX_ACL_LIST)) return 0;
		debug(D_LOCAL,"locate %s",name->rest);
		result = stat(name, &buf);
		if(result < 0) return 0;
		string_nformat(path, sizeof(path), "localhost:dev%" PRId64 ":%s", buf.st_dev, name->path);
		loc = new pfs_location();
		loc->append(path);
		return loc;
	}

	virtual int is_seekable() {
		return 1;
	}

	virtual int is_local() {
		return 1;
	}
};

static pfs_service_local pfs_service_local_instance;
pfs_service *pfs_service_local = &pfs_service_local_instance;

struct pfs_file * pfs_file_bootstrap( int fd, const char *path )
{
	pfs_name name;
	name.service = pfs_service_local;
	strcpy(name.path,path);
	strcpy(name.service_name,"local");
	name.host[0] = 0;
	name.port = 0;
	strcpy(name.rest,path);
	name.is_local = 1;
	return new pfs_file_local(&name,fd);
}

/* vim: set noexpandtab tabstop=4: */
