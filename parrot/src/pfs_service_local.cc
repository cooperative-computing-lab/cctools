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
}

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/vfs.h>
#include <sys/file.h>
#include <sys/time.h>
#include <utime.h>
#include <dirent.h>
#include <sys/poll.h>

#define END debug(D_LOCAL,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno)) ); return result;

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
	int is_a_pipe;
	int is_a_named_pipe;
	pfs_off_t last_offset;

public:
	pfs_file_local( pfs_name *name, int f, int i ) : pfs_file(name) {
		fd = f;
		last_offset = 0;
		is_a_pipe = i;
		is_a_named_pipe = -1;
	}

	virtual int close() {
		int result;
		debug(D_LOCAL,"close %d",fd);
		result = ::close(fd);
		END
	}

/*
This is a strange situation.

When an application wants to open a named pipe,
Parrot must issue a non-blocking open so that
everything is not halted while the connection is made,
much like what we do with sockets.

However, POSIX has something interesting to say about
a non-blocking open on a named pipe: it must instantly
return zero, indicating success, even if there is no-one
connected on the other side!  But, if the appl issues a
read right away, the read returns zero immediately, because
the pipe is not connected.

We aren't able to solve this at the time of the open,
because there appears no mechanism within POSIX to do the
right thing.  Instead, we solve it during read().
If read returns zero on offset zero and the file descriptor
refers to a named pipe, then convert the result into -1
EAGAIN, which forces higher layers of Parrot to put the
process to sleep and wait for actual input to become ready.
*/

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;

		debug(D_LOCAL,"read %d 0x%x %lld %lld",fd,data,length,offset);

		if(offset!=last_offset) ::lseek64(fd,offset,SEEK_SET);
		result = ::read(fd,data,length);
		if(result>0) last_offset = offset+result;

		if(result==0 && offset==0) {
			if(is_a_named_pipe==-1) {
				if(is_a_pipe==0) {
					struct stat64 info;
					::fstat64(fd,&info);
					if(S_ISFIFO(info.st_mode)) {
						is_a_named_pipe = 1;
					} else {
						is_a_named_pipe = 0;
					}
				} else {
					is_a_named_pipe = 0;
				}	
			}
			if(is_a_named_pipe) {
				result = -1;
				errno = EAGAIN;
			}
		}
		END
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;
		debug(D_LOCAL,"write %d 0x%x %lld %lld",fd,data,length,offset);
		if(offset!=last_offset) ::lseek64(fd,offset,SEEK_SET);
		result = ::write(fd,data,length);
		if(result>0) last_offset = offset+result;
		END
	}

	virtual int fstat( struct pfs_stat *buf ) {
		int result;
		struct stat64 lbuf;
		debug(D_LOCAL,"fstat %d 0x%x",fd,buf);
		result = ::fstat64(fd,&lbuf);
		if(result>=0) COPY_STAT(lbuf,*buf);
		END
	}

	virtual int fstatfs( struct pfs_statfs *buf ) {
		int result;
		struct statfs64 lbuf;
		debug(D_LOCAL,"fstatfs %d 0x%x",fd,buf);
		result = ::fstatfs64(fd,&lbuf);
		if(result>=0) COPY_STATFS(lbuf,*buf);
		END
	}

	virtual int ftruncate( pfs_size_t length ) {
		int result;
		debug(D_LOCAL,"truncate %d %lld",fd,length);
		result = ::ftruncate64(fd,length);
		END
	}

	virtual int fsync() {
		int result;
		debug(D_LOCAL,"fsync %d",fd);
		result = ::fsync(fd);
		END
	}

	virtual int fcntl( int cmd, void *arg ) {
		int result;
		debug(D_LOCAL,"fcntl %d %d 0x%x",fd,cmd,arg);
		if(cmd==F_SETFL) arg = (void*)(((PTRINT_T)arg)|O_NONBLOCK);
#if defined(CCTOOLS_OPSYS_LINUX) && defined(CCTOOLS_CPU_X86_64)
		if (cmd == PFS_GETLK64) cmd = F_GETLK;
		if (cmd == PFS_SETLK64) cmd = F_SETLK;
		if (cmd == PFS_SETLKW64) cmd = F_SETLKW;
#endif
		result = ::fcntl(fd,cmd,arg);
		END
	}

	virtual int ioctl( int cmd, void *arg ) {
		int result;
		debug(D_LOCAL,"ioctl %d 0x%x 0x%x",fd,cmd,arg);
		result = ::ioctl(fd,cmd,arg);
		END
	}

	virtual int fchmod( mode_t mode ) {
		int result;
		debug(D_LOCAL,"fchmod %d %d",fd,mode);
		result = ::fchmod(fd,mode);
		END
	}

	virtual int fchown( uid_t uid, gid_t gid ) {
		int result;
		debug(D_LOCAL,"fchown %d %d %d",fd,uid,gid);
		result = ::fchown(fd,uid,gid);
		END
	}

	virtual ssize_t fgetxattr( const char *name, void *data, size_t size ) {
		ssize_t result;
		debug(D_LOCAL,"fgetxattr %d %s",fd,name);
		result = ::fgetxattr(fd,name,data,size);
		END
	}

	virtual ssize_t flistxattr( char *list, size_t size ) {
		ssize_t result;
		debug(D_LOCAL,"flistxattr %d",fd);
		result = ::flistxattr(fd,list,size);
		END
	}

	virtual int fsetxattr( const char *name, const void *data, size_t size, int flags ) {
		int result;
		debug(D_LOCAL,"fsetxattr %d %s <> %d",fd,name,flags);
		result = ::fsetxattr(fd,name,data,size,flags);
		END
	}

	virtual int fremovexattr( const char *name ) {
		int result;
		debug(D_LOCAL,"fremovexattr %d %s",fd,name);
		result = ::fremovexattr(fd,name);
		END
	}

	virtual int flock( int op ) {
		int result;
		debug(D_LOCAL,"flock %d %d",fd,op);
		result = ::flock(fd,op);
		END
	}

	virtual void * mmap( void *start, pfs_size_t length, int prot, int flags, off_t offset ) {
		void *result;
		result = ::mmap((caddr_t)start,length,prot,flags,fd,offset);
		debug(D_LOCAL,"= %d %s",(PTRINT_T)result,(((PTRINT_T)result>=0) ? "" : strerror(errno)) );
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
		return !is_a_pipe;
	}
	
	virtual void poll_register( int which ) {
		pfs_poll_wakeon(fd,which);
	}

	virtual int poll_ready() {
		struct pollfd pfd;
		int result=0, flags =0;

		pfd.fd = fd;
		pfd.events = POLLIN|POLLOUT|POLLERR|POLLHUP|POLLPRI;
		pfd.revents = 0;

		result = ::poll(&pfd,1,0);
		if(result>0) {
			if(pfd.revents&POLLIN) flags |= PFS_POLL_READ;
			if(pfd.revents&POLLHUP) flags |= PFS_POLL_READ;
			if(pfd.revents&POLLOUT) flags |= PFS_POLL_WRITE;
			if(pfd.revents&POLLERR) flags |= PFS_POLL_READ;
			if(pfd.revents&POLLERR) flags |= PFS_POLL_WRITE;
			if(pfd.revents&POLLPRI) flags |= PFS_POLL_EXCEPT;
		}
		return flags;
	}
};

class pfs_service_local : public pfs_service {
public:
	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		pfs_file *result;

		if(!pfs_acl_check(name,ibox_acl_from_open_flags(flags))) return 0;

		flags |= O_NONBLOCK;
		debug(D_LOCAL,"open %s %d %d",name->rest,flags,(flags&O_CREAT) ? mode : 0);
		int fd = ::open64(name->rest,flags,mode);
		if(fd>=0) {
			result = new pfs_file_local(name,fd,0);
		} else {
			result = 0;
		}
		debug(D_LOCAL,"= %d %s",fd,(fd>=0) ? "" : strerror(errno));
		return result;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		struct dirent *d;
		DIR *dir;
		pfs_dir *result = 0;

		if(!pfs_acl_check_dir(name,IBOX_ACL_LIST)) return 0;

		debug(D_LOCAL,"getdir %s",name->rest);
		dir = ::opendir(name->rest);
		if(dir) {
			result = new pfs_dir(name);
			while((d=::readdir(dir))) {
				if(!strcmp(d->d_name,IBOX_ACL_BASE_NAME)) continue;
				result->append(d->d_name);
			}
			if(!strcmp(name->rest,"/")) {
				result->append("chirp");
			}
			closedir(dir);
		} else {
			result = 0;
       		}
		debug(D_LOCAL,"= %s",result ? "Success" : strerror(errno));
		return result;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"stat %s 0x%x",name->rest,buf);
		result = ::stat64(name->rest,&lbuf);
		if(result>=0) COPY_STAT(lbuf,*buf);
		END
	}
	virtual int statfs( pfs_name *name, struct pfs_statfs *buf ) {
		int result;
		struct statfs64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"statfs %s 0x%x",name->rest,buf);
		result = ::statfs64(name->rest,&lbuf);
		if(result>=0) COPY_STATFS(lbuf,*buf);
		END
	}
	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		struct stat64 lbuf;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		debug(D_LOCAL,"lstat %s 0x%x",name->rest,buf);
		result = ::lstat64(name->rest,&lbuf);
		if(result>=0) COPY_STAT(lbuf,*buf);
		END
	}
	virtual int access( pfs_name *name, mode_t mode ) {
		int result;
		if(!pfs_acl_check(name,ibox_acl_from_access_flags(mode))) return -1;
		debug(D_LOCAL,"access %s %d",name->rest,mode);
		result = ::access(name->rest,mode);
		END
	}
	virtual int chmod( pfs_name *name, mode_t mode ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"chmod %s %d",name->rest,mode);
		result = ::chmod(name->rest,mode);
		END
	}
	virtual int chown( pfs_name *name, uid_t uid, gid_t gid ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"chown %s %d %d",name->rest,uid,gid);
		result = ::chown(name->rest,uid,gid);
		END
	}
	virtual int lchown( pfs_name *name, uid_t uid, gid_t gid ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lchown %s %d %d",name->rest,uid,gid);
		result = ::lchown(name->rest,uid,gid);
		END
	}
	virtual int truncate( pfs_name *name, pfs_off_t length ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"truncate %s %lld",name->rest,length);
		result = ::truncate64(name->rest,length);
		END
	}
	virtual int utime( pfs_name *name, struct utimbuf *buf ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"utime %s 0x%x",name->rest,buf);
		result = ::utime(name->rest,buf);
		END
	}
	virtual int unlink( pfs_name *name ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"unlink %s",name->rest);
		result = ::unlink(name->rest);
		END
	}
	virtual int rename( pfs_name *oldname, pfs_name *newname ) {
		int result;
		if(!pfs_acl_check(oldname,IBOX_ACL_READ)) return -1;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"rename %s %s",oldname->rest,newname->rest);
		result = ::rename(oldname->rest,newname->rest);
		END
	}

	virtual ssize_t getxattr ( pfs_name *name, const char *attrname, void *value, size_t size )
	{
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"getxattr %s %s",name->rest,attrname);
		result = ::getxattr(name->rest,attrname,value,size);
		END
	}
	
	virtual ssize_t lgetxattr ( pfs_name *name, const char *attrname, void *value, size_t size )
	{
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"lgetxattr %s %s",name->rest,attrname);
		result = ::lgetxattr(name->rest,attrname,value,size);
		END
	}
	
	virtual ssize_t listxattr ( pfs_name *name, char *attrlist, size_t size )
	{
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"listxattr %s",name->rest);
		result = ::listxattr(name->rest,attrlist,size);
		END
	}
	
	virtual ssize_t llistxattr ( pfs_name *name, char *attrlist, size_t size )
	{
		ssize_t result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"llistxattr %s",name->rest);
		result = ::llistxattr(name->rest,attrlist,size);
		END
	}
	
	virtual int setxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags )
	{
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"setxattr %s %s <> %d",name->rest,attrname,flags);
		result = ::setxattr(name->rest,attrname,value,size,flags);
		END
	}
	
	virtual int lsetxattr ( pfs_name *name, const char *attrname, const void *value, size_t size, int flags )
	{
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lsetxattr %s %s <> %d",name->rest,attrname,flags);
		result = ::lsetxattr(name->rest,attrname,value,size,flags);
		END
	}
	
	virtual int removexattr ( pfs_name *name, const char *attrname )
	{
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"removexattr %s %s",name->rest,attrname);
		result = ::removexattr(name->rest,attrname);
		END
	}
	
	virtual int lremovexattr ( pfs_name *name, const char *attrname )
	{
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_LIST)) return -1;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"lremovexattr %s %s",name->rest,attrname);
		result = ::lremovexattr(name->rest,attrname);
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
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"canonicalize %s",name->rest);
		result = ::get_canonical_path(name->rest,newpath,PFS_PATH_MAX);
		END
	}

	virtual int link( pfs_name *oldname, pfs_name *newname ) {
		int result;
		if(!pfs_acl_check(oldname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"link %s %s",oldname->rest,newname->rest);
		result = ::link(oldname->rest,newname->rest);
		END
	}
	virtual int symlink( const char *linkname, pfs_name *newname ) {
		int result;
		if(!pfs_acl_check(newname,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"symlink %s %s",linkname,newname->rest);
		result = ::symlink(linkname,newname->rest);
		END
	}
	virtual int readlink( pfs_name *name, char *buf, pfs_size_t size ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_READ)) return -1;
		debug(D_LOCAL,"readlink %s 0x%x %d",name->rest,buf,size);
		result = ::readlink(name->rest,buf,size);
		END
	}
	virtual int mknod( pfs_name *name, mode_t mode, dev_t dev ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"mknod %s %d %d",name->rest,mode,dev);
		result = ::mknod(name->rest,mode,dev);
		END
	}
	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result;
		if(!pfs_acl_check(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"mkdir %s %d",name->rest,mode);
		result = ::mkdir(name->rest,mode);
		if(result==0 && pfs_username) ibox_acl_init_copy(name->rest);
		END
	}
	virtual int rmdir( pfs_name *name ) {
		int result;
		if(!pfs_acl_check_dir(name,IBOX_ACL_WRITE)) return -1;
		debug(D_LOCAL,"rmdir %s",name->rest);
		result = ::rmdir(name->rest);
		END
	}

	virtual int whoami( pfs_name *name, char *buf, int size ) {
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
		int result;
		struct pfs_stat buf;
		char path[PFS_PATH_MAX];
		pfs_location *loc;
		if(!pfs_acl_check_dir(name,IBOX_ACL_LIST)) return 0;
		debug(D_LOCAL,"locate %s",name->rest);
		result = stat(name, &buf);
		if(result < 0) return 0;
		snprintf(path, PFS_PATH_MAX, "localhost:dev%lld:%s", buf.st_dev, name->path);
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
	return new pfs_file_local(&name,fd,1);
}
