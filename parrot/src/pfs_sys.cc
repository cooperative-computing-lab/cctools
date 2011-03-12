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
}

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/un.h>

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
	retry:\

#define END \
	debug(D_LIBCALL,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno)) ); \
	if(result<0 && errno==EINTR) goto retry;\
	if(result<0 && errno==0) { debug(D_DEBUG,"whoops, converting errno=0 to ENOENT"); errno = ENOENT; }\
	return result;

int pfs_open( const char *path, int flags, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(path,flags,mode,pfs_force_cache);
	END
}

int pfs_open_cached( const char *path, int flags, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"open %s %u %u",path,flags,mode);
	result = pfs_current->table->open(path,flags,mode,1);
	END
}

int pfs_pipe( int *fds )
{
	BEGIN
	debug(D_LIBCALL,"pipe");
	result = pfs_current->table->pipe(fds);
	debug(D_LIBCALL,"= %d [%d,%d] %s",(int)result,fds[0],fds[1],((result>=0) ? "" : strerror(errno)) );
	if(result<0 && errno==EINTR) goto retry;
	if(result<0 && errno==0) errno = ENOENT;
	return result;
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
	debug(D_LIBCALL,"read %d 0x%x %lld",fd,data,length);
	result = pfs_current->table->read(fd,data,length);
	END
}

pfs_ssize_t pfs_write( int fd, const void *data, pfs_size_t length )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"write %d 0x%x %lld",fd,data,length);
	result = pfs_current->table->write(fd,data,length);
	END
}

pfs_ssize_t pfs_pread( int fd, void *data, pfs_size_t length, pfs_off_t offset )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"pread %d 0x%x %lld",fd,data,length);
	result = pfs_current->table->pread(fd,data,length,offset);
	END
}

pfs_ssize_t pfs_pwrite( int fd, const void *data, pfs_size_t length, pfs_off_t offset )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"pwrite %d 0x%x %lld",fd,data,length);
	result = pfs_current->table->pwrite(fd,data,length,offset);
	END
}

pfs_ssize_t pfs_readv( int fd, const struct iovec *vector, int count )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"readv %d 0x%x %d",fd,vector,count);
	result = pfs_current->table->readv(fd,vector,count);
	END
}

pfs_ssize_t pfs_writev( int fd, const struct iovec *vector, int count )
{
	pfs_ssize_t result;
	retry:
	debug(D_LIBCALL,"writev %d 0x%x %d",fd,vector,count);
	result = pfs_current->table->writev(fd,vector,count);
	END
}

pfs_off_t pfs_lseek( int fd, pfs_off_t offset, int whence )
{
	pfs_off_t result;
	retry:
	debug(D_LIBCALL,"lseek %d %lld %d",fd,offset,whence);
	result = pfs_current->table->lseek(fd,offset,whence);
	END
}

int pfs_ftruncate( int fd, pfs_off_t length )
{
	BEGIN
	debug(D_LIBCALL,"ftruncate %d %lld",fd,length);
	result = pfs_current->table->ftruncate(fd,length);
	END
}

int pfs_fstat( int fd, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"fstat %d %u",fd,buf);
	result = pfs_current->table->fstat(fd,buf);
	END
}

int pfs_fstatfs( int fd, struct pfs_statfs *buf )
{
	BEGIN
	debug(D_LIBCALL,"fstatfs %d 0x%x",fd,buf);
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
	debug(D_LIBCALL,"fcntl %d %d 0x%x",fd,cmd,arg);
	result = pfs_current->table->fcntl(fd,cmd,arg);
	END
}

int pfs_ioctl( int fd, int cmd, void *arg )
{
	BEGIN
	debug(D_LIBCALL,"ioctl %d 0x%x 0x%x",fd,cmd,arg);
	result = pfs_current->table->ioctl(fd,cmd,arg);
	END
}

int pfs_fchmod( int fd, mode_t mode )
{
	BEGIN
	result = pfs_current->table->fchmod(fd,mode);
	debug(D_LIBCALL,"fchmod %d %d",fd,mode);
	END
}

int pfs_fchown( int fd, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"fchown %d %d %d",fd,uid,gid);
	result = pfs_current->table->fchown(fd,uid,gid);
	END
}

int pfs_flock( int fd, int op )
{
	BEGIN
	debug(D_LIBCALL,"flock %d %d",fd,op);
	result = pfs_current->table->flock(fd,op);
	END
}

int pfs_select( int n, fd_set *rfds, fd_set *wfds, fd_set *efds, struct timeval *timeout )
{
	BEGIN
	debug(D_LIBCALL,"select %d 0x%x 0x%x 0x%x 0x%x",n,rfds,wfds,efds,timeout);
	result = pfs_current->table->select(n,rfds,wfds,efds,timeout);
	END
}

int pfs_poll( struct pollfd *ufds, unsigned nfds, int timeout )
{
	BEGIN
	debug(D_LIBCALL,"poll 0x%x %d %d",ufds,nfds,timeout);
	result = pfs_current->table->poll(ufds,nfds,timeout);
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
	debug(D_LIBCALL,"getcwd 0x%x %d",path,size);
	result = pfs_current->table->getcwd(path,size);
	debug(D_LIBCALL,"= %s",result ? result : "(null)");
	return result;
}

int pfs_dup( int old )
{
	BEGIN
	debug(D_LIBCALL,"dup %d",old);
	result = pfs_current->table->dup(old);
	END
}

int pfs_dup2( int old, int nfd )
{
	BEGIN
	debug(D_LIBCALL,"dup2 %d %d",old,nfd);
	result = pfs_current->table->dup2(old,nfd);
	END
}

int pfs_stat( const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"stat %s 0x%x",path,buf);
	result = pfs_current->table->stat(path,buf);
	END
}

int pfs_statfs( const char *path, struct pfs_statfs *buf )
{
	BEGIN
	debug(D_LIBCALL,"statfs %s 0x%x",path,buf);
	result = pfs_current->table->statfs(path,buf);
	END
}

int pfs_lstat( const char *path, struct pfs_stat *buf )
{
	BEGIN
	debug(D_LIBCALL,"lstat %s 0x%x",path,buf);
	result = pfs_current->table->lstat(path,buf);
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
	debug(D_LIBCALL,"chmod %s 0x%x",path,mode);
	result = pfs_current->table->chmod(path,mode);
	END
}

int pfs_chown( const char *path, uid_t uid, gid_t gid )
{
	BEGIN
	debug(D_LIBCALL,"chown %s %d %d",path,uid,gid);
	result = pfs_current->table->chown(path,uid,gid);
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
	debug(D_LIBCALL,"truncate %s %lld",path,length);
	result = pfs_current->table->truncate(path,length);
	END
}

int pfs_utime( const char *path, struct utimbuf *buf )
{
	BEGIN
	debug(D_LIBCALL,"utime %s 0x%x",path,buf);
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

int pfs_symlink( const char *oldpath, const char *newpath )
{
	BEGIN
	debug(D_LIBCALL,"symlink %s %s",oldpath,newpath);
	result = pfs_current->table->symlink(oldpath,newpath);
	END
}

int pfs_readlink( const char *path, char *buf, pfs_size_t size )
{
	BEGIN
	debug(D_LIBCALL,"readlink %s 0x%x %lld",path,buf,size);
	result = pfs_current->table->readlink(path,buf,size);
	END
}

int pfs_mknod( const char *path, mode_t mode, dev_t dev )
{
	BEGIN
	debug(D_LIBCALL,"mknod %s %d %d",path,mode,dev);
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

int pfs_socket( int domain, int type, int protocol )
{
	BEGIN
	debug(D_LIBCALL,"socket %d %d %d",domain,type,protocol);
	result = pfs_current->table->socket(domain,type,protocol);
	END
}

int pfs_socketpair( int domain, int type, int proto, int *fds)
{
	BEGIN
	debug(D_LIBCALL,"socketpair %d %d %d",domain,type,proto);
	result = pfs_current->table->socketpair(domain,type,proto,fds);
	END
}

int pfs_accept( int fd, struct sockaddr *addr, int * addrlen )
{
	BEGIN
	debug(D_LIBCALL,"accept %d 0x%x 0x%x",fd,addr,addrlen);
	result = pfs_current->table->accept(fd,addr,addrlen);
	END
}

int pfs_bind( int fd, const struct sockaddr *addr, int addrlen )
{
	BEGIN
	debug(D_LIBCALL,"bind %d 0x%x %d",fd,addr,addrlen);
	result = ::bind(pfs_current->table->get_real_fd(fd),addr,addrlen);
	END
}

int pfs_connect( int fd, const struct sockaddr *addr, int addrlen )
{
	BEGIN
	debug(D_LIBCALL,"connect %d 0x%x %d",fd,addr,addrlen);
	result = ::connect(pfs_current->table->get_real_fd(fd),addr,addrlen);
	END
}

int pfs_getpeername( int fd, struct sockaddr *addr, int * addrlen )
{
	BEGIN
	debug(D_LIBCALL,"getpeername %d 0x%x 0x%x",fd,addr,addrlen);
	result = ::getpeername(pfs_current->table->get_real_fd(fd),addr,(socklen_t*)addrlen);
	END
}

int pfs_getsockname( int fd, struct sockaddr *addr, int * addrlen )
{
	BEGIN
	debug(D_LIBCALL,"getsockname %d 0x%x 0x%x",fd,addr,addrlen);
	result = ::getsockname(pfs_current->table->get_real_fd(fd),addr,(socklen_t*)addrlen);
	END
}

int pfs_getsockopt( int fd, int level, int option, void *value, int * length )
{
	BEGIN
	debug(D_LIBCALL,"getsockopt %d %d %d 0x%x 0x%x",fd,level,option,value,length);
	result = ::getsockopt(pfs_current->table->get_real_fd(fd),level,option,value,(socklen_t*)length);
	END
}

int pfs_listen( int fd, int backlog )
{
	BEGIN
	debug(D_LIBCALL,"listen %d %d",fd,backlog);
	result = ::listen(pfs_current->table->get_real_fd(fd),backlog);
	END
}

int pfs_recv( int fd, void *data, int length, int flags )
{
	BEGIN
	debug(D_LIBCALL,"recv %d 0x%x %d %d",fd,data,length,flags);
	result = ::recv(pfs_current->table->get_real_fd(fd),data,length,flags);
	END
}

int pfs_recvfrom( int fd, void *data, int length, int flags, struct sockaddr *addr, int * addrlength)
{
	BEGIN
	debug(D_LIBCALL,"recvfrom %d 0x%x %d %d 0x%x 0x%x",fd,data,length,flags,addr,addrlength);
	result = ::recvfrom(pfs_current->table->get_real_fd(fd),data,length,flags,addr,(socklen_t*)addrlength);
	END
}

int pfs_recvmsg( int fd,  struct msghdr *msg, int flags )
{
	BEGIN
	debug(D_LIBCALL,"recvmsg %d 0x%x %d",fd,msg,flags);

	result = ::recvmsg(pfs_current->table->get_real_fd(fd),msg,flags);

	/*
	One use of recvmsg is to pass file descriptors from one process
	to another via the use of 'ancillary data'.  If this happens, then
	Parrot will own the file descriptor, and must virtually attach it
	to the process with an anonymous name.
	*/

	if(result>=0 && msg->msg_controllen>0) {
		struct cmsghdr *cmsg = (struct cmsghdr *) msg->msg_control;
		if(cmsg->cmsg_level==SOL_SOCKET && cmsg->cmsg_type==SCM_RIGHTS) {
			int fd_count = (cmsg->cmsg_len-sizeof(struct cmsghdr)) / sizeof(int);
			int *fds = (int*) CMSG_DATA(cmsg);
			int i;
			for(i=0;i<fd_count;i++) {
				int lfd = pfs_current->table->find_empty(3);
				pfs_current->table->attach(lfd,fds[i],O_RDWR,0700,"anonymous-socket-fd");
				fds[i] = lfd;
				debug(D_SYSCALL,"recvmsg got anonymous file descriptor %d",lfd);
			}
		}
	}

	END
}

int pfs_send( int fd, const void *data, int length, int flags )
{
	BEGIN
	debug(D_LIBCALL,"send %d 0x%x %d %d",fd,data,length,flags);
	result = ::send(pfs_current->table->get_real_fd(fd),data,length,flags);
	END
}

int pfs_sendmsg( int fd, const struct msghdr *msg, int flags )
{
	BEGIN
	debug(D_LIBCALL,"sendmsg %d 0x%x %d",fd,msg,flags);
	result = ::sendmsg(pfs_current->table->get_real_fd(fd),msg,flags);
	END
}

int pfs_sendto( int fd, const void *data, int length, int flags, const struct sockaddr *addr, int addrlength )
{
	BEGIN
	debug(D_LIBCALL,"sendto %d 0x%x %d %d 0x%x %d",fd,data,length,flags,addr,addrlength);
	result = ::sendto(pfs_current->table->get_real_fd(fd),data,length,flags,addr,addrlength);
	END
}

int pfs_setsockopt( int fd, int level, int option, const void *value, int length )
{
	BEGIN
	debug(D_LIBCALL,"setsockopt %d %d %d 0x%x %d",fd,level,option,value,length);
	result = ::setsockopt(pfs_current->table->get_real_fd(fd),level,option,value,length);
	END
}

int pfs_shutdown( int fd, int how )
{
	BEGIN
	debug(D_LIBCALL,"shutdown %d %d",fd,how);
	result = ::shutdown(pfs_current->table->get_real_fd(fd),how);
	END
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

int pfs_mkalloc( const char *path, pfs_ssize_t size, mode_t mode )
{
	BEGIN
	debug(D_LIBCALL,"mkalloc %s %lld %d",path,size,mode);
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
	debug(D_LIBCALL,"whoami %s %x %d",path,buf,size);
	result = pfs_current->table->whoami(path,buf,size);
	END
}

int pfs_getacl( const char *path, char *buf, int size )
{
	BEGIN
	debug(D_LIBCALL,"getacl %s %x %d",path,buf,size);
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
	debug(D_LIBCALL, "pfs_locate %s %x %d", path, buf, size);
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

int pfs_get_local_name( const char *rpath, char *lpath, char *firstline, int length )
{
	int fd;
	int result;

	fd = pfs_open_cached(rpath,O_RDONLY,0);
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

int pfs_is_nonblocking( int fd )
{
	int flags = pfs_fcntl(fd,F_GETFL,0);
	if(flags<0) {
		return 0;
	} else if(flags&O_NONBLOCK) {
		return 1;
	} else {
		return 0;
	}
}

int pfs_resolve_name( const char *path, struct pfs_name *pname )
{
	return pfs_current->table->resolve_name(path,pname);
}








