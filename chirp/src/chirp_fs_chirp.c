/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_chirp.h"
#include "chirp_protocol.h"
#include "chirp_reli.h"

#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#define CHIRP_FS_FD_MAX 1024

static int chirp_fs_chirp_timeout = 60;
static struct chirp_file * chirp_fs_file_table[ CHIRP_FS_FD_MAX ] = {0};
static const char * chirp_fs_chirp_hostport = 0;

#define SETUP_FILE \
if(fd<0 || fd>=CHIRP_FS_FD_MAX) { errno = EBADF; return -1; }\
struct chirp_file *file = chirp_fs_file_table[fd]; \
if(!fd) { errno = EBADF; return -1;}

#define STOPTIME (time(0)+chirp_fs_chirp_timeout)

static const char * chirp_fs_chirp_init( const char *url )
{
	char *h, *p;
	const char *path;

	/* find the first slash after chirp: */
	h = strchr(url,'/');

	/* skip over one or more slashes to get the hostname */
	while(*h=='/') h++;
	if(!h) return 0;

	/* now find the slash following the hostname */
	p = strchr(h,'/');
	if(p) {
		path = strdup(p);
	} else {
		path = "/";
	}

	chirp_fs_chirp_hostport = strdup(h);

	p = strchr(chirp_fs_chirp_hostport,'/');
	if(p) *p = 0;

	debug(D_CHIRP,"url: %s",url);

	return path;
}

static INT64_T chirp_fs_chirp_open(const char *path, INT64_T flags, INT64_T mode)
{
	int fd;

	for(fd=3;fd<CHIRP_FS_FD_MAX;fd++) {
		if(!chirp_fs_file_table[fd]) break;
	}

	if(fd==CHIRP_FS_FD_MAX) {
		errno = EMFILE;
		return -1;
	}

	struct chirp_file *file = chirp_reli_open(chirp_fs_chirp_hostport,path,flags,mode,STOPTIME);

	chirp_fs_file_table[fd] = file;

	return fd;
}

static INT64_T chirp_fs_chirp_close(int fd)
{
	SETUP_FILE
	chirp_fs_file_table[fd] = 0;
	return chirp_reli_close(file,STOPTIME);
}

static INT64_T chirp_fs_chirp_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pread(file,buffer,length,offset,STOPTIME);
}

static INT64_T chirp_fs_chirp_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_sread(file,vbuffer,length,stride_length,stride_skip,offset,STOPTIME);
}

static INT64_T chirp_fs_chirp_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pwrite(file,buffer,length,offset,STOPTIME);
}

static INT64_T chirp_fs_chirp_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_swrite(file,vbuffer,length,stride_length,stride_skip,offset,STOPTIME);
}

static INT64_T chirp_fs_chirp_fstat(int fd, struct chirp_stat * buf)
{
	SETUP_FILE
	return chirp_reli_fstat(file,buf,STOPTIME);
}

static INT64_T chirp_fs_chirp_fstatfs(int fd, struct chirp_statfs * buf)
{
	SETUP_FILE
	return chirp_reli_fstatfs(file,buf,STOPTIME);
}

static INT64_T chirp_fs_chirp_fchown(int fd, INT64_T uid, INT64_T gid)
{
	SETUP_FILE
	return chirp_reli_fchown(file,uid,gid,STOPTIME);
}

static INT64_T chirp_fs_chirp_fchmod(int fd, INT64_T mode)
{
	SETUP_FILE
	return chirp_reli_fchmod(file,mode,STOPTIME);
}

static INT64_T chirp_fs_chirp_ftruncate(int fd, INT64_T length)
{
	SETUP_FILE
	return chirp_reli_ftruncate(file,length,STOPTIME);
}

static INT64_T chirp_fs_chirp_fsync(int fd)
{
	SETUP_FILE
	return chirp_reli_fsync(file,STOPTIME);
}

static struct chirp_dir * chirp_fs_chirp_opendir(const char *path)
{
	return chirp_reli_opendir(chirp_fs_chirp_hostport,path,STOPTIME);
}

static struct chirp_dirent * chirp_fs_chirp_readdir( struct chirp_dir *dir )
{	
	return chirp_reli_readdir(dir);
}

static void chirp_fs_chirp_closedir( struct chirp_dir *dir )
{
	chirp_reli_closedir(dir);
}

static INT64_T chirp_fs_chirp_getfile(const char *path, struct link *link, time_t stoptime)
{
	/* ignore for now */
	errno = ENOSYS;
	return -1;
}

static INT64_T chirp_fs_chirp_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime)
{
	/* ignore for now */
	errno = ENOSYS;
	return -1;
}

static INT64_T chirp_fs_chirp_unlink(const char *path)
{
	return chirp_reli_unlink(chirp_fs_chirp_hostport,path,STOPTIME);
}

static INT64_T chirp_fs_chirp_rmall(const char *path)
{
	return chirp_reli_rmall(chirp_fs_chirp_hostport,path,STOPTIME);
}

static INT64_T chirp_fs_chirp_rename(const char *path, const char *newpath)
{
	return chirp_reli_rename(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

static INT64_T chirp_fs_chirp_link(const char *path, const char *newpath)
{
	return chirp_reli_link(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

static INT64_T chirp_fs_chirp_symlink(const char *path, const char *newpath)
{
	return chirp_reli_symlink(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

static INT64_T chirp_fs_chirp_readlink(const char *path, char *buf, INT64_T length)
{
	return chirp_reli_readlink(chirp_fs_chirp_hostport,path,buf,length,STOPTIME);
}

static INT64_T chirp_fs_chirp_chdir(const char *path)
{
	return 0;
}

static INT64_T chirp_fs_chirp_mkdir(const char *path, INT64_T mode)
{
	return chirp_reli_mkdir(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

static INT64_T chirp_fs_chirp_rmdir(const char *path)
{
	return chirp_reli_rmdir(chirp_fs_chirp_hostport,path,STOPTIME);
}

static INT64_T chirp_fs_chirp_stat(const char *path, struct chirp_stat * buf)
{
	return chirp_reli_stat(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

static INT64_T chirp_fs_chirp_lstat(const char *path, struct chirp_stat * buf)
{
	return chirp_reli_lstat(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

static INT64_T chirp_fs_chirp_statfs(const char *path, struct chirp_statfs * buf)
{
	return chirp_reli_statfs(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

static INT64_T chirp_fs_chirp_access(const char *path, INT64_T mode)
{
	return chirp_reli_access(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

static INT64_T chirp_fs_chirp_chmod(const char *path, INT64_T mode)
{
	return chirp_reli_chmod(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

static INT64_T chirp_fs_chirp_chown(const char *path, INT64_T uid, INT64_T gid)
{
	return chirp_reli_chown(chirp_fs_chirp_hostport,path,uid,gid,STOPTIME);
}

static INT64_T chirp_fs_chirp_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	return chirp_reli_lchown(chirp_fs_chirp_hostport,path,uid,gid,STOPTIME);
}

static INT64_T chirp_fs_chirp_truncate(const char *path, INT64_T length)
{
	return chirp_reli_truncate(chirp_fs_chirp_hostport,path,length,STOPTIME);
}

static INT64_T chirp_fs_chirp_utime(const char *path, time_t actime, time_t modtime)
{
	return chirp_reli_utime(chirp_fs_chirp_hostport,path,actime,modtime,STOPTIME);
}

static INT64_T chirp_fs_chirp_md5(const char *path, unsigned char digest[16])
{
	return chirp_reli_md5(chirp_fs_chirp_hostport,path,digest,STOPTIME);
}

static INT64_T chirp_fs_chirp_setrep( const char *path, int nreps )
{
	return chirp_reli_setrep(chirp_fs_chirp_hostport,path,nreps,STOPTIME);
}

static INT64_T chirp_fs_chirp_getxattr ( const char *path, const char *name, void *data, size_t size )
{
	return chirp_reli_getxattr(chirp_fs_chirp_hostport, path, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_fgetxattr ( int fd, const char *name, void *data, size_t size )
{
	SETUP_FILE
	return chirp_reli_fgetxattr(file, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_lgetxattr ( const char *path, const char *name, void *data, size_t size )
{
	return chirp_reli_lgetxattr(chirp_fs_chirp_hostport, path, name, data, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_listxattr ( const char *path, char *list, size_t size )
{
	return chirp_reli_listxattr(chirp_fs_chirp_hostport, path, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_flistxattr ( int fd, char *list, size_t size )
{
	SETUP_FILE
	return chirp_reli_flistxattr(file, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_llistxattr ( const char *path, char *list, size_t size )
{
	return chirp_reli_llistxattr(chirp_fs_chirp_hostport, path, list, size, STOPTIME);
}

static INT64_T chirp_fs_chirp_setxattr ( const char *path, const char *name, const void *data, size_t size, int flags )
{
	return chirp_reli_setxattr(chirp_fs_chirp_hostport, path, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_fsetxattr ( int fd, const char *name, const void *data, size_t size, int flags )
{
	SETUP_FILE
	return chirp_reli_fsetxattr(file, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_lsetxattr ( const char *path, const char *name, const void *data, size_t size, int flags )
{
	return chirp_reli_lsetxattr(chirp_fs_chirp_hostport, path, name, data, size, flags, STOPTIME);
}

static INT64_T chirp_fs_chirp_removexattr ( const char *path, const char *name )
{
	return chirp_reli_removexattr(chirp_fs_chirp_hostport, path, name, STOPTIME);
}

static INT64_T chirp_fs_chirp_fremovexattr ( int fd, const char *name )
{
	SETUP_FILE
	return chirp_reli_fremovexattr(file, name, STOPTIME);
}

static INT64_T chirp_fs_chirp_lremovexattr ( const char *path, const char *name )
{
	return chirp_reli_lremovexattr(chirp_fs_chirp_hostport, path, name, STOPTIME);
}

static int chirp_fs_chirp_do_acl_check()
{
	return 0;
}

struct chirp_filesystem chirp_fs_chirp = {
	chirp_fs_chirp_init,

	chirp_fs_chirp_open,
	chirp_fs_chirp_close,
	chirp_fs_chirp_pread,
	chirp_fs_chirp_pwrite,
	chirp_fs_chirp_sread,
	chirp_fs_chirp_swrite,
	chirp_fs_chirp_fstat,
	chirp_fs_chirp_fstatfs,
	chirp_fs_chirp_fchown,
	chirp_fs_chirp_fchmod,
	chirp_fs_chirp_ftruncate,
	chirp_fs_chirp_fsync,

	chirp_fs_chirp_opendir,
	chirp_fs_chirp_readdir,
	chirp_fs_chirp_closedir,

	chirp_fs_chirp_getfile,
	chirp_fs_chirp_putfile,

	chirp_fs_chirp_unlink,
	chirp_fs_chirp_rmall,
	chirp_fs_chirp_rename,
	chirp_fs_chirp_link,
	chirp_fs_chirp_symlink,
	chirp_fs_chirp_readlink,
	chirp_fs_chirp_chdir,
	chirp_fs_chirp_mkdir,
	chirp_fs_chirp_rmdir,
	chirp_fs_chirp_stat,
	chirp_fs_chirp_lstat,
	chirp_fs_chirp_statfs,
	chirp_fs_chirp_access,
	chirp_fs_chirp_chmod,
	chirp_fs_chirp_chown,
	chirp_fs_chirp_lchown,
	chirp_fs_chirp_truncate,
	chirp_fs_chirp_utime,
	chirp_fs_chirp_md5,
	chirp_fs_chirp_setrep,

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	chirp_fs_chirp_getxattr,
	chirp_fs_chirp_fgetxattr,
	chirp_fs_chirp_lgetxattr,
	chirp_fs_chirp_listxattr,
	chirp_fs_chirp_flistxattr,
	chirp_fs_chirp_llistxattr,
	chirp_fs_chirp_setxattr,
	chirp_fs_chirp_fsetxattr,
	chirp_fs_chirp_lsetxattr,
	chirp_fs_chirp_removexattr,
	chirp_fs_chirp_fremovexattr,
	chirp_fs_chirp_lremovexattr,
#else
	cfs_stub_getxattr,
	cfs_stub_fgetxattr,
	cfs_stub_lgetxattr,
	cfs_stub_listxattr,
	cfs_stub_flistxattr,
	cfs_stub_llistxattr,
	cfs_stub_setxattr,
	cfs_stub_fsetxattr,
	cfs_stub_lsetxattr,
	cfs_stub_removexattr,
	cfs_stub_fremovexattr,
	cfs_stub_lremovexattr,
#endif

	chirp_fs_chirp_do_acl_check
};
