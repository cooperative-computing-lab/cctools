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

const char * chirp_fs_chirp_init( const char *url )
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

INT64_T chirp_fs_chirp_open(const char *path, INT64_T flags, INT64_T mode)
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

INT64_T chirp_fs_chirp_close(int fd)
{
	SETUP_FILE
	chirp_fs_file_table[fd] = 0;
	return chirp_reli_close(file,STOPTIME);
}

INT64_T chirp_fs_chirp_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pread(file,buffer,length,offset,STOPTIME);
}

INT64_T chirp_fs_chirp_sread(int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_sread(file,vbuffer,length,stride_length,stride_skip,offset,STOPTIME);
}

INT64_T chirp_fs_chirp_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_pwrite(file,buffer,length,offset,STOPTIME);
}

INT64_T chirp_fs_chirp_swrite(int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset)
{
	SETUP_FILE
	return chirp_reli_swrite(file,vbuffer,length,stride_length,stride_skip,offset,STOPTIME);
}

INT64_T chirp_fs_chirp_fstat(int fd, struct chirp_stat * buf)
{
	SETUP_FILE
	return chirp_reli_fstat(file,buf,STOPTIME);
}

INT64_T chirp_fs_chirp_fstatfs(int fd, struct chirp_statfs * buf)
{
	SETUP_FILE
	return chirp_reli_fstatfs(file,buf,STOPTIME);
}

INT64_T chirp_fs_chirp_fchown(int fd, INT64_T uid, INT64_T gid)
{
	SETUP_FILE
	return chirp_reli_fchown(file,uid,gid,STOPTIME);
}

INT64_T chirp_fs_chirp_fchmod(int fd, INT64_T mode)
{
	SETUP_FILE
	return chirp_reli_fchmod(file,mode,STOPTIME);
}

INT64_T chirp_fs_chirp_ftruncate(int fd, INT64_T length)
{
	SETUP_FILE
	return chirp_reli_ftruncate(file,length,STOPTIME);
}

INT64_T chirp_fs_chirp_fsync(int fd)
{
	SETUP_FILE
	return chirp_reli_fsync(file,STOPTIME);
}

void *chirp_fs_chirp_opendir(const char *path)
{
	return chirp_reli_opendir(chirp_fs_chirp_hostport,path,STOPTIME);
}

char *chirp_fs_chirp_readdir(void *dir)
{	
	struct chirp_dirent *d;
	d = chirp_reli_readdir(dir);
	if(d) {
		return d->name;
	} else {
		return 0;
	}
}

void chirp_fs_chirp_closedir(void *dir)
{
	chirp_reli_closedir(dir);
}

INT64_T chirp_fs_chirp_getfile(const char *path, struct link *link, time_t stoptime)
{
	/* ignore for now */
	errno = ENOSYS;
	return -1;
}

INT64_T chirp_fs_chirp_putfile(const char *path, struct link * link, INT64_T length, INT64_T mode, time_t stoptime)
{
	/* ignore for now */
	errno = ENOSYS;
	return -1;
}

INT64_T chirp_fs_chirp_unlink(const char *path)
{
	return chirp_reli_unlink(chirp_fs_chirp_hostport,path,STOPTIME);
}

INT64_T chirp_fs_chirp_rename(const char *path, const char *newpath)
{
	return chirp_reli_rename(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

INT64_T chirp_fs_chirp_link(const char *path, const char *newpath)
{
	return chirp_reli_link(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

INT64_T chirp_fs_chirp_symlink(const char *path, const char *newpath)
{
	return chirp_reli_symlink(chirp_fs_chirp_hostport,path,newpath,STOPTIME);
}

INT64_T chirp_fs_chirp_readlink(const char *path, char *buf, INT64_T length)
{
	return chirp_reli_readlink(chirp_fs_chirp_hostport,path,buf,length,STOPTIME);
}

INT64_T chirp_fs_chirp_chdir(const char *path)
{
	return 0;
}

INT64_T chirp_fs_chirp_mkdir(const char *path, INT64_T mode)
{
	return chirp_reli_mkdir(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

INT64_T chirp_fs_chirp_rmdir(const char *path)
{
	return chirp_reli_rmdir(chirp_fs_chirp_hostport,path,STOPTIME);
}

INT64_T chirp_fs_chirp_stat(const char *path, struct chirp_stat * buf)
{
	return chirp_reli_stat(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

INT64_T chirp_fs_chirp_lstat(const char *path, struct chirp_stat * buf)
{
	return chirp_reli_lstat(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

INT64_T chirp_fs_chirp_statfs(const char *path, struct chirp_statfs * buf)
{
	return chirp_reli_statfs(chirp_fs_chirp_hostport,path,buf,STOPTIME);
}

INT64_T chirp_fs_chirp_access(const char *path, INT64_T mode)
{
	return chirp_reli_access(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

INT64_T chirp_fs_chirp_chmod(const char *path, INT64_T mode)
{
	return chirp_reli_chmod(chirp_fs_chirp_hostport,path,mode,STOPTIME);
}

INT64_T chirp_fs_chirp_chown(const char *path, INT64_T uid, INT64_T gid)
{
	return chirp_reli_chown(chirp_fs_chirp_hostport,path,uid,gid,STOPTIME);
}

INT64_T chirp_fs_chirp_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	return chirp_reli_lchown(chirp_fs_chirp_hostport,path,uid,gid,STOPTIME);
}

INT64_T chirp_fs_chirp_truncate(const char *path, INT64_T length)
{
	return chirp_reli_truncate(chirp_fs_chirp_hostport,path,length,STOPTIME);
}

INT64_T chirp_fs_chirp_utime(const char *path, time_t actime, time_t modtime)
{
	return chirp_reli_utime(chirp_fs_chirp_hostport,path,actime,modtime,STOPTIME);
}

INT64_T chirp_fs_chirp_md5(const char *path, unsigned char digest[16])
{
	return chirp_reli_md5(chirp_fs_chirp_hostport,path,digest,STOPTIME);
}

INT64_T chirp_fs_chirp_file_size(const char *path)
{
	struct chirp_stat info;
	if(chirp_fs_chirp_stat(path, &info) == 0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T chirp_fs_chirp_fd_size(int fd)
{
	struct chirp_stat info;
	if(chirp_fs_chirp_fstat(fd, &info) == 0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

int chirp_fs_chirp_do_acl_check()
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

	chirp_fs_chirp_file_size,
	chirp_fs_chirp_fd_size,
	chirp_fs_chirp_do_acl_check
};
