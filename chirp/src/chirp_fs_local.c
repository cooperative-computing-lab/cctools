/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_protocol.h"

#include "create_dir.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "int_sizes.h"
#include "stringtools.h"
#include "full_io.h"
#include "delete_dir.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <utime.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif
#include <sys/param.h>
#include <sys/mount.h>

#if defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif
#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#endif
#ifndef ENOATTR
#define ENOATTR  EINVAL
#endif

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#define ftruncate64 ftruncate
#define truncate64 truncate
#define statfs64 statfs
#define fstatfs64 fstatfs
#endif

#if CCTOOLS_OPSYS_DARWIN
#define lchown chown
#endif

/* Solaris has statfs, but it doesn't work! Use statvfs instead. */

#if CCTOOLS_OPSYS_SUNOS
#define statfs statvfs
#define fstatfs fstatvfs
#define statfs64 statvfs64
#define fstatfs64 fstatvfs64
#endif

#ifndef COPY_CSTAT
#define COPY_CSTAT( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).cst_dev = (a).st_dev;\
	(b).cst_ino = (a).st_ino;\
	(b).cst_mode = (a).st_mode;\
	(b).cst_nlink = (a).st_nlink;\
	(b).cst_uid = (a).st_uid;\
	(b).cst_gid = (a).st_gid;\
	(b).cst_rdev = (a).st_rdev;\
	(b).cst_size = (a).st_size;\
	(b).cst_blksize = (a).st_blksize;\
	(b).cst_blocks = (a).st_blocks;\
	(b).cst_atime = (a).st_atime;\
	(b).cst_mtime = (a).st_mtime;\
	(b).cst_ctime = (a).st_ctime;
#endif

static INT64_T chirp_fs_local_stat(const char *path, struct chirp_stat * buf);
static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat * buf);

/*
The provided url could have any of the following forms:
local:/path, file:/path, or just /path.
*/

static const char * chirp_fs_local_init( const char *url )
{
	char *c = strchr(url,':');
	if(c) {
		return strdup(c+1);
	} else {
		return strdup(url);
	}
}

static INT64_T chirp_fs_local_open(const char *path, INT64_T flags, INT64_T mode)
{
	mode = 0600 | (mode & 0100);
	return open64(path, flags, (int) mode);
}

static INT64_T chirp_fs_local_close(int fd)
{
	return close(fd);
}

static INT64_T chirp_fs_local_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	INT64_T result;
	result = full_pread64(fd, buffer, length, offset);
	if(result < 0 && errno == ESPIPE) {
		/* if this is a pipe, return whatever amount is available */
		result = read(fd, buffer, length);
	}
	return result;
}

static INT64_T chirp_fs_local_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	INT64_T result;
	result = full_pwrite64(fd, buffer, length, offset);
	if(result < 0 && errno == ESPIPE) {
		/* if this is a pipe, then just write without the offset. */
		result = full_write(fd, buffer, length);
	}
	return result;
}

static INT64_T chirp_fs_local_fstat(int fd, struct chirp_stat * buf)
{
	struct stat64 info;
	int result;
	result = fstat64(fd, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	buf->cst_mode = buf->cst_mode & (~0077);
	return result;
}

static INT64_T chirp_fs_local_fstatfs(int fd, struct chirp_statfs * buf)
{
	struct statfs64 info;
	int result;
	result = fstatfs64(fd, &info);
	if(result == 0) {
		memset(buf, 0, sizeof(*buf));
#ifdef CCTOOLS_OPSYS_SUNOS
		buf->f_type = info.f_fsid;
		buf->f_bsize = info.f_frsize;
#else
		buf->f_type = info.f_type;
		buf->f_bsize = info.f_bsize;
#endif
		buf->f_blocks = info.f_blocks;
		buf->f_bavail = info.f_bavail;
		buf->f_bfree = info.f_bfree;
		buf->f_files = info.f_files;
		buf->f_ffree = info.f_ffree;
	}
	return result;
}

static INT64_T chirp_fs_local_fchown(int fd, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_local_fchmod(int fd, INT64_T mode)
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode = 0600 | (mode & 0177);
	return fchmod(fd, mode);
}

static INT64_T chirp_fs_local_ftruncate(int fd, INT64_T length)
{
	return ftruncate64(fd, length);
}

static INT64_T chirp_fs_local_fsync(int fd)
{
	return fsync(fd);
}

struct chirp_dir {
	char *path;
	DIR *dir;
};

static struct chirp_dir * chirp_fs_local_opendir( const char *path )
{
	DIR *dir = opendir(path);
	if(dir) {
		struct chirp_dir *cdir = malloc(sizeof(*cdir));
		cdir->dir = dir;
		cdir->path = strdup(path);
		return cdir;
	} else {
		return 0;
	}
}

static struct chirp_dirent * chirp_fs_local_readdir( struct chirp_dir *dir )
{
	struct dirent *d = readdir(dir->dir);
	if(d) {
		static struct chirp_dirent cd;
		char subpath[CHIRP_PATH_MAX];
		cd.name = d->d_name;
		sprintf(subpath,"%s/%s",dir->path,cd.name);
	       	chirp_fs_local_lstat(subpath,&cd.info);
		return &cd;
	} else {
		return 0;
	}
}

static void chirp_fs_local_closedir( struct chirp_dir *dir )
{
	free(dir->path);
	closedir(dir->dir);
	free(dir);
}

static INT64_T chirp_fs_local_unlink(const char *path)
{
	int result = unlink(path);

	/*
	   On Solaris, an unlink on a directory
	   returns EPERM when it should return EISDIR.
	   Check for this cast, and then fix it.
	 */

	if(result < 0 && errno == EPERM) {
		struct stat64 info;
		result = stat64(path, &info);
		if(result == 0 && S_ISDIR(info.st_mode)) {
			result = -1;
			errno = EISDIR;
		} else {
			result = -1;
			errno = EPERM;
		}
	}

	return result;
}

static INT64_T chirp_fs_local_rmall(const char *path)
{
	return cfs_delete_dir(path);
}

static INT64_T chirp_fs_local_rename(const char *path, const char *newpath)
{
	return rename(path, newpath);
}

static INT64_T chirp_fs_local_link(const char *path, const char *newpath)
{
	return link(path, newpath);
}

static INT64_T chirp_fs_local_symlink(const char *path, const char *newpath)
{
	return symlink(path, newpath);
}

static INT64_T chirp_fs_local_readlink(const char *path, char *buf, INT64_T length)
{
	return readlink(path, buf, length);
}

static INT64_T chirp_fs_local_chdir(const char *path)
{
	return chdir(path);
}

static INT64_T chirp_fs_local_mkdir(const char *path, INT64_T mode)
{
	return mkdir(path, 0700);
}

/*
rmdir is a little unusual.
An 'empty' directory may contain some administrative
files such as an ACL and an allocation state.
Only delete the directory if it contains only those files.
*/

static INT64_T chirp_fs_local_rmdir(const char *path)
{
	struct chirp_dir *dir;
	struct chirp_dirent *d;
	int empty = 1;

	dir = chirp_fs_local_opendir(path);
	if(dir) {
		while((d = chirp_fs_local_readdir(dir))) {
			if(!strcmp(d->name, "."))
				continue;
			if(!strcmp(d->name, ".."))
				continue;
			if(!strncmp(d->name, ".__", 3))
				continue;
			empty = 0;
			break;
		}
		chirp_fs_local_closedir(dir);

		if(empty) {
			if(delete_dir(path)) {
				return 0;
			} else {
				return -1;
			}
		} else {
			errno = ENOTEMPTY;
			return -1;
		}
	} else {
		return -1;
	}
}

static INT64_T chirp_fs_local_stat(const char *path, struct chirp_stat * buf)
{
	struct stat64 info;
	int result;
	result = stat64(path, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	return result;
}

static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat * buf)
{
	struct stat64 info;
	int result;
	result = lstat64(path, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	return result;
}

static INT64_T chirp_fs_local_statfs(const char *path, struct chirp_statfs * buf)
{
	struct statfs64 info;
	int result;
	result = statfs64(path, &info);
	if(result == 0) {
		memset(buf, 0, sizeof(*buf));
#ifdef CCTOOLS_OPSYS_SUNOS
		buf->f_type = info.f_fsid;
		buf->f_bsize = info.f_frsize;
#else
		buf->f_type = info.f_type;
		buf->f_bsize = info.f_bsize;
#endif
		buf->f_blocks = info.f_blocks;
		buf->f_bavail = info.f_bavail;
		buf->f_bfree = info.f_bfree;
		buf->f_files = info.f_files;
		buf->f_ffree = info.f_ffree;
	}
	return result;
}

static INT64_T chirp_fs_local_access(const char *path, INT64_T mode)
{
	return access(path, mode);
}

static INT64_T chirp_fs_local_chmod(const char *path, INT64_T mode)
{
	struct chirp_stat info;

	int result = chirp_fs_local_stat(path, &info);
	if(result < 0)
		return result;

	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.

	if(S_ISDIR(info.cst_mode)) {
		// On a directory, the user cannot set the execute bit.
		mode = 0700 | (mode & 0077);
	} else {
		// On a file, the user can set the execute bit.
		mode = 0600 | (mode & 0177);
	}

	return chmod(path, mode);
}

static INT64_T chirp_fs_local_chown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_local_lchown(const char *path, INT64_T uid, INT64_T gid)
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_local_truncate(const char *path, INT64_T length)
{
	return truncate64(path, length);
}

static INT64_T chirp_fs_local_utime(const char *path, time_t actime, time_t modtime)
{
	struct utimbuf ut;
	ut.actime = actime;
	ut.modtime = modtime;
	return utime(path, &ut);
}

static INT64_T chirp_fs_local_setrep( const char *path, int nreps )
{
	errno = EINVAL;
	return -1;
}

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
static INT64_T chirp_fs_local_getxattr ( const char *path, const char *name, void *data, size_t size )
{
	return getxattr(path, name, data, size);
}

static INT64_T chirp_fs_local_fgetxattr ( int fd, const char *name, void *data, size_t size )
{
	return fgetxattr(fd, name, data, size);
}

static INT64_T chirp_fs_local_lgetxattr ( const char *path, const char *name, void *data, size_t size )
{
	return lgetxattr(path, name, data, size);
}

static INT64_T chirp_fs_local_listxattr ( const char *path, char *list, size_t size )
{
	return listxattr(path, list, size);
}

static INT64_T chirp_fs_local_flistxattr ( int fd, char *list, size_t size )
{
	return flistxattr(fd, list, size);
}

static INT64_T chirp_fs_local_llistxattr ( const char *path, char *list, size_t size )
{
	return llistxattr(path, list, size);
}

static INT64_T chirp_fs_local_setxattr ( const char *path, const char *name, const void *data, size_t size, int flags )
{
	return setxattr(path, name, data, size, flags);
}

static INT64_T chirp_fs_local_fsetxattr ( int fd, const char *name, const void *data, size_t size, int flags )
{
	return fsetxattr(fd, name, data, size, flags);
}

static INT64_T chirp_fs_local_lsetxattr ( const char *path, const char *name, const void *data, size_t size, int flags )
{
	return lsetxattr(path, name, data, size, flags);
}

static INT64_T chirp_fs_local_removexattr ( const char *path, const char *name )
{
	return removexattr(path, name);
}

static INT64_T chirp_fs_local_fremovexattr ( int fd, const char *name )
{
	return fremovexattr(fd, name);
}

static INT64_T chirp_fs_local_lremovexattr ( const char *path, const char *name )
{
	return lremovexattr(path, name);
}
#endif

static int chirp_fs_do_acl_check()
{
	return 1;
}

struct chirp_filesystem chirp_fs_local = {
	chirp_fs_local_init,

	chirp_fs_local_open,
	chirp_fs_local_close,
	chirp_fs_local_pread,
	chirp_fs_local_pwrite,
	cfs_basic_sread,
	cfs_basic_swrite,
	chirp_fs_local_fstat,
	chirp_fs_local_fstatfs,
	chirp_fs_local_fchown,
	chirp_fs_local_fchmod,
	chirp_fs_local_ftruncate,
	chirp_fs_local_fsync,

	chirp_fs_local_opendir,
	chirp_fs_local_readdir,
	chirp_fs_local_closedir,

	cfs_basic_getfile,
	cfs_basic_putfile,

	chirp_fs_local_unlink,
	chirp_fs_local_rmall,
	chirp_fs_local_rename,
	chirp_fs_local_link,
	chirp_fs_local_symlink,
	chirp_fs_local_readlink,
	chirp_fs_local_chdir,
	chirp_fs_local_mkdir,
	chirp_fs_local_rmdir,
	chirp_fs_local_stat,
	chirp_fs_local_lstat,
	chirp_fs_local_statfs,
	chirp_fs_local_access,
	chirp_fs_local_chmod,
	chirp_fs_local_chown,
	chirp_fs_local_lchown,
	chirp_fs_local_truncate,
	chirp_fs_local_utime,
	cfs_basic_md5,
	chirp_fs_local_setrep,

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
	chirp_fs_local_getxattr,
	chirp_fs_local_fgetxattr,
	chirp_fs_local_lgetxattr,
	chirp_fs_local_listxattr,
	chirp_fs_local_flistxattr,
	chirp_fs_local_llistxattr,
	chirp_fs_local_setxattr,
	chirp_fs_local_fsetxattr,
	chirp_fs_local_lsetxattr,
	chirp_fs_local_removexattr,
	chirp_fs_local_fremovexattr,
	chirp_fs_local_lremovexattr,
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

	chirp_fs_do_acl_check,
};
