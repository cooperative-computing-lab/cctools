/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_fs_local_scheduler.h"

#include "debug.h"
#include "xxmalloc.h"
#include "int_sizes.h"
#include "path.h"
#include "full_io.h"
#include "delete_dir.h"

#include <dirent.h>
#include <unistd.h>
#include <utime.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif
#ifndef ENOATTR
#define ENOATTR  EINVAL
#endif

#include <sys/mount.h>
#include <sys/param.h>
#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(CCTOOLS_OPSYS_CYGWIN) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
/* Cygwin does not have 64-bit I/O, while FreeBSD/Darwin has it by default. */
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
#elif defined(CCTOOLS_OPSYS_DARWIN)
#define lchown chown
#elif defined(CCTOOLS_OPSYS_SUNOS)
/* Solaris has statfs, but it doesn't work! Use statvfs instead. */
#define statfs statvfs
#define fstatfs fstatvfs
#define statfs64 statvfs64
#define fstatfs64 fstatvfs64
#endif

#define COPY_STAT_LOCAL_TO_CHIRP(cinfo,linfo) \
	do {\
		struct chirp_stat *cinfop = &(cinfo);\
		struct stat64 *linfop = &(linfo);\
		memset(cinfop, 0, sizeof(struct chirp_stat));\
		cinfop->cst_dev = linfop->st_dev;\
		cinfop->cst_ino = linfop->st_ino;\
		cinfop->cst_mode = linfop->st_mode & (~0077);\
		cinfop->cst_nlink = linfop->st_nlink;\
		cinfop->cst_uid = linfop->st_uid;\
		cinfop->cst_gid = linfop->st_gid;\
		cinfop->cst_rdev = linfop->st_rdev;\
		cinfop->cst_size = linfop->st_size;\
		cinfop->cst_blksize = linfop->st_blksize;\
		cinfop->cst_blocks = linfop->st_blocks;\
		cinfop->cst_atime = linfop->st_atime;\
		cinfop->cst_mtime = linfop->st_mtime;\
		cinfop->cst_ctime = linfop->st_ctime;\
	} while (0)

#ifdef CCTOOLS_OPSYS_SUNOS
#define COPY_STATFS_LOCAL_TO_CHIRP(cinfo,linfo) \
	do {\
		struct chirp_statfs *cinfop = &(cinfo);\
		struct statfs64 *linfop = &(linfo);\
		memset(cinfop, 0, sizeof(struct chirp_statfs));\
		cinfop->f_type = linfop->f_fsid;\
		cinfop->f_bsize = linfop->f_frsize;\
		cinfop->f_blocks = linfop->f_blocks;\
		cinfop->f_bavail = linfop->f_bavail;\
		cinfop->f_bfree = linfop->f_bfree;\
		cinfop->f_files = linfop->f_files;\
		cinfop->f_ffree = linfop->f_ffree;\
	} while (0)
#else
#define COPY_STATFS_LOCAL_TO_CHIRP(cinfo,linfo) \
	do {\
		struct chirp_statfs *cinfop = &(cinfo);\
		struct statfs64 *linfop = &(linfo);\
		memset(cinfop, 0, sizeof(struct chirp_statfs));\
		cinfop->f_type = linfop->f_type;\
		cinfop->f_bsize = linfop->f_bsize;\
		cinfop->f_blocks = linfop->f_blocks;\
		cinfop->f_bavail = linfop->f_bavail;\
		cinfop->f_bfree = linfop->f_bfree;\
		cinfop->f_files = linfop->f_files;\
		cinfop->f_ffree = linfop->f_ffree;\
	} while (0)
#endif

static char local_root[CHIRP_PATH_MAX];

static struct {
	INT64_T fd;
	char path[CHIRP_PATH_MAX];
} open_files[CHIRP_FILESYSTEM_MAXFD];

#define fdvalid(fd) (0 <= fd && fd < CHIRP_FILESYSTEM_MAXFD && open_files[fd].fd >= 0)
#define SETUP_FILE \
if(!fdvalid(fd)) return (errno = EBADF, -1);\
int lfd = open_files[fd].fd;\
(void)lfd; /* silence unused warnings */

#define RESOLVE(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_local_resolve(path, resolved_##path) == -1) return -1;\
path = resolved_##path;

#define RESOLVENULL(path) \
char resolved_##path[CHIRP_PATH_MAX];\
if (chirp_fs_local_resolve(path, resolved_##path) == -1) return NULL;\
path = resolved_##path;

/*
 * The provided url could have any of the following forms:
 * o `local://path'
 * o `file://path'
 * o `path'
 */
#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
static int chirp_fs_local_init(const char url[CHIRP_PATH_MAX])
{
	int i;
	char tmp[CHIRP_PATH_MAX];

	if (strprfx(url, "local://") || strprfx(url, "file://"))
		strcpy(tmp, strstr(url, "//")+2);
	else
		strcpy(tmp, url);

	path_collapse(tmp, local_root, 1);

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].fd = -1;

	return cfs_create_dir("/", 0711);
}

static int chirp_fs_local_fname (int fd, char path[CHIRP_PATH_MAX])
{
	SETUP_FILE
	strcpy(path, open_files[fd].path);
	return 0;
}

int chirp_fs_local_resolve (const char *path, char resolved[CHIRP_PATH_MAX])
{
	int n;
	char collapse[CHIRP_PATH_MAX];
	char absolute[CHIRP_PATH_MAX];
	path_collapse(path, collapse, 1);
	n = snprintf(absolute, sizeof(absolute), "%s/%s", local_root, collapse);
	assert(n >= 0); /* this should never happen */
	if ((size_t)n >= CHIRP_PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	path_collapse(absolute, resolved, 1);
	return 0;
}

static INT64_T getfd(void)
{
	INT64_T fd;
	/* find an unused file descriptor */
	for(fd = 0; fd < CHIRP_FILESYSTEM_MAXFD; fd++)
		if(open_files[fd].fd == -1)
			return fd;
	debug(D_CHIRP, "too many files open");
	errno = EMFILE;
	return -1;
}

static INT64_T chirp_fs_local_open(const char *path, INT64_T flags, INT64_T mode)
{
	const char *unresolved = path;
	RESOLVE(path)
	INT64_T fd = getfd();
	if (fd == -1) return -1;
	mode = 0600 | (mode & 0100);
	INT64_T lfd = open64(path, flags, (int) mode);
	if (lfd >= 0) {
		open_files[fd].fd = lfd;
		strcpy(open_files[fd].path, unresolved);
		return fd;
	} else {
		return -1;
	}
}

static INT64_T chirp_fs_local_close(int fd)
{
	SETUP_FILE
	if (close(lfd) == 0) {
		open_files[fd].fd = -1;
		open_files[fd].path[0] = '\0';
		return 0;
	} else {
		return -1;
	}
}

static INT64_T chirp_fs_local_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	INT64_T result;
	result = full_pread64(lfd, buffer, length, offset);
	if(result < 0 && errno == ESPIPE) {
		/* if this is a pipe, return whatever amount is available */
		result = read(lfd, buffer, length);
	}
	return result;
}

static INT64_T chirp_fs_local_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	SETUP_FILE
	INT64_T result;
	result = full_pwrite64(lfd, buffer, length, offset);
	if(result < 0 && errno == ESPIPE) {
		/* if this is a pipe, then just write without the offset. */
		result = full_write(lfd, buffer, length);
	}
	return result;
}

static INT64_T chirp_fs_local_lockf (int fd, int cmd, INT64_T len)
{
	SETUP_FILE
	return lockf(lfd, cmd, len);
}

static INT64_T chirp_fs_local_fstat(int fd, struct chirp_stat *info)
{
	SETUP_FILE
	struct stat64 linfo;
	int result = fstat64(lfd, &linfo);
	if(result == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	return result;
}

static INT64_T chirp_fs_local_fstatfs(int fd, struct chirp_statfs *info)
{
	SETUP_FILE
	struct statfs64 linfo;
	int result = fstatfs64(lfd, &linfo);
	if (result == 0)
		COPY_STATFS_LOCAL_TO_CHIRP(*info, linfo);
	return result;
}

static INT64_T chirp_fs_local_fchown(int fd, INT64_T uid, INT64_T gid)
{
	SETUP_FILE
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

static INT64_T chirp_fs_local_fchmod(int fd, INT64_T mode)
{
	SETUP_FILE
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode = 0600 | (mode & 0177);
	return fchmod(lfd, mode);
}

static INT64_T chirp_fs_local_ftruncate(int fd, INT64_T length)
{
	SETUP_FILE
	return ftruncate64(lfd, length);
}

static INT64_T chirp_fs_local_fsync(int fd)
{
	SETUP_FILE
	return fsync(lfd);
}

static INT64_T chirp_fs_local_unlink(const char *path)
{
	RESOLVE(path)

	int result = unlink(path);

	/*
	   On Solaris, an unlink on a directory
	   returns EPERM when it should return EISDIR.
	   Check for this cast, and then fix it.
	 */

	if(result < 0 && errno == EPERM) {
		struct stat64 linfo;
		result = stat64(path, &linfo);
		if(result == 0 && S_ISDIR(linfo.st_mode)) {
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
	RESOLVE(path)
	RESOLVE(newpath)
	return rename(path, newpath);
}

static INT64_T chirp_fs_local_link(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	return link(path, newpath);
}

static INT64_T chirp_fs_local_symlink(const char *path, const char *newpath)
{
	RESOLVE(path)
	RESOLVE(newpath)
	return symlink(path, newpath);
}

static INT64_T chirp_fs_local_readlink(const char *path, char *buf, INT64_T length)
{
	RESOLVE(path)
	return readlink(path, buf, length);
}

static INT64_T chirp_fs_local_chdir(const char *path)
{
	RESOLVE(path)
	return chdir(path);
}

static INT64_T chirp_fs_local_mkdir(const char *path, INT64_T mode)
{
	RESOLVE(path)
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
	int empty = 1;
	RESOLVE(path)

	DIR *dir = opendir(path);
	if(dir) {
		struct dirent *d;
		while((d = readdir(dir))) {
			if(!strcmp(d->d_name, "."))
				continue;
			if(!strcmp(d->d_name, ".."))
				continue;
			if(!strncmp(d->d_name, ".__", 3))
				continue;
			empty = 0;
			break;
		}
		closedir(dir);

		if(empty) {
			return delete_dir(path);
		} else {
			errno = ENOTEMPTY;
			return -1;
		}
	} else {
		return -1;
	}
}

static INT64_T chirp_fs_local_stat(const char *path, struct chirp_stat *info)
{
	RESOLVE(path)
	struct stat64 linfo;
	int result = stat64(path, &linfo);
	if(result == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	return result;
}

static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat *info)
{
	RESOLVE(path)
	struct stat64 linfo;
	int result = lstat64(path, &linfo);
	if(result == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	return result;
}

static INT64_T chirp_fs_local_statfs(const char *path, struct chirp_statfs *info)
{
	RESOLVE(path)
	struct statfs64 linfo;
	int result = statfs64(path, &linfo);
	if (result == 0)
		COPY_STATFS_LOCAL_TO_CHIRP(*info, linfo);
	return result;
}

static INT64_T chirp_fs_local_access(const char *path, INT64_T mode)
{
	RESOLVE(path)
	return access(path, mode);
}

struct chirp_dir {
	char path[CHIRP_PATH_MAX];
	DIR *dir;
	struct chirp_dirent cd;
};

static struct chirp_dir *chirp_fs_local_opendir(const char *path)
{
	RESOLVENULL(path)
	DIR *dir = opendir(path);
	if(dir) {
		struct chirp_dir *cdir = xxmalloc(sizeof(*cdir));
		cdir->dir = dir;
		strcpy(cdir->path, path); /* N.B. readdir passes this to chirp_fs_local_lstat */
		return cdir;
	} else {
		return 0;
	}
}

static struct chirp_dirent *chirp_fs_local_readdir(struct chirp_dir *dir)
{
	struct dirent *d = readdir(dir->dir);
	if(d) {
		char path[CHIRP_PATH_MAX];
		struct stat64 linfo;
		dir->cd.name = d->d_name;
		sprintf(path, "%s/%s", dir->path, dir->cd.name);
		memset(&dir->cd.info, 0, sizeof(dir->cd.info));
		dir->cd.lstatus = lstat64(path, &linfo);
		if(dir->cd.lstatus == 0)
			COPY_STAT_LOCAL_TO_CHIRP(dir->cd.info, linfo);
		return &dir->cd;
	} else {
		return 0;
	}
}

static void chirp_fs_local_closedir(struct chirp_dir *dir)
{
	closedir(dir->dir);
	free(dir);
}

static INT64_T chirp_fs_local_chmod(const char *path, INT64_T mode)
{
	struct stat64 linfo;
	RESOLVE(path)

	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.

	if (stat64(path, &linfo) == -1)
		return -1;
	if(S_ISDIR(linfo.st_mode)) {
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
	RESOLVE(path)
	return truncate64(path, length);
}

static INT64_T chirp_fs_local_utime(const char *path, time_t actime, time_t modtime)
{
	struct utimbuf ut;
	RESOLVE(path)
	ut.actime = actime;
	ut.modtime = modtime;
	return utime(path, &ut);
}

static INT64_T chirp_fs_local_setrep(const char *path, int nreps)
{
	errno = EINVAL;
	return -1;
}

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
static INT64_T chirp_fs_local_getxattr(const char *path, const char *name, void *data, size_t size)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return getxattr(path, name, data, size, 0, 0);
#else
	return getxattr(path, name, data, size);
#endif
}

static INT64_T chirp_fs_local_fgetxattr(int fd, const char *name, void *data, size_t size)
{
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	return fgetxattr(lfd, name, data, size, 0, 0);
#else
	return fgetxattr(lfd, name, data, size);
#endif
}

static INT64_T chirp_fs_local_lgetxattr(const char *path, const char *name, void *data, size_t size)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return getxattr(path, name, data, size, 0, XATTR_NOFOLLOW);
#else
	return lgetxattr(path, name, data, size);
#endif
}

static INT64_T chirp_fs_local_listxattr(const char *path, char *list, size_t size)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return listxattr(path, list, size, 0);
#else
	return listxattr(path, list, size);
#endif
}

static INT64_T chirp_fs_local_flistxattr(int fd, char *list, size_t size)
{
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	return flistxattr(lfd, list, size, 0);
#else
	return flistxattr(lfd, list, size);
#endif
}

static INT64_T chirp_fs_local_llistxattr(const char *path, char *list, size_t size)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return listxattr(path, list, size, XATTR_NOFOLLOW);
#else
	return llistxattr(path, list, size);
#endif
}

static INT64_T chirp_fs_local_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return setxattr(path, name, data, size, 0, flags);
#else
	return setxattr(path, name, data, size, flags);
#endif
}

static INT64_T chirp_fs_local_fsetxattr(int fd, const char *name, const void *data, size_t size, int flags)
{
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	return fsetxattr(lfd, name, data, size, 0, flags);
#else
	return fsetxattr(lfd, name, data, size, flags);
#endif
}

static INT64_T chirp_fs_local_lsetxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return setxattr(path, name, data, size, 0, XATTR_NOFOLLOW | flags);
#else
	return lsetxattr(path, name, data, size, flags);
#endif
}

static INT64_T chirp_fs_local_removexattr(const char *path, const char *name)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return removexattr(path, name, 0);
#else
	return removexattr(path, name);
#endif
}

static INT64_T chirp_fs_local_fremovexattr(int fd, const char *name)
{
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	return fremovexattr(lfd, name, 0);
#else
	return fremovexattr(lfd, name);
#endif
}

static INT64_T chirp_fs_local_lremovexattr(const char *path, const char *name)
{
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	return removexattr(path, name, XATTR_NOFOLLOW);
#else
	return lremovexattr(path, name);
#endif
}
#endif

static int chirp_fs_do_acl_check()
{
	return 1;
}

struct chirp_filesystem chirp_fs_local = {
	chirp_fs_local_init,

	chirp_fs_local_fname,

	chirp_fs_local_open,
	chirp_fs_local_close,
	chirp_fs_local_pread,
	chirp_fs_local_pwrite,
	cfs_basic_sread,
	cfs_basic_swrite,
	chirp_fs_local_lockf,
	chirp_fs_local_fstat,
	chirp_fs_local_fstatfs,
	chirp_fs_local_fchown,
	chirp_fs_local_fchmod,
	chirp_fs_local_ftruncate,
	chirp_fs_local_fsync,

	cfs_basic_search,

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

	chirp_fs_local_job_dbinit,
	chirp_fs_local_job_schedule,
};

/* vim: set noexpandtab tabstop=4: */
