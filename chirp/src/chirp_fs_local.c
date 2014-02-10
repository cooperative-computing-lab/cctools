/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_fs_local_scheduler.h"

#include "create_dir.h"
#include "debug.h"
#include "delete_dir.h"
#include "full_io.h"
#include "int_sizes.h"
#include "path.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <unistd.h>
#include <utime.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
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
		cinfop->cst_mode = linfop->st_mode;\
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

#define PREAMBLE(fmt, ...) \
	INT64_T rc = 0;\
	debug(D_LOCAL, fmt, __VA_ARGS__);

#define PROLOGUE \
	if (rc == -1)\
		debug(D_LOCAL, "= -1 (errno = %d; `%s')", errno, strerror(errno));\
	else\
		debug(D_LOCAL, "= %" PRId64, rc);\
	return rc;

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
	PREAMBLE("init(`%s')", url);
	int i;
	char tmp[CHIRP_PATH_MAX];

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].fd = -1;

	if (strprfx(url, "local://") || strprfx(url, "file://"))
		strcpy(tmp, strstr(url, "//")+2);
	else
		strcpy(tmp, url);

	path_collapse(tmp, local_root, 1);
	rc = create_dir(local_root, 0711);

	PROLOGUE
}

static int chirp_fs_local_fname (int fd, char path[CHIRP_PATH_MAX])
{
	PREAMBLE("fname(%d, %p)", fd, path);
	SETUP_FILE
	strcpy(path, open_files[fd].path);
	PROLOGUE
}

int chirp_fs_local_resolve (const char *path, char resolved[CHIRP_PATH_MAX])
{
	char collapse[CHIRP_PATH_MAX];
	char absolute[CHIRP_PATH_MAX];
	path_collapse(path, collapse, 1);
	int n = snprintf(absolute, sizeof(absolute), "%s/%s", local_root, collapse);
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
	PREAMBLE("open(`%s', 0x%" PRIx64 ", 0o%" PRIo64 ")", path, flags, mode);
	const char *unresolved = path;
	RESOLVE(path)
	INT64_T fd = getfd();
	if (fd == -1) return -1;
	mode &= S_IXUSR|S_IRWXG|S_IRWXO;
	mode |= S_IRUSR|S_IWUSR;
	rc = open64(path, flags, (int) mode);
	if (rc >= 0) {
		open_files[fd].fd = rc;
		strcpy(open_files[fd].path, unresolved);
		rc = fd;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_close(int fd)
{
	PREAMBLE("close(%d)", fd);
	SETUP_FILE
	rc = close(lfd);
	if (rc == 0) {
		open_files[fd].fd = -1;
		open_files[fd].path[0] = '\0';
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_pread(int fd, void *buffer, INT64_T length, INT64_T offset)
{
	PREAMBLE("pread(%d, %p, %zu, %" PRId64 ")", fd, buffer, (size_t)length, offset);
	SETUP_FILE
	rc = full_pread64(lfd, buffer, length, offset);
	if(rc < 0 && errno == ESPIPE) {
		/* if this is a pipe, return whatever amount is available */
		rc = read(lfd, buffer, length);
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_pwrite(int fd, const void *buffer, INT64_T length, INT64_T offset)
{
	PREAMBLE("pwrite(%d, %p, %zu, %" PRId64 ")", fd, buffer, (size_t)length, offset);
	SETUP_FILE
	rc = full_pwrite64(lfd, buffer, length, offset);
	if(rc < 0 && errno == ESPIPE) {
		/* if this is a pipe, then just write without the offset. */
		rc = full_write(lfd, buffer, length);
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_lockf (int fd, int cmd, INT64_T len)
{
	PREAMBLE("lockf(%d, 0o%o, %" PRId64 ")", fd, cmd, len);
	SETUP_FILE
	rc = lockf(lfd, cmd, len);
	PROLOGUE
}

static INT64_T chirp_fs_local_fstat(int fd, struct chirp_stat *info)
{
	PREAMBLE("fstat(%d, %p)", fd, info);
	SETUP_FILE
	struct stat64 linfo;
	rc = fstat64(lfd, &linfo);
	if(rc == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_fstatfs(int fd, struct chirp_statfs *info)
{
	PREAMBLE("fstatfs(%d, %p)", fd, info);
	SETUP_FILE
	struct statfs64 linfo;
	rc = fstatfs64(lfd, &linfo);
	if (rc == 0)
		COPY_STATFS_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_fchmod(int fd, INT64_T mode)
{
	struct stat64 linfo;
	PREAMBLE("fchmod(%d, 0o%" PRIo64 ")", fd, mode);
	SETUP_FILE
	mode &= S_IXUSR|S_IRWXG|S_IRWXO; /* users can only set owner execute and group/other bits */
	if (fstat64(lfd, &linfo) == -1)
		return -1;
	if(S_ISDIR(linfo.st_mode)) {
		mode |= S_IRWXU; /* all owner bits must be set */
	} else {
		mode |= S_IRUSR|S_IWUSR; /* owner read/write must be set */
	}
	rc = fchmod(lfd, mode);
	PROLOGUE
}

static INT64_T chirp_fs_local_ftruncate(int fd, INT64_T length)
{
	PREAMBLE("ftruncate(%d, %" PRId64 ")", fd, length);
	SETUP_FILE
	rc = ftruncate64(lfd, length);
	PROLOGUE
}

static INT64_T chirp_fs_local_fsync(int fd)
{
	PREAMBLE("fsync(%d)", fd);
	SETUP_FILE
	rc = fsync(lfd);
	PROLOGUE
}

static INT64_T chirp_fs_local_unlink(const char *path)
{
	PREAMBLE("unlink(`%s')", path);
	RESOLVE(path)

	rc = unlink(path);

	/*
	   On Solaris, an unlink on a directory
	   returns EPERM when it should return EISDIR.
	   Check for this cast, and then fix it.
	 */

	if(rc < 0 && errno == EPERM) {
		struct stat64 linfo;
		rc = stat64(path, &linfo);
		if(rc == 0 && S_ISDIR(linfo.st_mode)) {
			rc = -1;
			errno = EISDIR;
		} else {
			rc = -1;
			errno = EPERM;
		}
	}

	PROLOGUE
}

static INT64_T chirp_fs_local_rename(const char *old, const char *new)
{
	PREAMBLE("rename(`%s', `%s')", old, new);
	RESOLVE(old)
	RESOLVE(new)
	rc = rename(old, new);
	PROLOGUE
}

static INT64_T chirp_fs_local_link(const char *target, const char *path)
{
	PREAMBLE("link(`%s', `%s')", target, path);
	RESOLVE(target)
	RESOLVE(path)
	rc = link(target, path);
	PROLOGUE
}

static INT64_T chirp_fs_local_symlink(const char *target, const char *path)
{
	PREAMBLE("symlink(`%s', `%s')", target, path);
	RESOLVE(target)
	RESOLVE(path)
	rc = symlink(target, path);
	PROLOGUE
}

static INT64_T chirp_fs_local_readlink(const char *path, char *buf, INT64_T length)
{
	PREAMBLE("readlink(`%s', %p, %zu)", path, buf, (size_t)length);
	RESOLVE(path)
	rc = readlink(path, buf, length);
	PROLOGUE
}

static INT64_T chirp_fs_local_mkdir(const char *path, INT64_T mode)
{
	PREAMBLE("mkdir(`%s', 0o%" PRIo64 ")", path, mode);
	RESOLVE(path)
	mode &= S_IRWXG|S_IRWXO; /* users can only set group/other bits */
	mode |= S_IRWXU;
	rc = mkdir(path, mode);
	PROLOGUE
}

/*
rmdir is a little unusual.
An 'empty' directory may contain some administrative
files such as an ACL and an allocation state.
Only delete the directory if it contains only those files.
*/

static INT64_T chirp_fs_local_rmdir(const char *path)
{
	PREAMBLE("rmdir(`%s')", path);
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
			rc = delete_dir(path);
		} else {
			errno = ENOTEMPTY;
			rc = -1;
		}
	} else {
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_stat(const char *path, struct chirp_stat *info)
{
	PREAMBLE("stat(`%s', %p)", path, info);
	RESOLVE(path)
	struct stat64 linfo;
	rc = stat64(path, &linfo);
	if(rc == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat *info)
{
	PREAMBLE("lstat(`%s', %p)", path, info);
	RESOLVE(path)
	struct stat64 linfo;
	rc = lstat64(path, &linfo);
	if(rc == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_statfs(const char *path, struct chirp_statfs *info)
{
	PREAMBLE("statfs(`%s', %p)", path, info);
	RESOLVE(path)
	struct statfs64 linfo;
	rc = statfs64(path, &linfo);
	if (rc == 0)
		COPY_STATFS_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_access(const char *path, INT64_T amode)
{
	PREAMBLE("access(`%s', 0x%" PRIx64 ")", path, amode);
	RESOLVE(path)
	rc = access(path, amode);
	PROLOGUE
}

struct chirp_dir {
	char path[CHIRP_PATH_MAX];
	DIR *dir;
	struct chirp_dirent cd;
};

static struct chirp_dir *chirp_fs_local_opendir(const char *path)
{
	debug(D_LOCAL, "opendir(`%s')", path);
	RESOLVENULL(path)
	DIR *dir = opendir(path);
	if(dir) {
		struct chirp_dir *cdir = xxmalloc(sizeof(*cdir));
		cdir->dir = dir;
		strcpy(cdir->path, path); /* N.B. readdir passes this to chirp_fs_local_lstat */
		debug(D_LOCAL, "= %p", cdir);
		return cdir;
	} else {
		debug(D_LOCAL, "= NULL (errno = %d; `%s')", errno, strerror(errno));
		return 0;
	}
}

static struct chirp_dirent *chirp_fs_local_readdir(struct chirp_dir *dir)
{
	debug(D_LOCAL, "readdir(%p [`%s'])", dir, dir->path);
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
		debug(D_LOCAL, "= %p [name = `%s']", &dir->cd, dir->cd.name);
		return &dir->cd;
	} else {
		debug(D_LOCAL, "= NULL (errno = %d; `%s')", errno, strerror(errno));
		return 0;
	}
}

static void chirp_fs_local_closedir(struct chirp_dir *dir)
{
	debug(D_LOCAL, "closedir(%p [`%s'])", dir, dir->path);
	closedir(dir->dir);
	free(dir);
}

static INT64_T chirp_fs_local_chmod(const char *path, INT64_T mode)
{
	PREAMBLE("chmod(`%s', 0o%" PRIo64 ")", path, mode);
	struct stat64 linfo;
	RESOLVE(path)
	mode &= S_IXUSR|S_IRWXG|S_IRWXO; /* users can only set owner execute and group/other bits */
	if (stat64(path, &linfo) == -1)
		return -1;
	if(S_ISDIR(linfo.st_mode)) {
		mode |= S_IRWXU; /* all owner bits must be set */
	} else {
		mode |= S_IRUSR|S_IWUSR; /* owner read/write must be set */
	}
	rc = chmod(path, mode);
	PROLOGUE
}

static INT64_T chirp_fs_local_truncate(const char *path, INT64_T length)
{
	PREAMBLE("truncate(`%s', 0d%" PRId64 ")", path, length);
	RESOLVE(path)
	rc = truncate64(path, length);
	PROLOGUE
}

static INT64_T chirp_fs_local_utime(const char *path, time_t actime, time_t modtime)
{
	PREAMBLE("utime(`%s', actime = %" PRId64 " modtime = %" PRId64 ")", path, (int64_t)actime, (int64_t)modtime);
	struct utimbuf ut;
	RESOLVE(path)
	ut.actime = actime;
	ut.modtime = modtime;
	rc = utime(path, &ut);
	PROLOGUE
}

static INT64_T chirp_fs_local_setrep(const char *path, int nreps)
{
	errno = EINVAL;
	return -1;
}

#if defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H)
static INT64_T chirp_fs_local_getxattr(const char *path, const char *name, void *data, size_t size)
{
	PREAMBLE("getxattr(`%s', `%s', %p, %zu)", path, name, data, size);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = getxattr(path, name, data, size, 0, 0);
#else
	rc = getxattr(path, name, data, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_fgetxattr(int fd, const char *name, void *data, size_t size)
{
	PREAMBLE("fgetxattr(%d, `%s', %p, %zu)", fd, name, data, size);
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = fgetxattr(lfd, name, data, size, 0, 0);
#else
	rc = fgetxattr(lfd, name, data, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_lgetxattr(const char *path, const char *name, void *data, size_t size)
{
	PREAMBLE("lgetxattr(`%s', `%s', %p, %zu)", path, name, data, size);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = getxattr(path, name, data, size, 0, XATTR_NOFOLLOW);
#else
	rc = lgetxattr(path, name, data, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_listxattr(const char *path, char *list, size_t size)
{
	PREAMBLE("listxattr(`%s', %p, %zu)", path, list, size);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = listxattr(path, list, size, 0);
#else
	rc = listxattr(path, list, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_flistxattr(int fd, char *list, size_t size)
{
	PREAMBLE("flistxattr(%d, %p, %zu)", fd, list, size);
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = flistxattr(lfd, list, size, 0);
#else
	rc = flistxattr(lfd, list, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_llistxattr(const char *path, char *list, size_t size)
{
	PREAMBLE("llistxattr(`%s', %p, %zu)", path, list, size);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = listxattr(path, list, size, XATTR_NOFOLLOW);
#else
	rc = llistxattr(path, list, size);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	PREAMBLE("setxattr(`%s', `%s', %p, %zu, %d)", path, name, data, size, flags);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = setxattr(path, name, data, size, 0, flags);
#else
	rc = setxattr(path, name, data, size, flags);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_fsetxattr(int fd, const char *name, const void *data, size_t size, int flags)
{
	PREAMBLE("fsetxattr(%d, `%s', %p, %zu, %d)", fd, name, data, size, flags);
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = fsetxattr(lfd, name, data, size, 0, flags);
#else
	rc = fsetxattr(lfd, name, data, size, flags);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_lsetxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	PREAMBLE("lsetxattr(`%s', `%s', %p, %zu, %d)", path, name, data, size, flags);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = setxattr(path, name, data, size, 0, XATTR_NOFOLLOW | flags);
#else
	rc = lsetxattr(path, name, data, size, flags);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_removexattr(const char *path, const char *name)
{
	PREAMBLE("removexattr(`%s', `%s')", path, name);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = removexattr(path, name, 0);
#else
	rc = removexattr(path, name);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_fremovexattr(int fd, const char *name)
{
	PREAMBLE("fremovexattr(%d, `%s')", fd, name);
	SETUP_FILE
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = fremovexattr(lfd, name, 0);
#else
	rc = fremovexattr(lfd, name);
#endif
	PROLOGUE
}

static INT64_T chirp_fs_local_lremovexattr(const char *path, const char *name)
{
	PREAMBLE("lremovexattr(`%s', `%s')", path, name);
	RESOLVE(path)
#ifdef CCTOOLS_OPSYS_DARWIN
	rc = removexattr(path, name, XATTR_NOFOLLOW);
#else
	rc = lremovexattr(path, name);
#endif
	PROLOGUE
}
#endif

static int chirp_fs_do_acl_check()
{
	return 1;
}

struct chirp_filesystem chirp_fs_local = {
	chirp_fs_local_init,
	cfs_stub_destroy,

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
	cfs_basic_fchown,
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
	cfs_basic_rmall,
	chirp_fs_local_rename,
	chirp_fs_local_link,
	chirp_fs_local_symlink,
	chirp_fs_local_readlink,
	chirp_fs_local_mkdir,
	chirp_fs_local_rmdir,
	chirp_fs_local_stat,
	chirp_fs_local_lstat,
	chirp_fs_local_statfs,
	chirp_fs_local_access,
	chirp_fs_local_chmod,
	cfs_basic_chown,
	cfs_basic_lchown,
	chirp_fs_local_truncate,
	chirp_fs_local_utime,
	cfs_basic_hash,
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
