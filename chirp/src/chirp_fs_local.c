/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_fs_local_scheduler.h"

#include "catch.h"
#include "compat-at.h"
#include "debug.h"
#include "unlink_recursive.h"
#include "full_io.h"
#include "int_sizes.h"
#include "mkdir_recursive.h"
#include "path.h"
#include "uuid.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <unistd.h>
#include <utime.h>

#if defined(HAS_ATTR_XATTR_H)
#	include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#	include <sys/xattr.h>
#endif

#include <sys/mount.h>
#include <sys/param.h>
#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#	include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#	include <sys/statvfs.h>
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef CCTOOLS_OPSYS_DARWIN
/* Darwin has 64-bit I/O by default */
#	define stat64 stat
#	define fstat64 fstat
#	define ftruncate64 ftruncate
#	define statfs64 statfs
#	define fstatfs64 fstatfs
#	define fstatat64 fstatat
#endif

#ifndef O_CLOEXEC
#	define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#	define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#	define O_NOFOLLOW 0
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

static int rootfd = -1;

static struct {
	INT64_T fd;
	char path[CHIRP_PATH_MAX];
} open_files[CHIRP_FILESYSTEM_MAXFD];

static const char nulpath[1] = "";

#define PREAMBLE(fmt, ...) \
	INT64_T rc = 0;\
	int dirfd = -1;\
	char basename[CHIRP_PATH_MAX];\
	debug(D_LOCAL, fmt, __VA_ARGS__);\
	(void)rc;\
	(void)dirfd;\
	(void)basename[1];\

#define CLOSE_DIRFD(fd) \
	do {\
		if (fd >= 0) {\
			int s = errno;\
			close(fd);\
			errno = s;\
			fd = -1;\
		}\
	} while (0)

#define PROLOGUE \
	goto out;\
out:\
	if (rc == -1)\
		debug(D_LOCAL, "= -1 (errno = %d; `%s')", errno, strerror(errno));\
	else\
		debug(D_LOCAL, "= %" PRId64, rc);\
	CLOSE_DIRFD(dirfd);\
	return rc;

#define fdvalid(fd) (0 <= fd && fd < CHIRP_FILESYSTEM_MAXFD && open_files[fd].fd >= 0)
#define SETUP_FILE \
	if(!fdvalid(fd)) return (errno = EBADF, -1);\
	int lfd = open_files[fd].fd;\
	(void)lfd; /* silence unused warnings */

#define RESOLVE(path, follow) \
	int dirfd_##path;\
	char basename_##path[CHIRP_PATH_MAX];\
	if (chirp_fs_local_resolve(path, &dirfd_##path, basename_##path, follow) == -1) return -1;\
	strcpy(basename, basename_##path);\
	path = nulpath;\
	dirfd = dirfd_##path;

#define RESOLVENULL(path, follow) \
	int dirfd_##path;\
	char basename_##path[CHIRP_PATH_MAX];\
	if (chirp_fs_local_resolve(path, &dirfd_##path, basename_##path, follow) == -1) return NULL;\
	strcpy(basename, basename_##path);\
	path = nulpath;\
	dirfd = dirfd_##path;

/*
 * The provided url could have any of the following forms:
 * o `local://path'
 * o `file://path'
 * o `path'
 */
#define strprfx(s,p) (strncmp(s,p "",sizeof(p)-1) == 0)
static int chirp_fs_local_init(const char url[CHIRP_PATH_MAX], cctools_uuid_t *uuid)
{
	PREAMBLE("init(`%s')", url);
	int i;
	int fd = -1;
	char tmp[CHIRP_PATH_MAX];
	char root[CHIRP_PATH_MAX];

	for (i = 0; i < CHIRP_FILESYSTEM_MAXFD; i++)
		open_files[i].fd = -1;

	if (strprfx(url, "local://") || strprfx(url, "file://"))
		strcpy(tmp, strstr(url, "//")+2);
	else
		strcpy(tmp, url);

	path_collapse(tmp, root, 1);
	CATCHUNIX(mkdir_recursive(root, S_IRWXU|S_IXGRP|S_IXOTH));
	CATCHUNIX(rootfd = open(root, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOCTTY));
#if O_DIRECTORY == 0
	if (rootfd >= 0) {
		struct stat info;
		rc = UNIXRC(fstat(rootfd, &info));
		if (rc == 0) {
			if (!S_ISDIR(info.st_mode)) {
				PROTECT(close(rootfd));
				CATCH(ENOTDIR);
			}
		} else {
			PROTECT(close(rootfd));
			CATCH(rc);
		}
	}
#endif

	rc = openat(rootfd, ".__uuid", O_RDONLY, 0);
	if (rc >= 0) {
		fd = rc;
		memset(uuid->str, 0, sizeof uuid->str);
		rc = full_read(fd, uuid->str, UUID_LEN);
		PROTECT(close(fd));
		CATCHUNIX(rc);
		if ((size_t)rc < UUID_LEN)
			fatal("bad uuid");
	} else if (rc == -1 && errno == ENOENT) {
		cctools_uuid_create(uuid);
		CATCHUNIX(fd = openat(rootfd, ".__uuid", O_WRONLY|O_TRUNC|O_CREAT, S_IRUSR|S_IWUSR));
		rc = full_write(fd, uuid->str, UUID_LEN);
		PROTECT(close(fd));
		CATCHUNIX(rc);
		if ((size_t)rc < UUID_LEN)
			fatal("bad uuid write");
	}

	rc = 0;
	PROLOGUE
}

static int chirp_fs_local_fname (int fd, char path[CHIRP_PATH_MAX])
{
	PREAMBLE("fname(%d, %p)", fd, path);
	SETUP_FILE
	strcpy(path, open_files[fd].path);
	PROLOGUE
}

int chirp_fs_local_resolve (const char *path, int *dirfd, char basename[CHIRP_PATH_MAX], int follow)
{
	int i;
	int rc;
	int fd=-1;
	char working[CHIRP_PATH_MAX] = "";
	struct stat rootinfo;

	if (path[0] == 0)
		CATCH(EINVAL);

	CATCHUNIX(fstat(rootfd, &rootinfo));

	CATCHUNIX(fd = dup(rootfd));

	CATCHUNIX(snprintf(working, sizeof(working), "%s", path));
	if (rc >= CHIRP_PATH_MAX)
		CATCH(ENAMETOOLONG);

	for (i = 0; i < 100; i++) {
		char component[CHIRP_PATH_MAX];

		debug(D_DEBUG, "path '%s' resolution: working = '%s'", path, working);
		strcpy(basename, ""); /* mark as incomplete */

		char *slash = strchr(working, '/');
		if (slash) {
			if (slash == working) {
				slash += strspn(slash, "/");
				memmove(working, slash, strlen(slash)+1);
				CATCHUNIX(dup2(rootfd, fd));
				continue;
			} else {
				size_t len = (size_t)(slash-working);
				memcpy(component, working, len);
				component[len] = 0;
				slash += strspn(slash, "/");
				memmove(working, slash, strlen(slash)+1);
			}
			debug(D_DEBUG, "path '%s' resolution: component = '%s'", path, component);
		} else {
			if (working[0]) {
				strcpy(basename, working);
			} else {
				strcpy(basename, "."); /* refer to dirfd itself */
			}
			debug(D_DEBUG, "path '%s' resolution: final component: %s", path, basename);
			if (!follow)
				break; /* we're done! */
			strcpy(component, working);
			strcpy(working, "");
		}

		if (strcmp(component, "..") == 0) {
			struct stat info;
			CATCHUNIX(fstat(fd, &info));
			if (rootinfo.st_dev == info.st_dev && rootinfo.st_ino == info.st_ino) {
				debug(D_DEBUG, "caught .. at root");
				continue;
			}
		} else if (strcmp(component, ".") == 0) {
			continue;
		} else {
			char _sym[CHIRP_PATH_MAX] = "";
			char *sym = _sym;
			ssize_t n = readlinkat(fd, component, sym, CHIRP_PATH_MAX);
			if (n >= 0) {
				if (n < CHIRP_PATH_MAX) {
					char new[CHIRP_PATH_MAX];
					debug(D_DEBUG, "path '%s' resolution: component link: '%s' -> '%s'", path, component, sym);
					CATCHUNIX(snprintf(new, CHIRP_PATH_MAX, "%s/%s", sym, working));
					if (rc >= CHIRP_PATH_MAX)
						CATCH(ENAMETOOLONG);
					strcpy(working, new);
					continue;
				} else {
					CATCH(ENAMETOOLONG);
				}
			}
		}

		if (basename[0]) {
			break; /* we found the final component and it wasn't a link, we're done */
		}

		if (working[0] == 0) {
			/* On Linux and possibly other kernels, some system calls like
			 * rmdir/mkdir permit a trailing slash. Strictly speaking, this
			 * should always fail since Unix [1] specifies that paths ending in
			 * a forward slash are equivalent to "path/." (i.e. with a trailing
			 * . added). rmdir on a path with a trailing dot shall always fail
			 * [2]. Obviously mkdir on . should also fail with EEXIST.
			 *
			 * At this point, we already removed / after this component, so we
			 * only check if working is empty. The if above checks this. We've
			 * already confirmed component is not a link.
			 *
			 * [1] http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_12
			 * [2] http://pubs.opengroup.org/onlinepubs/9699919799/functions/rmdir.html
			*/
			strcpy(basename, component);
			break;
		}

		/* XXX Unavoidable race condition here between readlinkat and openat. O_NOFOLLOW catches it if supported by kernel. */
		/* Solution is using O_PATH if available. */

		int nfd = openat(fd, component, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOFOLLOW|O_NOCTTY, 0);
		CATCHUNIX(nfd);
		close(fd);
		fd = nfd;
#if O_DIRECTORY == 0
		struct stat info;
		CATCHUNIX(fstat(fd, &info));
		if (!S_ISDIR(info.st_mode))
			CATCH(ENOTDIR);
#endif
	}
	if (i == 100)
		CATCH(ELOOP);

	*dirfd = fd;

	rc = 0;
	goto out;
out:
	if (rc) {
		close(fd);
	}

	return RCUNIX(rc);
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
	RESOLVE(path, 1)
	INT64_T fd = getfd();
	if (fd >= 0) {
		mode &= S_IXUSR|S_IRWXG|S_IRWXO;
		mode |= S_IRUSR|S_IWUSR;
		rc = openat(dirfd, basename, flags|O_NOFOLLOW, mode);
		if (rc >= 0) {
			open_files[fd].fd = rc;
			strcpy(open_files[fd].path, unresolved);
			rc = fd;
		}
	} else {
		rc = -1;
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
	PREAMBLE("fchmod(%d, 0o%" PRIo64 ")", fd, mode);
	SETUP_FILE
	struct stat64 linfo;
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
	RESOLVE(path, 0)

	rc = unlinkat(dirfd, basename, 0);

	/*
	   On Solaris, an unlink on a directory
	   returns EPERM when it should return EISDIR.
	   Check for this case, and then fix it.
	 */

	if(rc < 0 && errno == EPERM) {
		struct stat64 linfo;
		rc = fstatat64(dirfd, basename, &linfo, AT_SYMLINK_NOFOLLOW);
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

static INT64_T chirp_fs_local_rmall(const char *path)
{
	PREAMBLE("rmall(`%s')", path);
	RESOLVE(path, 0)
	rc = unlinkat_recursive(dirfd, basename);
	PROLOGUE
}

static INT64_T chirp_fs_local_rename(const char *old, const char *new)
{
	PREAMBLE("rename(`%s', `%s')", old, new);
	RESOLVE(old, 0)
	RESOLVE(new, 0)
	rc = renameat(dirfd_old, basename_old, dirfd_new, basename_new);
	dirfd = -1; /* so prologue doesn't close it */
	CLOSE_DIRFD(dirfd_old);
	CLOSE_DIRFD(dirfd_new);
	PROLOGUE
}

static INT64_T chirp_fs_local_link(const char *target, const char *path)
{
	PREAMBLE("link(`%s', `%s')", target, path);
	RESOLVE(target, 0)
	RESOLVE(path, 0)
	rc = linkat(dirfd_target, basename_target, dirfd_path, basename_path, 0);
	dirfd = -1; /* so prologue doesn't close it */
	CLOSE_DIRFD(dirfd_target);
	CLOSE_DIRFD(dirfd_path);
	PROLOGUE
}

static INT64_T chirp_fs_local_symlink(const char *target, const char *path)
{
	PREAMBLE("symlink(`%s', `%s')", target, path);
	RESOLVE(path, 0)
	rc = symlinkat(target, dirfd_path, basename_path);
	PROLOGUE
}

static INT64_T chirp_fs_local_readlink(const char *path, char *buf, INT64_T length)
{
	PREAMBLE("readlink(`%s', %p, %zu)", path, buf, (size_t)length);
	RESOLVE(path, 0)
	rc = readlinkat(dirfd, basename, buf, length);
	PROLOGUE
}

static INT64_T chirp_fs_local_mkdir(const char *path, INT64_T mode)
{
	PREAMBLE("mkdir(`%s', 0o%" PRIo64 ")", path, mode);
	RESOLVE(path, 0)
	mode &= S_IRWXG|S_IRWXO; /* users can only set group/other bits */
	mode |= S_IRWXU;
	rc = mkdirat(dirfd, basename, mode);
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
	RESOLVE(path, 0)
	int empty = 1;

	struct stat info;
	if (fstatat(dirfd, basename, &info, AT_SYMLINK_NOFOLLOW) == 0 && S_ISLNK(info.st_mode)) {
		errno = ENOTDIR;
		rc = -1;
		goto out;
	}

	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		DIR *dir = fdopendir(fd);
		if (dir) {
			struct dirent *d;
			while((d = readdir(dir))) {
				if(strcmp(d->d_name, ".") == 0)
					continue;
				if(strcmp(d->d_name, "..") == 0)
					continue;
				if(strncmp(d->d_name, ".__", 3) == 0)
					continue;
				empty = 0;
				break;
			}

			if(!empty) {
				closedir(dir);
				errno = ENOTEMPTY;
				rc = -1;
				goto out;
			}

			closedir(dir);
			rc = unlinkat_recursive(dirfd, basename);
		} else {
			int s = errno;
			close(fd);
			errno = s;
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
	RESOLVE(path, 1)
	struct stat64 linfo;
	rc = fstatat64(dirfd, basename, &linfo, 0);
	if(rc == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat *info)
{
	PREAMBLE("lstat(`%s', %p)", path, info);
	RESOLVE(path, 0)
	struct stat64 linfo;
	rc = fstatat64(dirfd, basename, &linfo, AT_SYMLINK_NOFOLLOW);
	if(rc == 0)
		COPY_STAT_LOCAL_TO_CHIRP(*info, linfo);
	PROLOGUE
}

static INT64_T chirp_fs_local_statfs(const char *path, struct chirp_statfs *info)
{
	PREAMBLE("statfs(`%s', %p)", path, info);
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
		struct statfs64 linfo;
		rc = fstatfs64(fd, &linfo);
		if (rc == 0)
			COPY_STATFS_LOCAL_TO_CHIRP(*info, linfo);
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_access(const char *path, INT64_T amode)
{
	PREAMBLE("access(`%s', 0x%" PRIx64 ")", path, amode);
	RESOLVE(path, 1)
	rc = faccessat(dirfd, basename, amode, 0);
	PROLOGUE
}

struct chirp_dir {
	DIR *dir;
	struct chirp_dirent cd;
};

static struct chirp_dir *chirp_fs_local_opendir(const char *path)
{
	PREAMBLE("opendir(`%s')", path);
	RESOLVENULL(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_DIRECTORY|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		DIR *dir = fdopendir(fd);
		if (dir) {
			struct chirp_dir *cdir = xxmalloc(sizeof(*cdir));
			cdir->dir = dir;
			debug(D_LOCAL, "= %p", cdir);
			return cdir;
		} else {
			int s = errno;
			close(fd);
			errno = s;
			debug(D_LOCAL, "= NULL (errno = %d; `%s')", errno, strerror(errno));
			return 0;
		}
	} else {
		debug(D_LOCAL, "= NULL (errno = %d; `%s')", errno, strerror(errno));
		return 0;
	}
}

static struct chirp_dirent *chirp_fs_local_readdir(struct chirp_dir *dir)
{
	debug(D_LOCAL, "readdir(%p [%d])", dir, dirfd(dir->dir));
	struct dirent *d = readdir(dir->dir);
	if(d) {
		struct stat64 linfo;
		memset(&dir->cd.info, 0, sizeof(dir->cd.info));
		dir->cd.lstatus = fstatat64(dirfd(dir->dir), d->d_name, &linfo, AT_SYMLINK_NOFOLLOW);
		if(dir->cd.lstatus == 0)
			COPY_STAT_LOCAL_TO_CHIRP(dir->cd.info, linfo);
		dir->cd.name = d->d_name;
		debug(D_LOCAL, "= %p [name = `%s']", &dir->cd, dir->cd.name);
		return &dir->cd;
	} else {
		debug(D_LOCAL, "= NULL (errno = %d; `%s')", errno, strerror(errno));
		return 0;
	}
}

static void chirp_fs_local_closedir(struct chirp_dir *dir)
{
	debug(D_LOCAL, "closedir(%p [`%d'])", dir, dirfd(dir->dir));
	closedir(dir->dir);
	free(dir);
}

static INT64_T chirp_fs_local_chmod(const char *path, INT64_T mode)
{
	PREAMBLE("chmod(`%s', 0o%" PRIo64 ")", path, mode);
	RESOLVE(path, 1)
	mode &= S_IXUSR|S_IRWXG|S_IRWXO; /* users can only set owner execute and group/other bits */
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
		struct stat linfo;
		rc = fstat(fd, &linfo);
		if (rc == 0) {
			if(S_ISDIR(linfo.st_mode)) {
				mode |= S_IRWXU; /* all owner bits must be set */
			} else {
				mode |= S_IRUSR|S_IWUSR; /* owner read/write must be set */
			}
			rc = fchmod(fd, mode);
		}
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_truncate(const char *path, INT64_T length)
{
	PREAMBLE("truncate(`%s', 0d%" PRId64 ")", path, length);
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_WRONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
		rc = ftruncate64(fd, length);
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_utime(const char *path, time_t actime, time_t modtime)
{
	PREAMBLE("utime(`%s', actime = %" PRId64 " modtime = %" PRId64 ")", path, (int64_t)actime, (int64_t)modtime);
	RESOLVE(path, 1)
	struct timespec times[2] = {{.tv_sec = actime}, {.tv_sec = modtime}};
	rc = utimensat(dirfd, basename, times, AT_SYMLINK_NOFOLLOW);
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
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fgetxattr(fd, name, data, size, 0, 0);
#else
		rc = fgetxattr(fd, name, data, size);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
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
	RESOLVE(path, 0)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fgetxattr(fd, name, data, size, 0, 0);
#else
		rc = fgetxattr(fd, name, data, size);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		if (errno == ELOOP)
			errno = ENOTSUP;
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_listxattr(const char *path, char *list, size_t size)
{
	PREAMBLE("listxattr(`%s', %p, %zu)", path, list, size);
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = flistxattr(fd, list, size, 0);
#else
		rc = flistxattr(fd, list, size);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
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
	RESOLVE(path, 0)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = flistxattr(fd, list, size, 0);
#else
		rc = flistxattr(fd, list, size);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		if (errno == ELOOP)
			errno = ENOTSUP;
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_setxattr(const char *path, const char *name, const void *data, size_t size, int flags)
{
	PREAMBLE("setxattr(`%s', `%s', %p, %zu, %d)", path, name, data, size, flags);
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fsetxattr(fd, name, data, size, 0, flags);
#else
		rc = fsetxattr(fd, name, data, size, flags);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
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
	RESOLVE(path, 0)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fsetxattr(fd, name, data, size, 0, flags);
#else
		rc = fsetxattr(fd, name, data, size, flags);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		if (errno == ELOOP)
			errno = ENOTSUP;
		rc = -1;
	}
	PROLOGUE
}

static INT64_T chirp_fs_local_removexattr(const char *path, const char *name)
{
	PREAMBLE("removexattr(`%s', `%s')", path, name);
	RESOLVE(path, 1)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fremovexattr(fd, name, 0);
#else
		rc = fremovexattr(fd, name);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		rc = -1;
	}
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
	RESOLVE(path, 0)
	int fd = openat(dirfd, basename, O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NOCTTY, 0);
	if (fd >= 0) {
		int s;
#ifdef CCTOOLS_OPSYS_DARWIN
		rc = fremovexattr(fd, name, 0);
#else
		rc = fremovexattr(fd, name);
#endif
		s = errno;
		close(fd);
		errno = s;
	} else {
		if (errno == ELOOP)
			errno = ENOTSUP;
		rc = -1;
	}
	PROLOGUE
}
#endif /* defined(HAS_SYS_XATTR_H) || defined(HAS_ATTR_XATTR_H) */

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

	chirp_fs_local_unlink,
	chirp_fs_local_rmall,
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

/* vim: set noexpandtab tabstop=8: */
