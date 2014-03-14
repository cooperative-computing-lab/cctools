/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_fs_local_scheduler.h"
#include "chirp_protocol.h"
#include "chirp_acl.h"

#include "debug.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "int_sizes.h"
#include "path.h"
#include "full_io.h"
#include "delete_dir.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <fnmatch.h>
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

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
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

static INT64_T chirp_fs_local_fstat(int fd, struct chirp_stat *buf)
{
	SETUP_FILE
	struct stat64 info;
	int result;
	result = fstat64(lfd, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	buf->cst_mode = buf->cst_mode & (~0077);
	return result;
}

static INT64_T chirp_fs_local_fstatfs(int fd, struct chirp_statfs *buf)
{
	SETUP_FILE
	struct statfs64 info;
	int result;
	result = fstatfs64(lfd, &info);
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

static INT64_T chirp_fs_local_stat(const char *path, struct chirp_stat *buf)
{
	struct stat64 info;
	int result;
	RESOLVE(path)
	result = stat64(path, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	return result;
}

static INT64_T chirp_fs_local_lstat(const char *path, struct chirp_stat *buf)
{
	struct stat64 info;
	int result;
	RESOLVE(path)
	result = lstat64(path, &info);
	if(result == 0)
		COPY_CSTAT(info, *buf);
	return result;
}

static INT64_T chirp_fs_local_statfs(const char *path, struct chirp_statfs *buf)
{
	struct statfs64 info;
	int result;
	RESOLVE(path)
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
		struct stat64 buf;
		dir->cd.name = d->d_name;
		sprintf(path, "%s/%s", dir->path, dir->cd.name);
		memset(&dir->cd.info, 0, sizeof(dir->cd.info));
		dir->cd.lstatus = lstat64(path, &buf);
		if(dir->cd.lstatus == 0) {
			COPY_CSTAT(buf, dir->cd.info);
		}
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


static int search_to_access(int flags)
{
	int access_flags = F_OK;
	if(flags & CHIRP_SEARCH_R_OK)
		access_flags |= R_OK;
	if(flags & CHIRP_SEARCH_W_OK)
		access_flags |= W_OK;
	if(flags & CHIRP_SEARCH_X_OK)
		access_flags |= X_OK;

	return access_flags;
}

static int search_match_file(const char *pattern, const char *name)
{
	debug(D_DEBUG, "search_match_file(`%s', `%s')", pattern, name);
	/* Decompose the pattern in atoms which are each matched against. */
	while (1) {
		char atom[CHIRP_PATH_MAX];
		const char *end = strchr(pattern, '|'); /* disjunction operator */

		memset(atom, 0, sizeof(atom));
		if (end)
			strncpy(atom, pattern, (size_t)(end-pattern));
		else
			strcpy(atom, pattern);

		/* Here we might have a pattern like '*' which matches any file so we
		 * iteratively pull leading components off of `name' until we get a
		 * match.  In the case of '*', we would pull off all leading components
		 * until we reach the file name, which would always match '*'.
		 */
		const char *test = name;
		do {
			int result = fnmatch(atom, test, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, test, result);
			if(result == 0) {
				return 1;
			}
			test = strchr(test, '/');
			if (test) test += 1;
		} while (test);

		if (end)
			pattern = end+1;
		else
			break;
	}

	return 0;
}

static int search_should_recurse(const char *base, const char *pattern)
{
	debug(D_DEBUG, "search_should_recurse(base = `%s', pattern = `%s')", base, pattern);
	/* Decompose the pattern in atoms which are each matched against. */
	while (1) {
		char atom[CHIRP_PATH_MAX];

		if (*pattern != '/') return 1; /* unanchored pattern is always recursive */

		const char *end = strchr(pattern, '|'); /* disjunction operator */
		memset(atom, 0, sizeof(atom));
		if (end)
			strncpy(atom, pattern, (size_t)(end-pattern));
		else
			strcpy(atom, pattern);

		/* Here we want to determine if `base' matches earlier parts of
		 * `pattern' to see if we should recurse in the directory `base'. To do
		 * this, we strip off final parts of `pattern' until we get a match.
		 */
		while (*atom) {
			int result = fnmatch(atom, base, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, base, result);
			if(result == 0) {
				return 1;
			}
			char *last = strrchr(atom, '/');
			if (last) {
				*last = '\0';
			} else {
				break;
			}
		}

		if (end)
			pattern = end+1;
		else
			break;
	}
	return 0;
}

static int search_directory(const char *subject, const char * const base, char fullpath[CHIRP_PATH_MAX], const char *pattern, int flags, struct link *l, time_t stoptime)
{
	if(strlen(pattern) == 0)
		return 0;

	debug(D_DEBUG, "search_directory(subject = `%s', base = `%s', fullpath = `%s', pattern = `%s', flags = %d, ...)", subject, base, fullpath, pattern, flags);

	int access_flags = search_to_access(flags);
	int includeroot = flags & CHIRP_SEARCH_INCLUDEROOT;
	int metadata = flags & CHIRP_SEARCH_METADATA;
	int stopatfirst = flags & CHIRP_SEARCH_STOPATFIRST;

	int result = 0;
	void *dirp = chirp_fs_local_opendir(fullpath);
	char *current = fullpath + strlen(fullpath);	/* point to end to current directory */

	if(dirp) {
		errno = 0;
		struct chirp_dirent *entry;
		while((entry = chirp_fs_local_readdir(dirp))) {
			char *name = entry->name;

			if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strncmp(name, ".__", 3) == 0)
				continue;
			sprintf(current, "/%s", name);

			if(search_match_file(pattern, base)) {
				const char *matched;
				if (includeroot) {
					if (base-fullpath == 1 && fullpath[0] == '/') {
						matched = base;
					} else {
						matched = fullpath;
					}
				} else {
					matched = base;
				}

				result += 1;
				if (access_flags == F_OK || chirp_fs_local_access(fullpath, access_flags) == 0) {
					if(metadata) {
						/* A match was found, but the matched file couldn't be statted. Generate a result and an error. */
						struct chirp_stat buf;
						if(entry->lstatus == -1) {
							link_putfstring(l, "0:%s::\n", stoptime, matched); // FIXME is this a bug?
							link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_STAT, matched);
						} else {
							char statenc[CHIRP_STAT_MAXENCODING];
							chirp_stat_encode(statenc, &buf);
							link_putfstring(l, "0:%s:%s:\n", stoptime, matched, statenc);
							if(stopatfirst) return 1;
						}
					} else {
						link_putfstring(l, "0:%s::\n", stoptime, matched);
						if(stopatfirst) return 1;
					}
				} /* FIXME access failure */
			}

			if(cfs_isdir(fullpath) && search_should_recurse(base, pattern)) {
				if(chirp_acl_check_dir(fullpath, subject, CHIRP_ACL_LIST)) {
					int n = search_directory(subject, base, fullpath, pattern, flags, l, stoptime);
					if(n > 0) {
						result += n;
						if(stopatfirst)
							return result;
					}
				} else {
					link_putfstring(l, "%d:%d:%s:\n", stoptime, EPERM, CHIRP_SEARCH_ERR_OPEN, fullpath);
				}
			}
			*current = '\0';	/* clear current entry */
			errno = 0;
		}

		if(errno)
			link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_READ, fullpath);

		errno = 0;
		chirp_fs_local_closedir(dirp);
		if(errno)
			link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_CLOSE, fullpath);
	} else {
		link_putfstring(l, "%d:%d:%s:\n", stoptime, errno, CHIRP_SEARCH_ERR_OPEN, fullpath);
	}

	return result;
}

/* Note we need the subject because we must check the ACL for any nested directories. */
static INT64_T chirp_fs_local_search(const char *subject, const char *dir, const char *pattern, int flags, struct link *l, time_t stoptime)
{
	char fullpath[CHIRP_PATH_MAX];
	strcpy(fullpath, dir);
	path_remove_trailing_slashes(fullpath); /* this prevents double slashes from appearing in paths we examine. */

	debug(D_DEBUG, "chirp_fs_local_search(subject = `%s', dir = `%s', pattern = `%s', flags = %d, ...)", subject, dir, pattern, flags);

	/* FIXME we should still check for literal paths to search since we can optimize that */
	return search_directory(subject, fullpath + strlen(fullpath), fullpath, pattern, flags, l, stoptime);
}

static INT64_T chirp_fs_local_chmod(const char *path, INT64_T mode)
{
	struct stat buf;
	RESOLVE(path)

	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.

	if (stat(path, &buf) == -1)
		return -1;
	if(S_ISDIR(buf.st_mode)) {
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

	chirp_fs_local_search,

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
