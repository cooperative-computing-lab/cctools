/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#if !defined(HAS_OPENAT) || !defined(HAS_UTIMENSAT)

#include "compat-at.h"

#include "debug.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

static int getfullpath (int dirfd, const char *path, char fullpath[PATH_MAX])
{
	char dirpath[PATH_MAX] = ".";
	if (path[0] == '/') {
		dirpath[0] = 0;
	} else if (dirfd != AT_FDCWD) {
#if defined(CCTOOLS_OPSYS_DARWIN)
		if (fcntl(dirfd, F_GETPATH, dirpath) == -1)
			return -1;
#elif defined(CCTOOLS_OPSYS_LINUX)
		char dirpath[PATH_MAX];
		char procpath[PATH_MAX];
		snprintf(procpath, PATH_MAX, "/proc/self/fd/%d", dirfd);
		if (readlink(procpath, dirpath, PATH_MAX) == -1)
			return -1;
#else
#	error "Cannot proceed without support for *at system calls."
#endif
	}
	int rc = snprintf(fullpath, PATH_MAX, "%s/%s", dirpath, path);
	if (rc == -1)
		abort();
	else if (rc >= PATH_MAX)
		return (errno = ENAMETOOLONG, -1);
	debug(D_DEBUG, "full path %s", fullpath);
	return 0;
}

#ifndef HAS_OPENAT
int cctools_faccessat (int dirfd, const char *path, int amode, int flag)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	(void)flag;
	return access(fullpath, amode);
}

int cctools_fchmodat (int dirfd, const char *path, mode_t mode, int flag)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	(void)flag;
	return chmod(fullpath, mode);
}

DIR *cctools_fdopendir (int dirfd)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, ".", fullpath) == -1)
		return NULL;
	return opendir(fullpath);
}

int cctools_fstatat (int dirfd, const char *path, struct stat *buf, int flag)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	if (flag & AT_SYMLINK_NOFOLLOW) {
		return lstat(fullpath, buf);
	} else {
		return stat(fullpath, buf);
	}
}

int cctools_linkat (int dirfd, const char *path, int newdirfd, const char *newpath, int flag)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	char newfullpath[PATH_MAX];
	if (getfullpath(newdirfd, newpath, newfullpath) == -1)
		return -1;
	(void)flag;
	return link(fullpath, newfullpath);
}

int cctools_openat (int dirfd, const char *path, int oflag, mode_t cmode)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	return open(fullpath, oflag, cmode);
}

int cctools_mkdirat (int dirfd, const char *path, mode_t mode)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	return mkdir(fullpath, mode);
}

int cctools_readlinkat (int dirfd, const char *path, char *buf, size_t bufsize)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	return readlink(fullpath, buf, bufsize);
}

int cctools_renameat (int dirfd, const char *path, int newdirfd, const char *newpath)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	char newfullpath[PATH_MAX];
	if (getfullpath(newdirfd, newpath, newfullpath) == -1)
		return -1;
	return rename(fullpath, newfullpath);
}

int cctools_symlinkat (const char *target, int dirfd, const char *path)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	return symlink(target, fullpath);
}

int cctools_unlinkat (int dirfd, const char *path, int flag)
{
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	if (flag == AT_REMOVEDIR) {
		return rmdir(fullpath);
	} else {
		return unlink(fullpath);
	}
}
#endif /* HAS_OPENAT */

#ifndef HAS_UTIMENSAT
int cctools_utimensat (int dirfd, const char *path, const struct timespec times[2], int flag)
{
	struct timeval tv[2] = {{.tv_sec = times[0].tv_sec}, {.tv_sec = times[1].tv_sec}};
	char fullpath[PATH_MAX];
	if (getfullpath(dirfd, path, fullpath) == -1)
		return -1;
	(void)flag;
	return utimes(fullpath, tv);
}
#endif /* HAS_UTIMENSAT */

#endif /* !defined(HAS_OPENAT) || !defined(HAS_UTIMENSAT) */

/* vim: set noexpandtab tabstop=8: */
