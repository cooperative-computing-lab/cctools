/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef COMPAT_AT_H
#define COMPAT_AT_H

/** @file compat-at.h at syscall compatibility layer.
*/

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <limits.h>

#ifndef AT_FDCWD
#	define AT_FDCWD -100
#endif
#ifndef AT_SYMLINK_NOFOLLOW
#	define AT_SYMLINK_NOFOLLOW 1
#endif
#ifndef AT_REMOVEDIR
#	define AT_REMOVEDIR 2
#endif

#ifndef HAS_OPENAT
	int cctools_faccessat (int dirfd, const char *path, int amode, int flag);
	int cctools_fchmodat (int dirfd, const char *path, mode_t mode, int flag);
	DIR *cctools_fdopendir (int dirfd);
	int cctools_fstatat (int dirfd, const char *path, struct stat *buf, int flag);
	int cctools_linkat (int dirfd, const char *path, int newdirfd, const char *newpath, int flag);
	int cctools_mkdirat (int dirfd, const char *path, mode_t mode);
	int cctools_openat (int dirfd, const char *path, int oflag, mode_t cmode);
	int cctools_readlinkat (int dirfd, const char *path, char *buf, size_t bufsize);
	int cctools_renameat (int dirfd, const char *path, int newdirfd, const char *newpath);
	int cctools_symlinkat (const char *target, int dirfd, const char *path);
	int cctools_unlinkat (int dirfd, const char *path, int flag);
#	define faccessat  cctools_faccessat
#	define fchmodat   cctools_fchmodat
#	define fdopendir  cctools_fdopendir
#	define fstatat    cctools_fstatat
#	define linkat     cctools_linkat
#	define mkdirat    cctools_mkdirat
#	define openat     cctools_openat
#	define readlinkat cctools_readlinkat
#	define renameat   cctools_renameat
#	define symlinkat  cctools_symlinkat
#	define unlinkat   cctools_unlinkat
#endif /* HAS_OPENAT */

/* utimensat added in Linux 2.6.22 */
#ifndef HAS_UTIMENSAT
	int cctools_utimensat (int dirfd, const char *path, const struct timespec times[2], int flag);
#	define utimensat  cctools_utimensat
#endif

#endif /* COMPAT_AT_H */

/* vim: set noexpandtab tabstop=8: */
