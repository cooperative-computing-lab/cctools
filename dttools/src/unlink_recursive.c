/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "unlink_recursive.h"
#include "debug.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef O_CLOEXEC
#	define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#	define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#	define O_NOFOLLOW 0
#endif

int unlinkat_recursive (int dirfd, const char *path)
{
	int rc = unlinkat(dirfd, path, 0);
	if(rc<0 && errno==ENOENT) {
		/* If it doesn't exist, return success. */
		return 0;
	} else if(rc<0 && (errno == EISDIR || errno == EPERM || errno == ENOTEMPTY)) {
		int subdirfd = openat(dirfd, path, O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOCTTY|O_NOFOLLOW, 0);
		if (subdirfd >= 0) {
			DIR *dir = fdopendir(subdirfd);
			if(dir) {
				struct dirent *d;
				while((d = readdir(dir))) {
					if(!(strcmp(d->d_name, ".") == 0 || strcmp(d->d_name, "..") == 0)) {
						assert(strchr(d->d_name, '/') == NULL);
						/* On failure, just keep going so as to remove as much as possible. */
						unlinkat_recursive(subdirfd, d->d_name);
					}
				}
				closedir(dir);
			}
			close(subdirfd);

			/* If there was an interior failure, then this will also fail. */
			rc = unlinkat(dirfd, path, AT_REMOVEDIR);
			if(rc<0) warn(D_ERROR,"couldn't delete directory %s: %s\n",path,strerror(errno));
			return rc;
		} else {
			return -1;
		}
	} else {
		if(rc<0) warn(D_ERROR,"couldn't delete %s: %s\n",path,strerror(errno));
		return rc;
	}
}

int unlink_recursive (const char *path)
{
	return unlinkat_recursive(AT_FDCWD, path);
}

int unlink_dir_contents (const char *path)
{
	int dirfd = openat(AT_FDCWD, path, O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOCTTY, 0);
	if (dirfd >= 0) {
		int rcs = 0;
		DIR *dir = fdopendir(dirfd);
		if (dir) {
			struct dirent *d;
			while((d = readdir(dir))) {
				if(!(strcmp(d->d_name, ".") == 0 || strcmp(d->d_name, "..") == 0)) {
					assert(strchr(d->d_name, '/') == NULL);
					rcs |= unlinkat_recursive(dirfd, d->d_name);
				}
			}
			closedir(dir);
		} else {
			rcs = -1;
		}

		close(dirfd);
		return rcs != 0 ? -1 : 0;
	}

	return -1;
}

/* vim: set noexpandtab tabstop=8: */
