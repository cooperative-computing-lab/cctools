/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "mkdir_recursive.h"

#include "catch.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

int mkdirat_recursive (int fd, const char *path, mode_t mode)
{
	int rc;
	size_t i;

	if (strlen(path) >= PATH_MAX)
		CATCH(ENAMETOOLONG);

	for (i = strspn(path, "/"); path[i]; i += strspn(path+i, "/")) {
		char subpath[PATH_MAX] = "";
		size_t nextdelim = strcspn(path+i, "/");

		assert(i+nextdelim < PATH_MAX);
		memcpy(subpath, path, i+nextdelim);

		rc = mkdirat(fd, subpath, mode);
		if (rc == -1) {
			if (errno == EEXIST) {
				struct stat info;
				CATCHUNIX(fstatat(fd, subpath, &info, 0));
				if (!S_ISDIR(info.st_mode))
					CATCH(ENOTDIR);
			} else {
				CATCH(errno);
			}
		}

		i += nextdelim;
	}

	rc = 0;
	goto out;
out:
	return RCUNIX(rc);
}

int mkdir_recursive (const char *path, mode_t mode)
{
	return mkdirat_recursive(AT_FDCWD, path, mode);
}

int mkdirat_recursive_parents (int fd, const char *path, mode_t mode)
{
	int rc;
	char parent[PATH_MAX] = "";
	char *slash;

	if (strlen(path) >= PATH_MAX)
		CATCH(ENAMETOOLONG);

	strcpy(parent, path);
	slash = strrchr(parent+1, '/');
	if (slash) {
		*slash = 0;
		CATCHUNIX(mkdirat_recursive(fd, parent, mode));
	}

	rc = 0;
	goto out;
out:
	return RCUNIX(rc);
}

int mkdir_recursive_parents (const char *path, mode_t mode)
{
	return mkdirat_recursive_parents(AT_FDCWD, path, mode);
}

/* vim: set noexpandtab tabstop=8: */
