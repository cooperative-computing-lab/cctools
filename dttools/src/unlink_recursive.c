/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "unlink_recursive.h"

#include <dirent.h>
#include <unistd.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int unlink_recursive (const char *path)
{
	int rc = unlink(path);
	if(rc == -1 && (errno == EISDIR || errno == EPERM)) {
		DIR *dir = opendir(path);
		if (dir) {
			struct dirent *d;
			rc = 0;
			while(rc == 0 && (d = readdir(dir))) {
				if(strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
					char subpath[PATH_MAX];
					snprintf(subpath, sizeof(subpath), "%s/%s", path, d->d_name);
					rc = unlink_recursive(subpath);
					if (rc == -1) return -1;
				}
			}
			closedir(dir);
			rc = rmdir(path);
		}
	}
	return rc;
}

int unlink_dir_contents (const char *dirname)
{
	DIR *dir = opendir(dirname);
	if(dir) {
		struct dirent *d;
		int rc = 0;
		while (rc == 0 && (d = readdir(dir))) {
			if(strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
				char subpath[PATH_MAX];
				snprintf(subpath, sizeof(subpath), "%s/%s", dirname, d->d_name);
				rc = unlink_recursive(subpath);
				if (rc == -1) return -1;
			}
		}
		closedir(dir);
		return rc;
	} else {
		return -1;
	}
}

/* vim: set noexpandtab tabstop=4: */
