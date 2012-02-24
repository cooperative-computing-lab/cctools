/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "delete_dir.h"
#include "stringtools.h"

#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

int delete_dir(const char *dirname)
{
	char subdir[PATH_MAX];
	int result;
	struct dirent *d;
	DIR *dir;

	dir = opendir(dirname);
	if(!dir) {
		if(errno == ENOTDIR) {
			result = unlink(dirname);
			if(result == 0) {
				return 1;
			} else {
				return 0;
			}
		} else if(errno == ENOENT) {
			return 1;
		} else {
			return 0;
		}
	}

	result = 1;

	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, "."))
			continue;
		if(!strcmp(d->d_name, ".."))
			continue;
		sprintf(subdir, "%s/%s", dirname, d->d_name);
		if(!delete_dir(subdir)) {
			result = 0;
		}
	}

	closedir(dir);

	if(rmdir(dirname) != 0) {
		result = 0;
	}

	return result;
}
