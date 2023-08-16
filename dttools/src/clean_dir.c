/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "clean_dir.h"
#include "stringtools.h"

#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int clean_dir(const char *dirname, const char *delete_pattern)
{
	char subdir[PATH_MAX];
	struct dirent *d;
	DIR *dir;

	dir = opendir(dirname);
	if(!dir)
		return 0;

	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, "."))
			continue;
		if(!strcmp(d->d_name, ".."))
			continue;
		sprintf(subdir, "%s/%s", dirname, d->d_name);
		clean_dir(subdir, delete_pattern);
		if(string_match(delete_pattern, d->d_name)) {
			unlink(d->d_name);
		}
	}

	closedir(dir);

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
