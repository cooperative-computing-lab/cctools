/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "sort_dir.h"
#include "string_array.h"

#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

int sort_dir(const char *dirname, char ***list, int (*sort) (const char *a, const char *b))
{
	DIR *dir;
	size_t n = 0;

	*list = string_array_new();

	dir = opendir(dirname);
	if(dir) {
		struct dirent *d;

		while((d = readdir(dir))) {
			*list = string_array_append(*list, d->d_name);
			n += 1;
		}
		closedir(dir);
	}

	if(sort) {
		qsort(*list, n, sizeof(char *), (int (*)(const void *, const void *)) sort);
	}

	return 1;
}

/* vim: set noexpandtab tabstop=4: */
