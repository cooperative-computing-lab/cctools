/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "sort_dir.h"
#include "string_array.h"

#include <dirent.h>
#include <stdlib.h>
#include <string.h>

typedef int (*sort_dir_compare_t)(const char *a, const char *b);

static sort_dir_compare_t sort_dir_compare = strcmp;

static int sort_dir_qsort_compare(const void *a, const void *b)
{
	/* qsort passes pointers to the array elements (char **),
	 * not the strings themselves. */
	return sort_dir_compare(*(const char *const *)a, *(const char *const *)b);
}

int sort_dir(const char *dirname, char ***list, int (*sort)(const char *a, const char *b))
{
	DIR *dir;
	size_t n = 0;

	*list = string_array_new();

	dir = opendir(dirname);
	if (dir) {
		struct dirent *d;

		while ((d = readdir(dir))) {
			*list = string_array_append(*list, d->d_name);
			n += 1;
		}
		closedir(dir);
	} else {
		return 0;
	}

	sort_dir_compare = sort ? sort : strcmp;
	qsort(*list, n, sizeof(char *), sort_dir_qsort_compare);

	return 1;
}

void sort_dir_free(char **list)
{
	free(list);
}

/* vim: set noexpandtab tabstop=8: */
