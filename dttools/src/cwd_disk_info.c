/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cwd_disk_info.h"

#include <limits.h>
#include <ftw.h>

#include "debug.h"

/* Use at most 128 file descriptors before reusing them. */
/* Hierarchies deeper than that will produce a hit in performance. */
#define MAX_FILE_DESCRIPTORS 128

static int64_t total_usage = 0;

static int update_cwd_usage(const char *path, const struct stat *s, int typeflag, struct FTW *f)
{
	switch(typeflag)
	{
		case FTW_F:
			total_usage += s->st_size;
			break;
		default:
			break;
	}

	return 0;
}

int cwd_disk_info_get(const char *path, int64_t *total)
{
	total_usage = 0;

	int result = nftw(path, update_cwd_usage, MAX_FILE_DESCRIPTORS, FTW_PHYS);

	if(result != 0) {
		debug(D_DEBUG, "error reading %s disk usage.\n", path);
		result = 0;
	}

	/* B to MB */
	*total = total_usage/(1024 * 1024);

	return result;
}

