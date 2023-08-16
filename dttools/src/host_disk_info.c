/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "host_disk_info.h"
#include "path_disk_size_info.h"
#include "debug.h"
#include "macros.h"

#include <time.h>

#include <sys/types.h>
#include <sys/param.h>
#include <sys/mount.h>

int host_disk_info_get(const char *path, UINT64_T * avail, UINT64_T * total)
{
	int result;
	struct statfs s;

	result = statfs(path, &s);
	if(result < 0)
		return result;

	*total = ((UINT64_T) s.f_bsize) * s.f_blocks;
	*avail = ((UINT64_T) s.f_bsize) * s.f_bavail;

	return 0;
}

int check_disk_space_for_filesize(char *path, int64_t file_size, uint64_t disk_avail_threshold) {
	uint64_t disk_avail, disk_total;

	if(disk_avail_threshold > 0) {
		host_disk_info_get(path, &disk_avail, &disk_total);
		if(file_size > 0) {
			if((uint64_t)file_size > disk_avail || (disk_avail - file_size) < disk_avail_threshold) {
				debug(D_DEBUG, "File of size %"PRId64" MB will lower available disk space (%"PRIu64" MB) below threshold (%"PRIu64" MB).\n", file_size/MEGA, disk_avail/MEGA, disk_avail_threshold/MEGA);
				return 0;
			}
		} else {
			if(disk_avail < disk_avail_threshold) {
				debug(D_DEBUG, "Available disk space (%"PRIu64" MB) lower than threshold (%"PRIu64" MB).\n", disk_avail/MEGA, disk_avail_threshold/MEGA);
				return 0;
			}
		}
	}

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
