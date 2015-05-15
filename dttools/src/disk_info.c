/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "disk_info.h"
#include "cwd_disk_info.h"
#include "debug.h"
#include "macros.h"

#include <time.h>

#include <sys/types.h>
#include <sys/param.h>
#include <sys/mount.h>

#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif

#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#ifdef HAS_SYS_VFS_H
#include <sys/vfs.h>
#endif

int disk_info_get(const char *path, UINT64_T * avail, UINT64_T * total)
{
#ifdef CCTOOLS_OPSYS_SUNOS
	int result;
	struct statvfs s;

	result = statvfs(path, &s);
	if(result < 0)
		return result;

	*total = ((UINT64_T) s.f_bsize) * s.f_blocks;
	*avail = ((UINT64_T) s.f_bsize) * s.f_bfree;

	return 0;
#else
	int result;
	struct statfs s;

	result = statfs(path, &s);
	if(result < 0)
		return result;

	*total = ((UINT64_T) s.f_bsize) * s.f_blocks;
	*avail = ((UINT64_T) s.f_bsize) * s.f_bavail;

	return 0;
#endif
}

/* Slower disk check, poor man's du on workspace */
int check_disk_workspace(char *workspace, int64_t *workspace_usage, int force, int64_t manual_disk_option, int measure_wd_interval, time_t last_cwd_measure_time, int64_t last_workspace_usage, UINT64_T disk_avail_threshold) {

    if(manual_disk_option < 1)
        return 1;

    if( force || (time(0) - last_cwd_measure_time) >= measure_wd_interval ) {
        cwd_disk_info_get(workspace, &last_workspace_usage);
        debug(D_DEBUG, "worker disk usage: %" PRId64 "\n", last_workspace_usage);
        last_cwd_measure_time = time(0);
    }

    if(workspace_usage) {
        *workspace_usage = last_workspace_usage;
    }

	// Use threshold only if smaller than specified disk size.
    int64_t disk_limit = manual_disk_option - disk_avail_threshold;
    if(disk_limit < 0)
        disk_limit = manual_disk_option;

    if(last_workspace_usage > disk_limit) {
        debug(D_DEBUG, "worker disk usage %"PRId64 " larger than: %" PRId64 "!\n", last_workspace_usage + disk_avail_threshold, manual_disk_option);
        return 0;
    } else {
        return 1;
    }
}

int check_disk_space_for_filesize(char *path, INT64_T file_size, UINT64_T disk_avail_threshold) {
    uint64_t disk_avail, disk_total;

    if(disk_avail_threshold > 0) {
        disk_info_get(path, &disk_avail, &disk_total);
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



/* vim: set noexpandtab tabstop=4: */
