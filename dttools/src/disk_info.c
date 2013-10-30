/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "disk_info.h"

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

/* vim: set noexpandtab tabstop=4: */
