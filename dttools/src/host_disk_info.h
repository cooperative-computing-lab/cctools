/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DISK_INFO_H
#define DISK_INFO_H

#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif

#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#ifdef HAS_SYS_VFS_H
#include <sys/vfs.h>
#endif


#include "int_sizes.h"
#include <time.h>

/** @file host_disk_info.h
Query disk space properties.
*/

/** Get the total and available space on a disk.
@param path A filename on the disk to be examined.
@param avail A pointer to an integer that will be filled with the available space in bytes.
@param total A pointer to an integer that will be filled with the total space in bytes.
@return Greater than or equal to zero on success, less than zero otherwise.
*/
int host_disk_info_get(const char *path, UINT64_T * avail, UINT64_T * total);

/** Return whether a file will fit in the given directory.
@param path A filename of the disk to be measured.
@param file_size An integer that describes how large the incoming file is.
@param disk_avail_threshold An unsigned integer that describes the minimum available space to leave.
@return 0 if the file will not fit, 1 if the file fits.
*/
int check_disk_space_for_filesize(char *path, INT64_T file_size, UINT64_T disk_avail_threshold);

#endif
