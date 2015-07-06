/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DISK_INFO_H
#define DISK_INFO_H

#include "int_sizes.h"
#include <time.h>

/** @file disk_info.h
Query disk space properties.
*/

/** Get the total and available space on a disk.
@param path A filename on the disk to be examined.
@param avail A pointer to an integer that will be filled with the available space in bytes.
@param total A pointer to an integer that will be filled with the total space in bytes.
@return Greater than or equal to zero on success, less than zero otherwise.
*/
int disk_info_get(const char *path, UINT64_T * avail, UINT64_T * total);

/** Return whether a file will fit in the given directory.
@param workspace_usage A pointer to an integer that will be filled with the workspace usage.
@param force An integer that describes if the action is to be forced.
@param manual_disk_option An integer that describes static manual_disk_option in worker.
@param measure_wq_interval An integer that describes how often the cwd should be rechecked.
@param last_cwd_measure_time An time value that describes how recently cwd was checked.
@param last_workspace_usage An integer that describes previous reading from check_disk_workspace.
@param disk_avail_threshold An unsigned integer that describes the lowest amount of free space to be left.
@return Zero if the file will not fit, one if the file fits.
*/
int check_disk_workspace(char *workspace, int64_t *workspace_usage, int force, int64_t manual_disk_option, int measure_wd_interval, time_t *last_cwd_measure_time, int64_t *last_workspace_usage, UINT64_T disk_avail_threshold);


/** Return whether a file will fit in the given directory.
@param path A filename of the disk to be measured.
@param file_size An integer that describes how large the incoming file is.
@param disk_avail_threshold An unsigned integer that describes the minimum available space to leave.
@return Zero if the file will not fit, one if the file fits.
*/
int check_disk_space_for_filesize(char *path, INT64_T file_size, UINT64_T disk_avail_threshold);

#endif
