/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CWD_INFO_H
#define CWD_INFO_H

#include "int_sizes.h"

/** @file path_disk_size_info.h
Query disk space on the given directory.
*/

/** Get the total disk usage on path.
@param path Directory to be measured.
@param total A pointer to an integer that will be filled with the total space in bytes.
@return Greater than or equal to zero on success, less than zero otherwise.
*/
int path_disk_size_info_get(const char *path, int64_t * total);

#endif
