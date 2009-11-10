/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DISK_INFO_H
#define DISK_INFO_H

#include "int_sizes.h"

/** @file disk_info.h
Query disk space properties.
*/

/** Get the total and available space on a disk.
@param path A filename on the disk to be examined.
@param avail A pointer to an integer that will be filled with the available space in bytes.
@param total A pointer to an integer that will be filled with the total space in bytes.
@return Greater than or equal to zero on success, less than zero otherwise.
*/
int disk_info_get( const char *path, UINT64_T *avail, UINT64_T *total );

#endif
