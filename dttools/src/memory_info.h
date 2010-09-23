/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MEMORY_INFO_H
#define MEMORY_INFO_H

#include "int_sizes.h"

/** @file memory_info.h Get current memory status. */

/** Get current memory status.
Uses various techniques on different processors to get
the physical amount of memory installed, and the amount currently available.
@param avail Will be filled in with the memory currently available, measured in bytes.
@param total Will be filled in with the memory physically installed, measured in bytes.
@return One on success, zero on failure. 
*/
int memory_info_get( UINT64_T *avail, UINT64_T *total );

/** Get current memory usage by this process.
@param rss Will be filled in with the current resident memory usage of this process, in bytes.
@param total Will be filled in with the total virtual memory size of this process, in bytes.
*/

int memory_usage_get( UINT64_T *rss, UINT64_T *total );

#endif
