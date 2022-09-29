/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MEMORY_INFO_H
#define MEMORY_INFO_H

#include <stdint.h>

/** @file host_memory_info.h Get current memory status. */

/** Get current memory status.
Uses various techniques on different processors to get
the physical amount of memory installed, and the amount currently available.
@param avail Will be filled in with the memory currently available, measured in bytes.
@param total Will be filled in with the memory physically installed, measured in bytes.
@return One on success, zero on failure.
*/
int host_memory_info_get(uint64_t * avail, uint64_t * total);

/** Get current memory usage by this process.
@param rss Will be filled in with the current resident memory usage of this process, in bytes.
@param total Will be filled in with the total virtual memory size of this process, in bytes.
*/

int host_memory_usage_get(uint64_t * rss, uint64_t * total);

#endif
