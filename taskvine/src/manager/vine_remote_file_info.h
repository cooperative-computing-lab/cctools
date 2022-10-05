/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_REMOTE_FILE_INFO_H
#define VINE_REMOTE_FILE_INFO_H

#include "taskvine.h"

struct vine_remote_file_info {
	vine_file_t type;
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
};

struct vine_remote_file_info * vine_remote_file_info_create( vine_file_t type, int64_t size, time_t mtime );
void vine_remote_file_info_delete( struct vine_remote_file_info *rinfo );

#endif

