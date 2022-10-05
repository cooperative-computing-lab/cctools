/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_REMOTE_FILE_INFO_H
#define DS_REMOTE_FILE_INFO_H

#include "taskvine.h"

struct ds_remote_file_info {
	ds_file_t type;
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
};

struct ds_remote_file_info * ds_remote_file_info_create( ds_file_t type, int64_t size, time_t mtime );
void ds_remote_file_info_delete( struct ds_remote_file_info *rinfo );

#endif

