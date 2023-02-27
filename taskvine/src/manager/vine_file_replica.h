/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_REMOTE_FILE_INFO_H
#define VINE_REMOTE_FILE_INFO_H

#include "taskvine.h"

struct vine_file_replica {
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
	uint8_t           in_cache;
};

struct vine_file_replica * vine_file_replica_create( int64_t size, time_t mtime );
void vine_file_replica_delete( struct vine_file_replica *rinfo );

#endif

