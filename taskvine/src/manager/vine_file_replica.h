/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_FILE_REPLICA_H
#define VINE_FILE_REPLICA_H

#include "taskvine.h"

typedef enum {
	VINE_FILE_REPLICA_STATE_PENDING,  // The replica is in the process of being transferred/created.
	VINE_FILE_REPLICA_STATE_READY,    // The replica exists and is ready to be used.
	VINE_FILE_REPLICA_STATE_DELETING, // The replica is in the process of being deleted.
	VINE_FILE_REPLICA_STATE_DELETED,  // The replica has been deleted.
} vine_file_replica_state_t;

struct vine_file_replica {
	vine_file_type_t  type;
	vine_cache_level_t cache_level;
	int64_t           size;
	time_t            mtime;
	timestamp_t       transfer_time;
	timestamp_t       last_failure_time;
	vine_file_replica_state_t state;
};

struct vine_file_replica * vine_file_replica_create(vine_file_type_t type, vine_cache_level_t cache_level, int64_t size, time_t mtime);

void vine_file_replica_delete(struct vine_file_replica *r);

#endif

