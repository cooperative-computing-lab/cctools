/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_replica.h"
#include "vine_counters.h"

struct vine_file_replica *vine_file_replica_create(vine_file_type_t type, vine_cache_level_t cache_level, int64_t size, time_t mtime)
{
	struct vine_file_replica *r = malloc(sizeof(*r));
	r->type = type;
	r->cache_level = cache_level;
	r->size = size;
	r->mtime = mtime;
	r->transfer_time = 0;
	r->last_failure_time = 0;
	r->state = VINE_FILE_REPLICA_STATE_PENDING;

	vine_counters.replica.created++;
	return r;
}

void vine_file_replica_delete(struct vine_file_replica *r)
{
	if (!r) {
		return;
	}

	free(r);
	vine_counters.replica.deleted++;
}