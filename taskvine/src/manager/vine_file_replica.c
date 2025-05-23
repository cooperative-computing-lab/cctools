/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_replica.h"
#include "debug.h"
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
	if (!r)
		return;
	free(r);
	vine_counters.replica.deleted++;
}

int vine_file_replica_change_state(struct vine_file_replica *r, vine_file_replica_state_t new_state)
{
	if (!r) {
		return 0;
	}

	vine_file_replica_state_t old_state = r->state;

	/* if no change, technically a success */
	if (old_state == new_state) {
		return 1;
	}

	/* check allowed transitions */
	int allowed = 0;
	switch (old_state) {
	case VINE_FILE_REPLICA_STATE_PENDING:
		allowed = (new_state == VINE_FILE_REPLICA_STATE_READY ||
				new_state == VINE_FILE_REPLICA_STATE_DELETING ||
				new_state == VINE_FILE_REPLICA_STATE_DELETED);
		break;
	case VINE_FILE_REPLICA_STATE_READY:
		allowed = (new_state == VINE_FILE_REPLICA_STATE_DELETING ||
				new_state == VINE_FILE_REPLICA_STATE_DELETED);
		break;
	case VINE_FILE_REPLICA_STATE_DELETING:
		allowed = (new_state == VINE_FILE_REPLICA_STATE_DELETED ||
				new_state == VINE_FILE_REPLICA_STATE_READY);
		break;
	case VINE_FILE_REPLICA_STATE_DELETED:
		/* can't transition from DELETED to any other state */
		allowed = 0;
		break;
	}

	if (!allowed) {
		debug(D_VINE, "Invalid state transition from %s to %s", old_state == VINE_FILE_REPLICA_STATE_PENDING ? "PENDING" : old_state == VINE_FILE_REPLICA_STATE_READY ? "READY"
														   : old_state == VINE_FILE_REPLICA_STATE_DELETING	      ? "DELETING"
																					      : "DELETED",
				new_state == VINE_FILE_REPLICA_STATE_PENDING ? "PENDING" : new_state == VINE_FILE_REPLICA_STATE_READY ? "READY"
									   : new_state == VINE_FILE_REPLICA_STATE_DELETING	      ? "DELETING"
																      : "DELETED");
		return 0;
	}

	/* change the state */
	r->state = new_state;
	debug(D_VINE, "File replica state transition: %s -> %s", old_state == VINE_FILE_REPLICA_STATE_PENDING ? "PENDING" : old_state == VINE_FILE_REPLICA_STATE_READY ? "READY"
													    : old_state == VINE_FILE_REPLICA_STATE_DELETING	       ? "DELETING"
																				       : "DELETED",
			new_state == VINE_FILE_REPLICA_STATE_PENDING ? "PENDING" : new_state == VINE_FILE_REPLICA_STATE_READY ? "READY"
								   : new_state == VINE_FILE_REPLICA_STATE_DELETING	      ? "DELETING"
															      : "DELETED");
	return 1;
}
