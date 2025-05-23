/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_replica.h"
#include "debug.h"
#include "vine_counters.h"

/* === File Replica State Machine:
PENDING ->  READY             : receive "cache-update" — file has physically arrived on the worker
PENDING ->  DELETING          : receive "cache-invalid" — file was expected but not found on the worker
PENDING ->  DELETED           : worker disconnect — file was never ready and worker is now unreachable

READY   ->  DELETING          : send "unlink" — manager initiates deletion of a valid file
READY   ->  DELETED           : worker disconnect — file was on a worker that has now disconnected

DELETING -> DELETED           : receive "unlink-complete" — worker confirms deletion
DELETING -> DELETED           : worker disconnect — deletion in progress, but worker is now unreachable

DELETED  -> (no transition)   : terminal state,
*/
static const int vine_file_replica_allowed_state_transitions[4][4] = {
		// From/To:   PENDING  READY  DELETING   DELETED
		/* PENDING   */ {0, 1, 1, 1},
		/* READY     */ {0, 0, 1, 1},
		/* DELETING  */ {0, 0, 0, 1},
		/* DELETED   */ {0, 0, 0, 0}};

static int vine_file_replica_is_state_transition_allowed(vine_file_replica_state_t from, vine_file_replica_state_t to)
{
	return vine_file_replica_allowed_state_transitions[from][to];
}

static const char *vine_file_replica_state_to_string(vine_file_replica_state_t state)
{
	switch (state) {
	case VINE_FILE_REPLICA_STATE_PENDING:
		return "PENDING";
	case VINE_FILE_REPLICA_STATE_READY:
		return "READY";
	case VINE_FILE_REPLICA_STATE_DELETING:
		return "DELETING";
	case VINE_FILE_REPLICA_STATE_DELETED:
		return "DELETED";
	default:
		return "UNKNOWN";
	}
}

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

	if (!vine_file_replica_is_state_transition_allowed(old_state, new_state)) {
		debug(D_ERROR, "Invalid state transition from %s to %s", vine_file_replica_state_to_string(old_state), vine_file_replica_state_to_string(new_state));
		return 0;
	}

	r->state = new_state;
	return 1;
}
