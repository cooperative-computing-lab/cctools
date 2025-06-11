/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_replica.h"
#include "vine_counters.h"
#include "debug.h"

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

/******* File Replica State Machine:

Current State | unlink    | cache-update  | cache-invalid
+-------------+-----------+---------------+----------------
PENDING       | DELETING  | READY         | DELETED
READY         | DELETED   | *READY        | DELETED
DELETING      | *DELETING | DELETED       | DELETED
DELETED       | —         | —             | —

*We temporarily allow the trasition from READY to READY due to a race condition observed: A task
is considered complete when the manager receives a complete message. A file is considered physically present
when the manager receives a cache-update message. The combination of a task and its output file is treated as
completed only after both messages are received. However, a race condition may occur if a worker crashes midway.
If a worker crashes after sending the cache-update but before the original task’s complete is sent or properly
handled, the cleanup process will return the original task to the ready queue (from WAITING_RETRIEVAL to READY).
At the same time, the file's recovery task is submitted to bring it back. As a result, both the original and
recovery tasks may run concurrently, each attempting to produce the same output file, because the file recovery
logic is unaware that the original task has been rescheduled, and the manager cannot correlate that both tasks
are producing the same data. We will better handle this in a later version and update this part accordingly.

*DELETING -> DELETING is allowed to allow for worker removals, in this case, the state is not changed and we
will manually clean up replicas in @cleanup_worker_files
*/

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
	}

	/* should never happen */
	return "UNKNOWN";
}

static const char *vine_file_replica_state_transition_event_to_string(vine_file_replica_state_transition_event_t event)
{
	switch (event) {
	case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_UNLINK:
		return "UNLINK";
	case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_UPDATE:
		return "CACHE_UPDATE";
	case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_INVALID:
		return "CACHE_INVALID";
	}

	/* should never happen */
	return "UNKNOWN";
}

int vine_file_replica_change_state_on_event(struct vine_file_replica *replica, vine_file_replica_state_transition_event_t event)
{
	if (!replica) {
		return 0;
	}

	vine_file_replica_state_t old_state = replica->state;

	switch (old_state) {
	case VINE_FILE_REPLICA_STATE_PENDING:
		switch (event) {
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_UNLINK:
			replica->state = VINE_FILE_REPLICA_STATE_DELETING;
			return 1;
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_UPDATE:
			replica->state = VINE_FILE_REPLICA_STATE_READY;
			return 1;
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_INVALID:
			replica->state = VINE_FILE_REPLICA_STATE_DELETED;
			return 1;
		}
		break;
	case VINE_FILE_REPLICA_STATE_READY:
		switch (event) {
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_UPDATE:
			replica->state = VINE_FILE_REPLICA_STATE_READY;
			return 1;
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_UNLINK:
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_INVALID:
			replica->state = VINE_FILE_REPLICA_STATE_DELETED;
			return 1;
		}
		break;
	case VINE_FILE_REPLICA_STATE_DELETING:
		switch (event) {
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_UPDATE:
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_CACHE_INVALID:
			replica->state = VINE_FILE_REPLICA_STATE_DELETED;
			return 1;
		case VINE_FILE_REPLICA_STATE_TRANSITION_EVENT_UNLINK:
			return 1;
		}
		break;
	case VINE_FILE_REPLICA_STATE_DELETED:
		/* old state should never be DELETED as all those replicas are immediately deleted @process_replica_on_event */
		break;
	}

	debug(D_ERROR, "Invalid replica state transition: %s -> %s\n", vine_file_replica_state_to_string(old_state), vine_file_replica_state_transition_event_to_string(event));

	return 0;
}