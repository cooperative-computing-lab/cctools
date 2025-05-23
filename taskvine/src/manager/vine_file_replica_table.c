/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

#include <math.h>

#include "vine_file_replica_table.h"
#include "set.h"
#include "vine_blocklist.h"
#include "vine_current_transfers.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_manager.h"
#include "vine_manager_put.h"
#include "vine_worker_info.h"

#include "stringtools.h"

#include "debug.h"
#include "macros.h"

// add a file to the remote file table.
int vine_file_replica_table_insert(struct vine_manager *m, struct vine_worker_info *w, const char *cachename, struct vine_file_replica *replica)
{
	if (hash_table_lookup(w->current_files, cachename)) {
		return 0;
	}

	w->inuse_cache += replica->size;
	hash_table_insert(w->current_files, cachename, replica);

	double prev_available = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache + replica->size);
	if (prev_available >= m->current_max_worker->disk) {
		/* the current worker may have been the one with the maximum available space, so we update it. */
		m->current_max_worker->disk = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);
	}

	struct set *workers = hash_table_lookup(m->file_worker_table, cachename);
	if (!workers) {
		workers = set_create(4);
		hash_table_insert(m->file_worker_table, cachename, workers);
	}

	set_insert(workers, w);

	return 1;
}

// remove a file from the remote file table.
struct vine_file_replica *vine_file_replica_table_remove(struct vine_manager *m, struct vine_worker_info *w, const char *cachename)
{
	if (!w) {
		// Handle error: invalid pointer
		return 0;
	}

	if (!hash_table_lookup(w->current_files, cachename)) {
		return 0;
	}

	struct vine_file_replica *replica = hash_table_remove(w->current_files, cachename);
	if (replica) {
		w->inuse_cache -= replica->size;
	}

	double available = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);
	if (available > m->current_max_worker->disk) {
		/* the current worker has more space than we knew before for all workers, so we update it. */
		m->current_max_worker->disk = available;
	}

	struct set *workers = hash_table_lookup(m->file_worker_table, cachename);

	if (workers) {
		set_remove(workers, w);
		if (set_size(workers) < 1) {
			hash_table_remove(m->file_worker_table, cachename);
			set_delete(workers);
		}
	}

	return replica;
}

// lookup a file in posession of a specific worker
struct vine_file_replica *vine_file_replica_table_lookup(struct vine_worker_info *w, const char *cachename)
{
	return hash_table_lookup(w->current_files, cachename);
}

// count the number of in-cluster replicas of a file
int vine_file_replica_count(struct vine_manager *m, struct vine_file *f)
{
	return set_size(hash_table_lookup(m->file_worker_table, f->cached_name));
}

// find a worker (randomly) in posession of a specific file, and is ready to transfer it.
struct vine_worker_info *vine_file_replica_table_find_worker(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers) {
		return 0;
	}

	int total_count = set_size(workers);
	if (total_count < 1) {
		return 0;
	}

	int random_index = random() % total_count;

	struct vine_worker_info *peer = NULL;
	struct vine_worker_info *peer_selected = NULL;
	struct vine_file_replica *replica = NULL;

	int offset_bookkeep;
	SET_ITERATE_RANDOM_START(workers, offset_bookkeep, peer)
	{
		random_index--;
		if (!peer->transfer_port_active)
			continue;

		timestamp_t current_time = timestamp_get();
		if (current_time - peer->last_transfer_failure < q->transient_error_interval) {
			debug(D_VINE, "Skipping worker source after recent failure : %s", peer->transfer_host);
			continue;
		}

		if ((replica = hash_table_lookup(peer->current_files, cachename)) && replica->state == VINE_FILE_REPLICA_STATE_READY) {
			int current_transfers = peer->outgoing_xfer_counter;
			if (current_transfers < q->worker_source_max_transfers) {
				peer_selected = peer;
				if (random_index < 0) {
					return peer_selected;
				}
			}
		}
	}

	return peer_selected;
}

// trigger replications of file to satisfy temp_replica_count
int vine_file_replica_table_replicate(struct vine_manager *m, struct vine_file *f, struct set *sources, int to_find)
{
	int nsources = set_size(sources);
	int round_replication_request_sent = 0;

	/* get the elements of set so we can insert new replicas to sources */
	struct vine_worker_info **sources_frozen = (struct vine_worker_info **)set_values(sources);
	struct vine_worker_info *source;

	for (int i = 0; i < nsources; i++) {

		source = sources_frozen[i];
		int dest_found = 0;

		// skip if the file on the source is not ready to transfer
		struct vine_file_replica *replica = hash_table_lookup(source->current_files, f->cached_name);
		if (!replica || replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}

		char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);

		// skip if the source is busy with other transfers
		if (source->outgoing_xfer_counter >= m->worker_source_max_transfers) {
			continue;
		}

		char *id;
		struct vine_worker_info *dest;
		int offset_bookkeep;

		HASH_TABLE_ITERATE_RANDOM_START(m->worker_table, offset_bookkeep, id, dest)
		{
			// skip if the source and destination are on the same host
			if (set_lookup(sources, dest) || strcmp(source->hostname, dest->hostname) == 0) {
				continue;
			}

			// skip if the destination is not ready to transfer
			if (!dest->transfer_port_active) {
				continue;
			}

			// skip if the destination is busy with other transfers
			if (dest->incoming_xfer_counter >= m->worker_source_max_transfers) {
				continue;
			}

			debug(D_VINE, "replicating %s from %s to %s", f->cached_name, source->addrport, dest->addrport);

			vine_manager_put_url_now(m, dest, source_addr, f);

			round_replication_request_sent++;

			// break if we have found enough destinations for this source
			if (++dest_found >= MIN(m->file_source_max_transfers, to_find)) {
				break;
			}

			// break if the source becomes busy with transfers
			if (source->outgoing_xfer_counter >= m->worker_source_max_transfers) {
				break;
			}
		}

		free(source_addr);

		// break if we have sent enough replication requests for this file
		if (round_replication_request_sent >= to_find) {
			break;
		}
	}

	free(sources_frozen);

	return round_replication_request_sent;
}

/*
Count number of replicas of a file in the system.
*/
int vine_file_replica_table_count_replicas(struct vine_manager *q, const char *cachename, vine_file_replica_state_t state)
{
	struct vine_worker_info *w;
	struct vine_file_replica *r;
	int count = 0;

	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (workers) {
		SET_ITERATE(workers, w)
		{
			r = hash_table_lookup(w->current_files, cachename);
			if (r && r->state == state) {
				count++;
			}
		}
	}

	return count;
}

int vine_file_replica_table_exists_somewhere(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers) {
		return 0;
	}

	struct vine_worker_info *peer;

	SET_ITERATE(workers, peer)
	{
		if (peer->transfer_port_active) {
			return 1;
		}
	}

	return 0;
}

/******* File Replica State Machine:
PENDING ->  READY             : receive "cache-update" — file has physically arrived on the worker
PENDING ->  DELETING          : receive "cache-invalid" — file was expected but not found on the worker
PENDING ->  DELETED           : worker disconnect — file was never ready and worker is now unreachable

READY   ->  DELETING          : send "unlink" — manager initiates deletion of a valid file
READY   ->  DELETED           : worker disconnect — file was on a worker that has now disconnected

DELETING -> DELETED           : receive "unlink-complete" — worker confirms deletion
DELETING -> DELETED           : worker disconnect — deletion in progress, but worker is now unreachable

DELETED  -> (no transition)   : terminal state,

Additionally, we allow self-transitions to all states.
*/
static const int vine_file_replica_allowed_state_transitions[4][4] = {
		// From/To:   PENDING  READY  DELETING   DELETED
		/* PENDING   */ {1, 1, 1, 1},
		/* READY     */ {0, 1, 1, 1},
		/* DELETING  */ {0, 0, 1, 1},
		/* DELETED   */ {0, 0, 0, 1}};

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

static int vine_file_replica_table_change_replica_state(struct vine_manager *q, struct vine_worker_info *w, struct vine_file_replica *r, const char *cachename, vine_file_replica_state_t new_state)
{
	if (!r) {
		return 0;
	}

	vine_file_replica_state_t old_state = r->state;

	/* if the self-to-self transition happens, something might be stinky there... */
	if (old_state == new_state) {
		debug(D_VINE,
				"Self state transition for file %s from %s to %s\n",
				cachename,
				vine_file_replica_state_to_string(old_state),
				vine_file_replica_state_to_string(new_state));
	}

	/* check if the state transition is allowed */
	if (!vine_file_replica_is_state_transition_allowed(old_state, new_state)) {
		debug(D_ERROR,
				"Invalid state transition for file %s from %s to %s\n",
				cachename,
				vine_file_replica_state_to_string(old_state),
				vine_file_replica_state_to_string(new_state));
		return 0;
	}

	r->state = new_state;

	/* the replica is cleaned only when it's state is set to DELETED */
	if (r->state == VINE_FILE_REPLICA_STATE_DELETED) {
		struct vine_file_replica *removed_replica = vine_file_replica_table_remove(q, w, cachename);
		if (removed_replica) {
			vine_file_replica_delete(removed_replica);
		}
	}

	return 1;
}

int vine_file_replica_table_handle_receive_cache_update(struct vine_manager *q, struct vine_worker_info *w, const char *cachename, int64_t size, time_t mtime)
{
	if (!q || !w || !cachename) {
		return 0;
	}

	struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);
	if (!replica) {
		/* this is OK, the file may have been deleted already */
		return 1;
	}

	/* race condition check: if file is in DELETING state, reject cache-update.
	 * we do not check for DELETED state as all deleted files are removed immediately in the current implementation */
	if (replica->state == VINE_FILE_REPLICA_STATE_DELETING) {
		return 1;
	}

	/* update replica stat */
	replica->size = size;
	replica->mtime = mtime;
	replica->transfer_time = timestamp_get();

	/* change state from PENDING to READY */
	return vine_file_replica_table_change_replica_state(q, w, replica, cachename, VINE_FILE_REPLICA_STATE_READY);
}

int vine_file_replica_table_handle_receive_cache_invalid(struct vine_manager *q, struct vine_worker_info *w, const char *cachename)
{
	if (!q || !w || !cachename) {
		return 0;
	}

	struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);
	if (!replica) {
		/* this is OK, the file may have been deleted already */
		return 1;
	}

	/* no race condition on cache-invalid */

	/* change state to DELETED */
	if (!vine_file_replica_table_change_replica_state(q, w, replica, cachename, VINE_FILE_REPLICA_STATE_DELETED)) {
		return 0;
	}

	return 1;
}

int vine_file_replica_table_handle_receive_unlink_complete(struct vine_manager *q, struct vine_worker_info *w, const char *cachename, int success)
{
	if (!q || !w || !cachename) {
		return 0;
	}

	struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);
	if (!replica) {
		/* this is OK as the replica may have been deleted already */
		return 1;
	}

	/* no race condition on unlink-complete */

	/* NOTE: in the current implementation, success is always true, but may be useful in the future */
	return vine_file_replica_table_change_replica_state(q, w, replica, cachename, VINE_FILE_REPLICA_STATE_DELETED);
}

void vine_file_replica_table_handle_worker_disconnect(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!q || !w) {
		return;
	}

	/* remove all file replicas on this worker */

	char *cached_name = NULL;
	char **cached_names = hash_table_keys_array(w->current_files);

	if (!cached_names) {
		return;
	}

	int i = 0;
	while ((cached_name = cached_names[i])) {
		i++;

		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cached_name);
		if (!replica) {
			continue;
		}

		/* set state to DELETED before removal to comply with the state machine:
		 * all operations must follow a valid state transition */
		vine_file_replica_table_change_replica_state(q, w, replica, cached_name, VINE_FILE_REPLICA_STATE_DELETED);
	}

	hash_table_free_keys_array(cached_names);
}

int vine_file_replica_table_handle_send_unlink(struct vine_manager *q, struct vine_worker_info *w, const char *cachename)
{
	if (!q || !w || !cachename) {
		return 0;
	}

	struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);
	if (!replica) {
		/* this is OK, the file may have been deleted already */
		return 1;
	}

	/* only send unlink if state is PENDING or READY */
	if (!vine_file_replica_table_change_replica_state(q, w, replica, cachename, VINE_FILE_REPLICA_STATE_DELETING)) {
		return 0;
	}

	/* send unlink message to worker */
	vine_manager_send(q, w, "unlink %s\n", cachename);

	return 1;
}