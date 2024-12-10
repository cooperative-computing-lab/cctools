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
	w->inuse_cache += replica->size;
	hash_table_insert(w->current_files, cachename, replica);

	double prev_available = w->resources->disk.total - ceil(BYTES_TO_MEGABYTES(w->inuse_cache + replica->size));
	if (prev_available >= m->current_max_worker->disk) {
		/* the current worker may have been the one with the maximum available space, so we update it. */
		m->current_max_worker->disk = w->resources->disk.total - ceil(BYTES_TO_MEGABYTES(w->inuse_cache));
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
			int current_transfers = vine_current_transfers_source_in_use(q, peer);
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

	int i = 0;
	for (source = sources_frozen[i]; i < nsources; i++) {

		int dest_found = 0;

		struct vine_file_replica *replica = hash_table_lookup(source->current_files, f->cached_name);
		if (!replica || replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}

		char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
		int source_in_use = vine_current_transfers_source_in_use(m, source);

		char *id;
		struct vine_worker_info *dest;
		int offset_bookkeep;

		HASH_TABLE_ITERATE_RANDOM_START(m->worker_table, offset_bookkeep, id, dest)
		{
			// skip if the source is the same as the destination
			if (set_lookup(sources, dest) || strcmp(source->hostname, dest->hostname) == 0) {
				continue;
			}

			// skip if the destination is not ready to transfer
			if (!dest->transfer_port_active) {
				continue;
			}

			// skip if the destination is busy with other transfers
			if (vine_current_transfers_dest_in_use(m, dest) >= m->worker_source_max_transfers) {
				continue;
			}

			debug(D_VINE, "replicating %s from %s to %s", f->cached_name, source->addrport, dest->addrport);

			vine_manager_put_url_now(m, dest, source_addr, f);

			// break if the source has paired with enough destinations for this file
			if (++dest_found >= MIN(m->file_source_max_transfers, to_find)) {
				break;
			}

			// break if the source is busy with multiple transfers
			if (++source_in_use >= m->worker_source_max_transfers) {
				break;
			}

			// break if we have found enough destinations
			if (++round_replication_request_sent >= to_find) {
				break;
			}
		}

		if (round_replication_request_sent >= to_find) {
			break;
		}

		free(source_addr);
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
