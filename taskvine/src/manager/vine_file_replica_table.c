/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

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
int vine_file_replica_table_replicate(struct vine_manager *m, struct vine_file *f)
{
	int found = 0;

	if (vine_current_transfers_get_table_size(m) >= hash_table_size(m->worker_table) * m->worker_source_max_transfers) {
		return found;
	}

	struct set *sources = hash_table_lookup(m->file_worker_table, f->cached_name);
	if (!sources) {
		return found;
	}

	int nsources = set_size(sources);
	int to_find = MIN(m->temp_replica_count - nsources, m->transfer_replica_per_cycle);
	if (to_find < 1) {
		return found;
	}

	debug(D_VINE, "Found %d workers holding %s, %d replicas needed", nsources, f->cached_name, to_find);

	/* get the elements of set so we can insert new replicas to sources */
	struct vine_worker_info **sources_frozen = (struct vine_worker_info **)set_values(sources);
	struct vine_worker_info *source;

	int i = 0;
	for (source = sources_frozen[i]; i < nsources; i++) {
		if (found >= to_find) {
			break;
		}

		int found_per_source = 0;

		struct vine_file_replica *replica = hash_table_lookup(source->current_files, f->cached_name);
		if (!replica || replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}

		char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
		int source_in_use = vine_current_transfers_source_in_use(m, source);

		char *id;
		struct vine_worker_info *peer;
		int offset_bookkeep;
		HASH_TABLE_ITERATE_RANDOM_START(m->worker_table, offset_bookkeep, id, peer)
		{

			if (found_per_source >= MIN(m->file_source_max_transfers, to_find)) {
				break;
			}

			if (source_in_use >= m->worker_source_max_transfers) {
				break;
			}

			if (!peer->transfer_port_active) {
				continue;
			}

			if (set_lookup(sources, peer)) {
				continue;
			}

			if (vine_current_transfers_dest_in_use(m, peer) >= m->worker_source_max_transfers) {
				continue;
			}

			if (strcmp(source->hostname, peer->hostname) == 0) {
				continue;
			}

			debug(D_VINE, "replicating %s from %s to %s", f->cached_name, source->addrport, peer->addrport);

			vine_manager_put_url_now(m, peer, source_addr, f);

			source_in_use++;
			found_per_source++;
			found++;
		}

		free(source_addr);
	}

	free(sources_frozen);

	return found;
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
