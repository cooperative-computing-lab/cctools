/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_file_replica_table.h"
#include "set.h"
#include "vine_current_transfers.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_manager.h"
#include "vine_worker_info.h"

#include "stringtools.h"

#include "debug.h"

// add a file to the remote file table.
int vine_file_replica_table_insert(struct vine_manager *m, struct vine_worker_info *w, const char *cachename,
		struct vine_file_replica *replica)
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
struct vine_file_replica *vine_file_replica_table_remove(
		struct vine_manager *m, struct vine_worker_info *w, const char *cachename)
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

		if ((replica = hash_table_lookup(peer->current_files, cachename)) &&
				replica->state == VINE_FILE_REPLICA_STATE_READY) {

			// generate a peer address stub as it would appear in the transfer table
			char *peer_addr = string_format("worker://%s:%d", peer->transfer_addr, peer->transfer_port);
			int current_transfers = vine_current_transfers_source_in_use(q, peer);
			free(peer_addr);

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

// return an array of up to q->temp_replica_count workers that do not have the file cachename
// and are not on the same host as worker w.
struct vine_worker_info **vine_file_replica_table_find_replication_targets(
		struct vine_manager *q, struct vine_worker_info *w, const char *cachename, int *count)
{
	char *id;
	struct vine_worker_info *peer;
	struct vine_file_replica *replica;

	int found = 0;

	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	struct vine_worker_info **filtered = malloc(sizeof(struct vine_worker_info) * (q->temp_replica_count));

	if (!workers) {
		*count = 0;
		return filtered;
	}

	// some random distribution
	int offset_bookkeep;
	HASH_TABLE_ITERATE_RANDOM_START(q->worker_table, offset_bookkeep, id, peer)
	{
		if (found == q->temp_replica_count)
			break;
		if (!peer->transfer_port_active)
			continue;

		if (!(replica = hash_table_lookup(peer->current_files, cachename)) &&
				(strcmp(w->hostname, peer->hostname))) {
			if ((vine_current_transfers_source_in_use(q, peer) < q->worker_source_max_transfers) &&
					(vine_current_transfers_dest_in_use(q, peer) <
							q->worker_source_max_transfers)) {
				debug(D_VINE, "found replication target : %s", peer->transfer_addr);
				filtered[found] = peer;
				found++;
			}
		}
	}
	*count = found;
	return filtered;
}

/*
Count number of replicas of a file in the system.
XXX Note that this implementation is another inefficient linear search.
*/

int vine_file_replica_table_count_replicas(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers) {
		return 0;
	}

	return set_size(workers);
}

/*
 */
int vine_file_replica_table_exists_somewhere(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers) {
		return 0;
	}

	struct vine_worker_info *w;
	struct vine_file_replica *r;
	SET_ITERATE(workers, w)
	{
		r = hash_table_lookup(w->current_files, cachename);
		if (r && r->state == VINE_FILE_REPLICA_STATE_READY)
			return 1;
	}

	return 0;
}
