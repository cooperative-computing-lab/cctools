/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_file_replica_table.h"
#include "vine_current_transfers.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_manager.h"
#include "vine_worker_info.h"

#include "stringtools.h"

#include "debug.h"

// add a file to the remote file table.
int vine_file_replica_table_insert(
		struct vine_worker_info *w, const char *cachename, struct vine_file_replica *remote_info)
{
	hash_table_insert(w->current_files, cachename, remote_info);
	return 1;
}

// remove a file from the remote file table.
struct vine_file_replica *vine_file_replica_table_remove(struct vine_worker_info *w, const char *cachename)
{
	return hash_table_remove(w->current_files, cachename);
}

// lookup a file in posession of a specific worker
struct vine_file_replica *vine_file_replica_table_lookup(struct vine_worker_info *w, const char *cachename)
{
	struct vine_file_replica *remote_info = hash_table_lookup(w->current_files, cachename);
	return remote_info;
}

// find a worker in posession of a specific file, and is ready to transfer it.
struct vine_worker_info *vine_file_replica_table_find_worker(struct vine_manager *q, const char *cachename)
{
	char *id;
	struct vine_worker_info *peer;
	struct vine_file_replica *remote_info;
	HASH_TABLE_ITERATE(q->worker_table, id, peer)
	{
		// generate a peer address stub as it would appear in the transfer table
		char *peer_addr = string_format("worker://%s:%d", peer->transfer_addr, peer->transfer_port);
		if ((remote_info = hash_table_lookup(peer->current_files, cachename)) && remote_info->in_cache) {
			if (vine_current_transfers_worker_in_use(q, peer_addr) < q->worker_source_max_transfers) {
				free(peer_addr);
				return peer;
			}
		}
		free(peer_addr);
	}
	return 0;
}

/*
Determine if this file is cached *anywhere* in the system.
XXX Note that this implementation is another inefficient linear search.
*/

int vine_file_replica_table_exists_somewhere(struct vine_manager *q, const char *cachename)
{
	char *key;
	struct vine_worker_info *w;
	struct vine_file_replica *r;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		r = hash_table_lookup(w->current_files, cachename);
		if (r)
			return 1;
	}

	return 0;
}
