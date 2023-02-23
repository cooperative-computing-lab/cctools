/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_manager.h"
#include "vine_file.h"
#include "vine_remote_file_info.h"
#include "vine_worker_info.h"
#include "vine_remote_file_table.h"
#include "vine_current_transfers.h"

#include "stringtools.h"

#include "debug.h"

int vine_remote_file_table_insert(struct vine_worker_info *w, const char *cachename, struct vine_remote_file_info *remote_info)
{
	hash_table_insert(w->current_files, cachename, remote_info);
	return 1;
}

struct vine_remote_file_info *vine_remote_file_table_remove(struct vine_worker_info *w, const char *cachename)
{
	return hash_table_remove(w->current_files, cachename);
}

struct vine_remote_file_info *vine_remote_file_table_lookup(struct vine_worker_info *w, const char *cachename)
{
	struct vine_remote_file_info *remote_info = hash_table_lookup(w->current_files, cachename);
	return remote_info;
}

struct vine_worker_info *vine_remote_file_table_find_worker(struct vine_manager *q, const char *cachename)
{
	char *id;
	struct vine_worker_info *peer;
	struct vine_remote_file_info *remote_info;
	HASH_TABLE_ITERATE(q->worker_table, id, peer){
		// generate a peer address stub as it would appear in the transfer table
		char *peer_addr =  string_format("worker://%s:%d", peer->transfer_addr, peer->transfer_port);
		if((remote_info = hash_table_lookup(peer->current_files, cachename)) && remote_info->in_cache) 
		{
			if(vine_current_transfers_worker_in_use(q, peer_addr) < q->worker_source_max_transfers) 
			{
				free(peer_addr);
				return peer;
			}
		}
		free(peer_addr);
	}
	return 0;
}


