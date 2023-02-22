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

struct vine_worker_info *vine_remote_file_table_query(struct vine_manager *q, const char *cachename)
{
	char *id;
	struct vine_worker_info *peer;
	struct vine_remote_file_info *remote_info;
	HASH_TABLE_ITERATE(q->worker_table, id, peer){
		if((remote_info = hash_table_lookup(peer->current_files, cachename)) && remote_info->in_cache) {
			return peer;
		}
	}
	return 0;
}


