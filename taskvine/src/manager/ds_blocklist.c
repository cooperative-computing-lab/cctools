/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_blocklist.h"

#include "hash_table.h"
#include "debug.h"

#include <stdlib.h>

struct ds_blocklist_info * ds_blocklist_info_create()
{
	struct ds_blocklist_info *info;
	info = malloc(sizeof(*info));
	memset(info,0,sizeof(*info));
	return info;
}

void ds_blocklist_info_delete( struct ds_blocklist_info *info )
{
	free(info);
}

void ds_blocklist_unblock( struct ds_manager *q, const char *host )
{
	struct ds_blocklist_info *info;
	info = hash_table_remove(q->worker_blocklist,host);
	if(info) ds_blocklist_info_delete(info);
}

struct jx *ds_blocklist_to_jx( struct ds_manager  *q )
{
	if(hash_table_size(q->worker_blocklist) < 1) {
		return NULL;
	}

	struct jx *j = jx_array(0);

	char *hostname;
	struct ds_blocklist_info *info;

	hash_table_firstkey(q->worker_blocklist);
	while(hash_table_nextkey(q->worker_blocklist, &hostname, (void *) &info)) {
		if(info->blocked) {
			jx_array_insert(j, jx_string(hostname));
		}
	}

	return j;
}

/* deadline < 1 means release all, regardless of release_at time. */
void ds_blocklist_unblock_all_by_time(struct ds_manager *q, time_t deadline)
{
	char *hostname;
	struct ds_blocklist_info *info;

	hash_table_firstkey(q->worker_blocklist);
	while(hash_table_nextkey(q->worker_blocklist, &hostname, (void *) &info)) {
		if(!info->blocked)
			continue;

		/* do not clear if blocked indefinitely, and we are not clearing the whole list. */
		if(info->release_at < 1 && deadline > 0)
			continue;

		/* do not clear if the time for this host has not meet the deadline. */
		if(deadline > 0 && info->release_at > deadline)
			continue;

		debug(D_DS, "Clearing hostname %s from blocklist.\n", hostname);
		ds_blocklist_unblock(q, hostname);
	}
}

void ds_blocklist_block( struct ds_manager *q, const char *hostname, time_t timeout )
{
	struct ds_blocklist_info *info = hash_table_lookup(q->worker_blocklist,hostname);
	if(!info) {
		info = ds_blocklist_info_create();
		hash_table_insert(q->worker_blocklist,hostname,info);
	}

	q->stats->workers_blocked++;

	/* count the times the worker goes from active to blocked. */
	if(!info->blocked)
		info->times_blocked++;

	info->blocked = 1;

	if(timeout > 0) {
		debug(D_DS, "Blocking host %s by %" PRIu64 " seconds (blocked %d times).\n", hostname, (uint64_t) timeout, info->times_blocked);
		info->release_at = time(0) + timeout;
	} else {
		debug(D_DS, "Blocking host %s indefinitely.\n", hostname);
		info->release_at = -1;
	}
}

int ds_blocklist_is_blocked( struct ds_manager *q, const char *hostname )
{
	struct ds_blocklist_info *info = hash_table_lookup(q->worker_blocklist,hostname);
	return info && info->blocked;
}



