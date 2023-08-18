/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_blocklist.h"

#include "debug.h"
#include "hash_table.h"

#include <stdlib.h>

struct vine_blocklist_info *vine_blocklist_info_create()
{
	struct vine_blocklist_info *info;
	info = malloc(sizeof(*info));
	memset(info, 0, sizeof(*info));
	return info;
}

void vine_blocklist_info_delete(struct vine_blocklist_info *info) { free(info); }

void vine_blocklist_unblock(struct vine_manager *q, const char *host)
{
	struct vine_blocklist_info *info;
	info = hash_table_remove(q->worker_blocklist, host);
	if (info)
		vine_blocklist_info_delete(info);
}

struct jx *vine_blocklist_to_jx(struct vine_manager *q)
{
	if (hash_table_size(q->worker_blocklist) < 1) {
		return NULL;
	}

	struct jx *j = jx_array(0);

	char *hostname;
	struct vine_blocklist_info *info;

	HASH_TABLE_ITERATE(q->worker_blocklist, hostname, info)
	{
		if (info->blocked) {
			jx_array_insert(j, jx_string(hostname));
		}
	}

	return j;
}

/* deadline < 1 means release all, regardless of release_at time. */
void vine_blocklist_unblock_all_by_time(struct vine_manager *q, time_t deadline)
{
	char *hostname;
	struct vine_blocklist_info *info;

	HASH_TABLE_ITERATE(q->worker_blocklist, hostname, info)
	{
		if (!info->blocked)
			continue;

		/* do not clear if blocked indefinitely, and we are not clearing the whole list. */
		if (info->release_at < 1 && deadline > 0)
			continue;

		/* do not clear if the time for this host has not meet the deadline. */
		if (deadline > 0 && info->release_at > deadline)
			continue;

		debug(D_VINE, "Clearing hostname %s from blocklist.\n", hostname);
		vine_blocklist_unblock(q, hostname);
	}
}

void vine_blocklist_block(struct vine_manager *q, const char *hostname, time_t timeout)
{
	struct vine_blocklist_info *info = hash_table_lookup(q->worker_blocklist, hostname);
	if (!info) {
		info = vine_blocklist_info_create();
		hash_table_insert(q->worker_blocklist, hostname, info);
	}

	q->stats->workers_blocked++;

	/* count the times the worker goes from active to blocked. */
	if (!info->blocked)
		info->times_blocked++;

	info->blocked = 1;

	if (timeout > 0) {
		debug(D_VINE,
				"Blocking host %s by %" PRIu64 " seconds (blocked %d times).\n",
				hostname,
				(uint64_t)timeout,
				info->times_blocked);
		info->release_at = time(0) + timeout;
	} else {
		debug(D_VINE, "Blocking host %s indefinitely.\n", hostname);
		info->release_at = -1;
	}
}

int vine_blocklist_is_blocked(struct vine_manager *q, const char *hostname)
{
	struct vine_blocklist_info *info = hash_table_lookup(q->worker_blocklist, hostname);
	return info && info->blocked;
}
