/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_current_transfers.h"
#include "vine_manager.h"
#include "xxmalloc.h"

#include "debug.h"

struct vine_transfer_pair {
	struct vine_worker_info *to;
	struct vine_worker_info *source_worker;
	char *source_url;
};

static struct vine_transfer_pair *vine_transfer_pair_create(
		struct vine_worker_info *to, struct vine_worker_info *source_worker, const char *source_url)
{
	struct vine_transfer_pair *t = malloc(sizeof(struct vine_transfer_pair));
	t->to = to;
	t->source_worker = source_worker;
	t->source_url = source_url ? xxstrdup(source_url) : 0;
	return t;
}

static void vine_transfer_pair_delete(struct vine_transfer_pair *p)
{
	if (p) {
		free(p->source_url);
		free(p);
	}
}

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *to,
		struct vine_worker_info *source_worker, const char *source_url)
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);

	char *transfer_id = strdup(uuid.str);
	struct vine_transfer_pair *t = vine_transfer_pair_create(to, source_worker, source_url);

	hash_table_insert(q->current_transfer_table, transfer_id, t);
	return transfer_id;
}

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	p = hash_table_remove(q->current_transfer_table, id);
	if (p) {
		vine_transfer_pair_delete(p);
		return 1;
	} else {
		return 0;
	}
}

int vine_current_transfers_set_failure(struct vine_manager *q, char *id)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	int throttled = 0;

	if (!p)
		return throttled;

	struct vine_worker_info *source_worker = p->source_worker;
	if (source_worker) {
		source_worker->last_transfer_failure = timestamp_get();
		debug(D_VINE,
				"Setting transfer failure timestamp on source worker: %s:%d",
				source_worker->hostname,
				source_worker->transfer_port);
		throttled++;
	}

	struct vine_worker_info *to = p->to;
	if (to) {
		to->last_transfer_failure = timestamp_get();
		debug(D_VINE,
				"Setting transfer failure timestamp on destination worker: %s:%d",
				to->hostname,
				to->transfer_port);
		throttled++;
	}

	return throttled;
}

// count the number transfers coming from a specific source
int vine_current_transfers_source_in_use(struct vine_manager *q, struct vine_worker_info *source_worker)
{
	char *id;
	struct vine_transfer_pair *t;
	int c = 0;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (source_worker == t->source_worker)
			c++;
	}
	return c;
}

// count the number transfers coming from a specific remote url (not a worker)
int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source)
{
	char *id;
	struct vine_transfer_pair *t;
	int c = 0;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (source == t->source_url)
			c++;
	}
	return c;
}

// count the number of ongoing transfers to a specific worker
int vine_current_transfers_dest_in_use(struct vine_manager *q, struct vine_worker_info *w)
{
	char *id;
	struct vine_transfer_pair *t;
	int c = 0;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (t->to == w)
			c++;
	}
	return c;
}

// remove all transactions involving a worker from the transfer table - if a worker failed or is being deleted
// intentionally
int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	debug(D_VINE, "Removing instances of worker from transfer table");

	int removed = 0;
	if (!w) {
		return removed;
	}

	char *id;
	struct vine_transfer_pair *t;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (t->to == w || t->source_worker == w) {
			vine_current_transfers_remove(q, id);
			removed++;
		}
	}

	return removed;
}

void vine_current_transfers_print_table(struct vine_manager *q)
{
	char *id;
	struct vine_transfer_pair *t;
	struct vine_worker_info *w;
	debug(D_VINE, "-----------------TRANSFER-TABLE--------------------");
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		w = t->source_worker;
		if (w) {
			debug(D_VINE,
					"%s : source: %s:%d url: %s",
					id,
					w->transfer_host,
					w->transfer_port,
					t->source_url);
		} else {
			debug(D_VINE, "%s : source: remote url: %s", id, t->source_url);
		}
	}
	debug(D_VINE, "-----------------END-------------------------------");
}

void vine_current_transfers_clear(struct vine_manager *q)
{
	hash_table_clear(q->current_transfer_table, (void *)vine_transfer_pair_delete);
}
