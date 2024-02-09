/*
Copyright (C) 2024- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_factory.h"

#include "vine_manager.h"
#include "vine_worker_info.h"
#include "vine_factory_info.h"

#include "catalog_query.h"
#include "stringtools.h"
#include "hash_table.h"
#include "list.h"
#include "jx.h"
#include "jx_parse.h"
#include "debug.h"
#include "xxmalloc.h"

#include <assert.h>
#include <string.h>

/*
Consider a newly arriving worker that declares it was created
by a specific factory.  If this puts us over the limit for that
factory, then disconnect it.
*/

int vine_manager_factory_worker_arrive( struct vine_manager *q, struct vine_worker_info *w, const char *factory_name ) {
			
	/* The manager is now obliged to query the catalog for factory info. */
	q->fetch_factory = 1;

	/* Remember that this worker came from this specific factory. */
	w->factory_name = xxstrdup(factory_name);

	/* If we are over the desired number of workers from this factory, disconnect. */
	struct vine_factory_info *f = vine_factory_info_lookup(q, w->factory_name);
	if (f && f->connected_workers + 1 > f->max_workers) {
		vine_manager_shut_down_worker(q, w);
		return 0;
	}

	return 1;
}

/*
Consider a worker that is disconnecting, and remove the factory state if needed.
*/

void vine_manager_factory_worker_leave( struct vine_manager *q, struct vine_worker_info *w )
{	
	if (w->factory_name) {
		struct vine_factory_info *f = vine_factory_info_lookup(q, w->factory_name);
		if (f)
			f->connected_workers--;
	}
}

/*
If this currently connected worker is over the factory limit,
and isn't running anything, then shut it down.
*/

int vine_manager_factory_worker_prune( struct vine_manager *q, struct vine_worker_info *w )
{
	if(w->factory_name) {
		struct vine_factory_info *f = vine_factory_info_lookup(q, w->factory_name);
		if (f && f->connected_workers > f->max_workers && itable_size(w->current_tasks) < 1) {
			debug(D_VINE, "Final task received from worker %s, shutting down.", w->hostname);
			vine_manager_shut_down_worker(q, w);
			return 1;
		}
	}

	return 0;
}

/*
Remove idle workers associated with a given factory, so as to scale down
cleanly by not cancelling active work.
*/

static int vine_manager_factory_trim_workers(struct vine_manager *q, struct vine_factory_info *f)
{
	if (!f)
		return 0;
	assert(f->name);

	// Iterate through all workers and shut idle ones down
	struct vine_worker_info *w;
	char *key;
	int trimmed_workers = 0;

	struct hash_table *idle_workers = hash_table_create(0, 0);
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		if (f->connected_workers - trimmed_workers <= f->max_workers)
			break;
		if (w->factory_name && !strcmp(f->name, w->factory_name) && itable_size(w->current_tasks) < 1) {
			hash_table_insert(idle_workers, key, w);
			trimmed_workers++;
		}
	}

	HASH_TABLE_ITERATE(idle_workers, key, w)
	{
		hash_table_remove(idle_workers, key);
		hash_table_firstkey(idle_workers);
		vine_manager_shut_down_worker(q, w);
	}
	hash_table_delete(idle_workers);

	debug(D_VINE, "Trimmed %d workers from %s", trimmed_workers, f->name);
	return trimmed_workers;
}

/*
Given a JX description of a factory, update our internal vine_factory_info
records to match that description.  If the description indicates that
we have more workers than desired, trim the workers associated with that
factory.
*/

static void vine_manager_factory_update(struct vine_manager *q, struct jx *j)
{
	const char *name = jx_lookup_string(j, "factory_name");
	if (!name)
		return;

	struct vine_factory_info *f = vine_factory_info_lookup(q, name);

	f->seen_at_catalog = 1;
	int found = 0;
	struct jx *m = jx_lookup_guard(j, "max_workers", &found);
	if (found) {
		int old_max_workers = f->max_workers;
		f->max_workers = m->u.integer_value;
		// Trim workers if max_workers reduced.
		if (f->max_workers < old_max_workers) {
			vine_manager_factory_trim_workers(q, f);
		}
	}
}

/*
Query the catalog to discover what factories are feeding this manager,
and update all of the factory info to correspond.
*/

void vine_manager_factory_update_all(struct vine_manager *q, time_t stoptime)
{
	struct catalog_query *cq;
	struct jx *jexpr = NULL;
	struct jx *j;

	// Iterate through factory_table to create a query filter.
	int first_name = 1;
	buffer_t filter;
	buffer_init(&filter);
	char *factory_name = NULL;
	struct vine_factory_info *f = NULL;
	buffer_putfstring(&filter, "type == \"vine_factory\" && (");

	HASH_TABLE_ITERATE(q->factory_table, factory_name, f)
	{
		buffer_putfstring(&filter, "%sfactory_name == \"%s\"", first_name ? "" : " || ", factory_name);
		first_name = 0;
		f->seen_at_catalog = 0;
	}
	buffer_putfstring(&filter, ")");
	jexpr = jx_parse_string(buffer_tolstring(&filter, NULL));
	buffer_free(&filter);

	// Query the catalog server
	debug(D_VINE, "Retrieving factory info from catalog server(s) at %s ...", q->catalog_hosts);
	if ((cq = catalog_query_create(q->catalog_hosts, jexpr, stoptime))) {
		// Update the table
		while ((j = catalog_query_read(cq, stoptime))) {
			vine_manager_factory_update(q, j);
			jx_delete(j);
		}
		catalog_query_delete(cq);
	} else {
		debug(D_VINE, "Failed to retrieve factory info from catalog server(s) at %s.", q->catalog_hosts);
	}

	// Remove outdated factories
	struct list *outdated_factories = list_create();
	HASH_TABLE_ITERATE(q->factory_table, factory_name, f)
	{
		if (!f->seen_at_catalog && f->connected_workers < 1) {
			list_push_tail(outdated_factories, f);
		}
	}
	list_clear(outdated_factories, (void *)vine_factory_info_delete);
	list_delete(outdated_factories);
}
