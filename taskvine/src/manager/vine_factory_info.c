/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_factory_info.h"
#include "vine_manager.h"

#include "debug.h"
#include "xxmalloc.h"

struct vine_factory_info *vine_factory_info_create(const char *name)
{
	struct vine_factory_info *f;
	f = malloc(sizeof(*f));
	f->name = xxstrdup(name);
	f->connected_workers = 0;
	f->max_workers = INT_MAX;
	f->seen_at_catalog = 0;
	return f;
}

void vine_factory_info_delete(struct vine_factory_info *f)
{
	if (f) {
		free(f->name);
		free(f);
	}
}

struct vine_factory_info *vine_factory_info_lookup(struct vine_manager *q, const char *name)
{
	struct vine_factory_info *f = hash_table_lookup(q->factory_table, name);
	if (f)
		return f;

	f = vine_factory_info_create(name);
	hash_table_insert(q->factory_table, name, f);
	return f;
}

void vine_factory_info_remove(struct vine_manager *q, const char *name)
{
	struct vine_factory_info *f = hash_table_remove(q->factory_table, name);
	if (f) {
		vine_factory_info_delete(f);
	} else {
		debug(D_VINE, "Failed to remove unrecorded factory %s", name);
	}
}
