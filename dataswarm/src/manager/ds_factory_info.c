#include "ds_manager.h"
#include "ds_factory_info.h"

#include "xxmalloc.h"
#include "debug.h"

struct ds_factory_info *ds_factory_info_create( const char *name )
{
	struct ds_factory_info *f;
	f = malloc(sizeof(*f));
	f->name = xxstrdup(name);
	f->connected_workers = 0;
	f->max_workers = INT_MAX;
	f->seen_at_catalog = 0;
	return f;
}

void ds_factory_info_delete( struct ds_factory_info *f )
{
	if (f) {
		free(f->name);
		free(f);
	}
}

struct ds_factory_info *ds_factory_info_lookup(struct ds_manager *q, const char *name)
{
	struct ds_factory_info *f = hash_table_lookup(q->factory_table, name);
	if(f) return f;

	f = ds_factory_info_create(name);
	hash_table_insert(q->factory_table, name, f);
	return f;
}

void ds_factory_info_remove( struct ds_manager *q, const char *name )
{
	struct ds_factory_info *f = hash_table_remove(q->factory_table, name);
	if (f) {
		ds_factory_info_delete(f);
	} else {
		debug(D_DS, "Failed to remove unrecorded factory %s", name);
	}
}

