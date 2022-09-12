
#ifndef DS_FACTORY_INFO_H
#define DS_FACTORY_INFO_H

#include "ds_manager.h"

struct ds_factory_info {
	char *name;
	int   connected_workers;
	int   max_workers;
	int   seen_at_catalog;
};

struct ds_factory_info *ds_factory_info_create( const char *name );
void ds_factory_info_delete( struct ds_factory_info *f );

struct ds_factory_info *ds_factory_info_lookup( struct ds_manager *q, const char *name );
void ds_factory_info_remove(struct ds_manager *q, const char *name);

#endif
