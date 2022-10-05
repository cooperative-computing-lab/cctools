
#ifndef VINE_FACTORY_INFO_H
#define VINE_FACTORY_INFO_H

#include "vine_manager.h"

struct vine_factory_info {
	char *name;
	int   connected_workers;
	int   max_workers;
	int   seen_at_catalog;
};

struct vine_factory_info *vine_factory_info_create( const char *name );
void vine_factory_info_delete( struct vine_factory_info *f );

struct vine_factory_info *vine_factory_info_lookup( struct vine_manager *q, const char *name );
void vine_factory_info_remove(struct vine_manager *q, const char *name);

#endif
