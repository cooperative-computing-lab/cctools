#ifndef DATASWARM_RESOURCES_H
#define DATASWARM_RESOURCES_H

#include "jx.h"

struct ds_resources {
	int cores;
	int memory;
	int disk;
};

struct ds_resources * ds_resources_create( struct jx *jresources );
struct jx * ds_resources_to_jx( struct ds_resources *r );
void ds_resources_delete( struct ds_resources *r );

#endif
