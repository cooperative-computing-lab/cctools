#ifndef DATASWARM_RESOURCES_H
#define DATASWARM_RESOURCES_H

#include "jx.h"

struct dataswarm_resources {
	int cores;
	int memory;
	int disk;
};

struct dataswarm_resources * dataswarm_resources_create( struct jx *jresources );
struct jx * dataswarm_resources_to_jx( struct dataswarm_resources *r );
void dataswarm_resources_delete( struct dataswarm_resources *r );

#endif
