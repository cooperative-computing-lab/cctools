#ifndef DATASWARM_RESOURCES_H
#define DATASWARM_RESOURCES_H

#include "jx.h"

struct ds_resources {
	int64_t cores;
	int64_t memory;
	int64_t disk;
};

struct ds_resources * ds_resources_create( int64_t cores, int64_t memory, int64_t disk );
struct ds_resources * ds_resources_create_from_jx( struct jx *jresources );
struct jx * ds_resources_to_jx( struct ds_resources *r );

int  ds_resources_compare( struct ds_resources *a, struct ds_resources *b );
void ds_resources_add( struct ds_resources *a, struct ds_resources *b );
void ds_resources_sub( struct ds_resources *a, struct ds_resources *b );

void ds_resources_delete( struct ds_resources *r );


#endif
