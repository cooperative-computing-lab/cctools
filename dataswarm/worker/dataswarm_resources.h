#ifndef DATASWARM_RESOURCES_H
#define DATASWARM_RESOURCES_H

struct dataswarm_resources {
	int cores;
	int memory;
	int disk;
};

struct dataswarm_resources * dataswarm_resources_create( struct jx *jresources );
void dataswarm_resources_delete( struct dataswarm_resources *r );

#endif
