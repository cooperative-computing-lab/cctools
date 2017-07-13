#ifndef MAKEFLOW_LOCAL_RESOURCES_H
#define MAKEFLOW_LOCAL_RESOURCES_H

#include "dag_node.h"

struct makeflow_local_resources {
	int cores;
	int memory;
	int disk;
};

struct makeflow_local_resources * makeflow_local_resources_create();
void makeflow_local_resources_delete( struct makeflow_local_resources *r );

void makeflow_local_resources_print( struct makeflow_local_resources *r );

void makeflow_local_resources_measure( struct makeflow_local_resources *r );
int  makeflow_local_resources_available( struct makeflow_local_resources *r, struct dag_node *n );
void makeflow_local_resources_subtract( struct makeflow_local_resources *r, struct dag_node *n );
void makeflow_local_resources_add( struct makeflow_local_resources *r, struct dag_node *n );


#endif
