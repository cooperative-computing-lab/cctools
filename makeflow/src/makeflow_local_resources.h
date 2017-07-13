#ifndef MAKEFLOW_LOCAL_RESOURCES_H
#define MAKEFLOW_LOCAL_RESOURCES_H

#include "dag_node.h"
#include "rmsummary.h"

struct rmsummary * makeflow_local_resources_create();
void makeflow_local_resources_delete( struct rmsummary *r );

void makeflow_local_resources_print( struct rmsummary *r );

void makeflow_local_resources_measure( struct rmsummary *r );
int  makeflow_local_resources_available( struct rmsummary *r, struct dag_node *n );
void makeflow_local_resources_subtract( struct rmsummary *r, struct dag_node *n );
void makeflow_local_resources_add( struct rmsummary *r, struct dag_node *n );


#endif
