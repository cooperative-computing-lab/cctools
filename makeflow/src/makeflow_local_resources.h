#ifndef MAKEFLOW_LOCAL_RESOURCES_H
#define MAKEFLOW_LOCAL_RESOURCES_H

#include "dag_node.h"
#include "rmsummary.h"

void makeflow_local_resources_print( struct rmsummary *r );

void makeflow_local_resources_measure( struct rmsummary *r );
int  makeflow_local_resources_available( struct rmsummary *r, const struct rmsummary *asked);
void makeflow_local_resources_subtract( struct rmsummary *r, struct dag_node *n );
void makeflow_local_resources_add( struct rmsummary *r, struct dag_node *n );


#endif
