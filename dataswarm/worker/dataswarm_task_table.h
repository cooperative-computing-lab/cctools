#ifndef DATASWARM_TASK_TABLE_H
#define DATASWARM_TASK_TABLE_H

#include "dataswarm_message.h"
#include "dataswarm_worker.h"
#include "jx.h"

dataswarm_result_t dataswarm_task_table_submit( struct dataswarm_worker *w, const char *taskid, struct jx *jtask );
dataswarm_result_t dataswarm_task_table_get( struct dataswarm_worker *w, const char *taskid, struct jx **jtask );
dataswarm_result_t dataswarm_task_table_remove( struct dataswarm_worker *w, const char *taskid );

void dataswarm_task_table_advance( struct dataswarm_worker *w );

#endif
