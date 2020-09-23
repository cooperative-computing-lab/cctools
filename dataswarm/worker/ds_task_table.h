#ifndef DS_TASK_TABLE_H
#define DS_TASK_TABLE_H

#include "comm/ds_message.h"
#include "dataswarm_worker.h"
#include "jx.h"

ds_result_t ds_task_table_submit( struct dataswarm_worker *w, const char *taskid, struct jx *jtask );
ds_result_t ds_task_table_get( struct dataswarm_worker *w, const char *taskid, struct jx **jtask );
ds_result_t ds_task_table_remove( struct dataswarm_worker *w, const char *taskid );

void ds_task_table_advance( struct dataswarm_worker *w );

#endif
