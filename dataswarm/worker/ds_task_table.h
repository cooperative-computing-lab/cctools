#ifndef DATASWARM_TASK_TABLE_H
#define DATASWARM_TASK_TABLE_H

#include "ds_message.h"
#include "ds_worker.h"
#include "jx.h"

ds_result_t ds_task_table_submit( struct ds_worker *w, const char *taskid, struct jx *jtask );
ds_result_t ds_task_table_get( struct ds_worker *w, const char *taskid, struct jx **jtask );
ds_result_t ds_task_table_remove( struct ds_worker *w, const char *taskid );
ds_result_t ds_task_table_list( struct ds_worker *w, struct jx **result );

/* Act on tasks to move their state machines forward. */
void ds_task_table_advance( struct ds_worker *w );

/* Load all existing tasks from disk. */
void ds_task_table_recover( struct ds_worker *w );

#endif
