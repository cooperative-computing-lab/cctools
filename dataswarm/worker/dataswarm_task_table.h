#ifndef DATASWARM_TASK_TABLE_H
#define DATASWARM_TASK_TABLE_H

#include "dataswarm_message.h"
#include "dataswarm_worker.h"
#include "jx.h"

dataswarm_result_t dataswarm_task_table_submit( struct dataswarm_worker *w, const char *taskid, struct jx *jtask );
dataswarm_result_t dataswarm_task_table_get( struct dataswarm_worker *w, const char *taskid, struct jx **jtask );
dataswarm_result_t dataswarm_task_table_remove( struct dataswarm_worker *w, const char *taskid );

/* Act on tasks to move their state machines forward. */
void dataswarm_task_table_advance( struct dataswarm_worker *w );

/* Load all existing tasks from disk. */
void dataswarm_task_table_recover( struct dataswarm_worker *w );

/* Remove all previously-deleted tasks on startup. */
void dataswarm_task_table_purge( struct dataswarm_worker *w );

#endif
