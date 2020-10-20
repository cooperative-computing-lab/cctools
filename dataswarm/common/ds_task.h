#ifndef DATASWARM_TASK_H
#define DATASWARM_TASK_H

#include "ds_mount.h"
#include "ds_resources.h"
#include "jx.h"

typedef enum {
	DS_TASK_READY,        /* Task definition has been completed and task is ready to be dispatched/executed */
	DS_TASK_DISPATCHED,   /* Task has been sent to a worker */
	DS_TASK_RUNNING,      /* Task is running at a worker, as reported by the worker */
	DS_TASK_DONE,         /* Task has completed at a worker, as reported by the worker */
	DS_TASK_RETRIEVED,    /* Task result and outputs has been retrieved from the worker */
	DS_TASK_DELETING,     /* Task is being deleted at worker */
	DS_TASK_DELETED,      /* Task has been deleted at worker, as reported by the worker */
	DS_TASK_ERROR         /* Internal error when managing the task */
} ds_task_state_t;

struct ds_task {
	char *command;
	char *taskid;
	ds_task_state_t state;
	struct ds_mount *mounts;
	struct ds_resources *resources;
	struct jx *environment;
};

struct ds_task * ds_task_create( struct jx *jtask );
struct ds_task * ds_task_create_from_file( const char *filename );

struct jx * ds_task_to_jx( struct ds_task *task );
int ds_task_to_file( struct ds_task *task, const char *filename );

const char *ds_task_state_string( ds_task_state_t state );
void ds_task_delete( struct ds_task *t );

#endif
