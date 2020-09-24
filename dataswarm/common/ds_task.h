#ifndef DATASWARM_TASK_H
#define DATASWARM_TASK_H

#include "ds_mount.h"
#include "ds_resources.h"
#include "ds_process.h"
#include "jx.h"

typedef enum {
	DS_TASK_READY,
	DS_TASK_RUNNING,
	DS_TASK_DONE,
	DS_TASK_FAILED,
	DS_TASK_DELETING,
	DS_TASK_DELETED
} ds_task_state_t;

struct ds_task {
	char *command;
	char *taskid;
	ds_task_state_t state;
	struct ds_mount *mounts;
	struct ds_resources *resources;
	struct jx *environment;
	struct ds_process *process;
};

struct ds_task * ds_task_create( struct jx *jtask );
struct jx * ds_task_to_jx( struct ds_task *task );
const char *ds_task_state_string( ds_task_state_t state );
void ds_task_delete( struct ds_task *t );

#endif
