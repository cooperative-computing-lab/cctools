#ifndef DATASWARM_TASK_H
#define DATASWARM_TASK_H

#include "dataswarm_mount.h"
#include "dataswarm_resources.h"
#include "dataswarm_process.h"
#include "jx.h"

typedef enum {
	DATASWARM_TASK_READY,
	DATASWARM_TASK_RUNNING,
	DATASWARM_TASK_DONE,
	DATASWARM_TASK_FAILED,
	DATASWARM_TASK_KILLED,
	DATASWARM_TASK_DELETING
} dataswarm_task_state_t;

struct dataswarm_task {
	const char *command;
	const char *taskid;
	dataswarm_task_state_t state;
	struct dataswarm_mount *mounts;
	struct dataswarm_resources *resources;
	struct jx *environment;
	struct dataswarm_process *process;
	struct jx *jtask;
};

struct dataswarm_task * dataswarm_task_create( struct jx *jtask );
struct jx * dataswarm_task_to_jx( struct dataswarm_task *task );
void dataswarm_task_delete( struct dataswarm_task *t );

#endif
