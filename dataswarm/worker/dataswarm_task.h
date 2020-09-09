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
	DATASWARM_TASK_DELETING,
	DATASWARM_TASK_DELETED
} dataswarm_task_state_t;

struct dataswarm_task {
	char *command;
	char *taskid;
	dataswarm_task_state_t state;
	struct dataswarm_mount *mounts;
	struct dataswarm_resources *resources;
	struct jx *environment;
	struct dataswarm_process *process;
};

struct dataswarm_task * dataswarm_task_create( struct jx *jtask );
struct jx * dataswarm_task_to_jx( struct dataswarm_task *task );
const char *dataswarm_task_state_string( dataswarm_task_state_t state );
void dataswarm_task_delete( struct dataswarm_task *t );

#endif
