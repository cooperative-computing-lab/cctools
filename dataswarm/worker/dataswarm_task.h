#ifndef DATASWARM_TASK_H
#define DATASWARM_TASK_H

#include "dataswarm_mount.h"
#include "dataswarm_resources.h"
#include "jx.h"

struct dataswarm_task {
	const char *command;
	const char *taskid;
	struct dataswarm_mount *mounts;
	struct dataswarm_resources *resources;
	struct jx *environment;
	struct jx *jtask;
};

struct dataswarm_task * dataswarm_task_create( struct jx *jtask );
void dataswarm_task_delete( struct dataswarm_task *t );

#endif
