#ifndef DATASWARM_PROCESS_H
#define DATASWARM_PROCESS_H

#include "dataswarm_task.h"
#include "timestamp.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

/*
dataswarm_process is a running instance of a dataswarm task.
This object is private to the dataswarm_worker.
*/

struct dataswarm_process {
	struct dataswarm_task *task;

	pid_t pid;
	int task_status;
	int exit_status;

	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;

	char *sandbox;
	char *tmpdir; // TMPDIR per task, expected to be a subdir of sandbox.
};

struct dataswarm_process * dataswarm_process_create( struct dataswarm_task *task );
pid_t dataswarm_process_execute( struct dataswarm_process *p );
void  dataswarm_process_kill( struct dataswarm_process *p );
void  dataswarm_process_delete( struct dataswarm_process *p );

#endif
