#ifndef DATASWARM_PROCESS_H
#define DATASWARM_PROCESS_H

#include "dataswarm_task.h"
#include "dataswarm_worker.h"
#include "timestamp.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

/*
dataswarm_process is a running instance of a dataswarm task.
This object is private to the dataswarm_worker.
*/

typedef enum {
  DATASWARM_PROCESS_READY,
  DATASWARM_PROCESS_RUNNING,
  DATASWARM_PROCESS_DONE
} dataswarm_process_state_t;

struct dataswarm_process {
	struct dataswarm_task *task;

	// The current state of the process, necessary to make sure that
	// we don't accidentally repeat un-repeatable actions like wait().
	dataswarm_process_state_t state;

	// The sandbox directory which serves as the working dir for the process.
	char *sandbox;

	// A temp directory within the sandbox, to encourage programs
	// to place temporary data there, instead of in /tmp.
	char *tmpdir;

	// The Unix pid of the process
	// This is valid only if state==RUNNING 
	pid_t pid;

	// The Unix exit status (parse with WIFEXITED() etc)
	// This is valid only if state==DONE.
	int unix_status;

	// The resource consumption of the process.
	// This is valid only if state==DONE
	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;

};

/* Create a new process for this task and set up the corresponding sandbox. */
struct dataswarm_process * dataswarm_process_create( struct dataswarm_task *task, struct dataswarm_worker *w );

/* Start the process running, return true on success. */
int  dataswarm_process_start( struct dataswarm_process *p );

/* Send a kill signal to a process (if still running).  After doing so, must call isdone() to collect the status. */
void dataswarm_process_kill( struct dataswarm_process *p );

/* Nonblocking check to see if a process is done.  Returns true if complete. */
int  dataswarm_process_isdone( struct dataswarm_process *p );

/* Delete a process object.  If necessary, will kill and wait for the process. */
void dataswarm_process_delete( struct dataswarm_process *p );

#endif
