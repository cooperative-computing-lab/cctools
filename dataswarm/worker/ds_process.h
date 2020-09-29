#ifndef DATASWARM_PROCESS_H
#define DATASWARM_PROCESS_H

#include "../common/ds_task.h"
#include "ds_worker.h"
#include "timestamp.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

/*
ds_process is a running instance of a dataswarm task.
*/

typedef enum {
  DS_PROCESS_READY,
  DS_PROCESS_RUNNING,
  DS_PROCESS_DONE
} ds_process_state_t;

struct ds_process {
	struct ds_task *task;

	// The current state of the process, necessary to make sure that
	// we don't accidentally repeat un-repeatable actions like wait().
	ds_process_state_t state;

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
struct ds_process * ds_process_create( struct ds_task *task, struct ds_worker *worker );

/* Start the process running, return true on success. */
int  ds_process_start( struct ds_process *p, struct ds_worker *w );

/* Send a kill signal to a process (if still running).  After doing so, must call isdone() to collect the status. */
void ds_process_kill( struct ds_process *p );

/* Nonblocking check to see if a process is done.  Returns true if complete. */
int  ds_process_isdone( struct ds_process *p );

/* Delete a process object.  If necessary, will kill and wait for the process. */
void ds_process_delete( struct ds_process *p );

#endif
