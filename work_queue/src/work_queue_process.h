#ifndef WORK_QUEUE_PROCESS_H
#define WORK_QUEUE_PROCESS_H

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>
#include "timestamp.h"

/*
work_queue_process is a running instance of a work_queue_task.
This object is private to the work_queue_worker.
*/

struct work_queue_process {
	pid_t pid;
	int status;
	
	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;
	
	char *output_file_name;
	int output_fd;
	struct work_queue_task *task;
};

struct work_queue_process * work_queue_process_create( struct work_queue_task *t );
pid_t work_queue_process_execute( const char *cmd, struct work_queue_process *p );
void  work_queue_process_kill( struct work_queue_process *p );
void  work_queue_process_delete( struct work_queue_process *p );

#endif
