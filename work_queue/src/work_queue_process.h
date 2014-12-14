#ifndef WORK_QUEUE_PROCESS_H
#define WORK_QUEUE_PROCESS_H

#include "work_queue.h"
#include "timestamp.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

#define MAX_BUFFER_SIZE 4096

/*
work_queue_process is a running instance of a work_queue_task.
This object is private to the work_queue_worker.
*/

struct work_queue_process {
	pid_t pid;
	int task_status;                // Any of WORK_QUEUE_RESULT_*
	int exit_status;                // Exit code, or signal number to task process.

	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;

	char *sandbox;

	char *output_file_name;
	int output_fd;

	struct work_queue_task *task;

    // TODO adjust the method
    // struct work_queue_docker_process *docker_proc;

    char container_id[MAX_BUFFER_SIZE];
};

struct work_queue_process * work_queue_process_create( int taskid );
pid_t work_queue_process_execute( struct work_queue_process *p, int container_mode );
void  work_queue_process_kill( struct work_queue_process *p );
void  work_queue_process_delete( struct work_queue_process *p);

#endif
