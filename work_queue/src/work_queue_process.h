#ifndef WORK_QUEUE_PROCESS_H
#define WORK_QUEUE_PROCESS_H

#include "work_queue.h"
#include "timestamp.h"
#include "path_disk_size_info.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

#define NONE 0
#define DOCKER 1
#define DOCKER_PRESERVE 2
#define UMBRELLA 3

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

	/* expected disk usage by the process. If no cache is used, it is the same as in task. */
	int64_t disk;
	int loop_mount; /**< 1 if the task sandbox was mounted on a loop device. 0 otherwise. */

	/* disk size and number of files found in the process sandbox. */
	int64_t sandbox_size;
	int64_t sandbox_file_count;

	/* state between complete disk measurements. */
	struct path_disk_size_info *disk_measurement_state;

	char container_id[MAX_BUFFER_SIZE];
};

struct work_queue_process * work_queue_process_create( struct work_queue_task *task, int disk_allocation );
pid_t work_queue_process_execute( struct work_queue_process *p, int container_mode, ... );
// lunching process with container, arg_3 can be either img_name or container_name, depending on container_mode
void  work_queue_process_kill( struct work_queue_process *p );
void  work_queue_process_delete( struct work_queue_process *p );
void  work_queue_process_compute_disk_needed( struct work_queue_process *p );

int work_queue_process_measure_disk(struct work_queue_process *p, int max_time_on_measurement);

#endif
