#ifndef DS_PROCESS_H
#define DS_PROCESS_H

#include "ds_manager.h"
#include "timestamp.h"
#include "path_disk_size_info.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

#define MAX_BUFFER_SIZE 4096

/*
ds_process is a running instance of a ds_task.
This object is private to the ds_worker.
*/

struct ds_process {
	pid_t pid;
	int task_status;                // Any of DS_RESULT_*
	int exit_status;                // Exit code, or signal number to task process.

	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;

	char *cache_dir;
	char *sandbox;
	char *tmpdir;                   // TMPDIR per task, expected to be a subdir of sandbox.
	char *output_file_name;
	int output_fd;

	struct ds_task *task;

	/* expected disk usage by the process. If no cache is used, it is the same as in task. */
	int64_t disk;

	/* disk size and number of files found in the process sandbox. */
	int64_t sandbox_size;
	int64_t sandbox_file_count;

	/* state between complete disk measurements. */
	struct path_disk_size_info *disk_measurement_state;

	/* variables for coprocess funciton calls */
	char *coprocess_name;
	int coprocess_port;
};

struct ds_process * ds_process_create( struct ds_task *task );
pid_t ds_process_execute( struct ds_process *p );
void  ds_process_kill( struct ds_process *p );
void  ds_process_delete( struct ds_process *p );
void  ds_process_compute_disk_needed( struct ds_process *p );

int ds_process_measure_disk(struct ds_process *p, int max_time_on_measurement);

#endif
