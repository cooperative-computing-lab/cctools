/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_PROCESS_H
#define VINE_PROCESS_H

#include "vine_manager.h"
#include "vine_task.h"
#include "vine_cache.h"

#include "timestamp.h"
#include "path_disk_size_info.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

#define MAX_BUFFER_SIZE 4096

/*
vine_process is a running instance of a vine_task.
This object is private to the vine_worker.
*/

struct vine_process {
	pid_t pid;
	vine_result_t result;                // Any of VINE_RESULT_*
	int exit_code;                 // Exit code, or signal number to task process.

	struct rusage rusage;
	timestamp_t execution_start;
	timestamp_t execution_end;

	char *cache_dir;
	char *sandbox;
	char *tmpdir;                   // TMPDIR per task, expected to be a subdir of sandbox.
	char *output_file_name;
	int output_fd;

	struct vine_task *task;

	/* expected disk usage by the process. If no cache is used, it is the same as in task. */
	int64_t disk;

	/* disk size and number of files found in the process sandbox. */
	int64_t sandbox_size;
	int64_t sandbox_file_count;

	/* state between complete disk measurements. */
	struct path_disk_size_info *disk_measurement_state;

	/* variables for coprocess funciton calls */
	struct vine_coprocess *coprocess;
};

struct vine_process * vine_process_create( struct vine_task *task );
pid_t vine_process_execute( struct vine_process *p );
void  vine_process_set_exit_status( struct vine_process *p, int status );
void  vine_process_kill( struct vine_process *p );
void  vine_process_delete( struct vine_process *p );
void  vine_process_compute_disk_needed( struct vine_process *p );

int vine_process_measure_disk(struct vine_process *p, int max_time_on_measurement);
char *vine_process_get_library_name(struct vine_process *p);

int vine_process_execute_and_wait( struct vine_task *task, struct vine_cache *cache, struct link *manager );

#endif
