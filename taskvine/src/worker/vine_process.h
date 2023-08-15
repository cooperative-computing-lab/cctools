/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_PROCESS_H
#define VINE_PROCESS_H

#include "vine_manager.h"
#include "vine_task.h"

#include "timestamp.h"
#include "path_disk_size_info.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

typedef enum {
	VINE_PROCESS_TYPE_STANDARD,   // standard task with command line
	VINE_PROCESS_TYPE_LIBRARY,   // task providing serverless library
	VINE_PROCESS_TYPE_FUNCTION,  // task invoking serverless library
	VINE_PROCESS_TYPE_MINI_TASK, // internal task used to create file
	VINE_PROCESS_TYPE_TRANSFER,  // internal task used to transfer file
} vine_process_type_t;

/*
vine_process is a running instance of a vine_task.
This object is private to the vine_worker.
*/

struct vine_process {
	vine_process_type_t type;
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

	/* If a normal task, the details of the task to execute. */
	struct vine_task *task;

	/* If a function-call task, this is the specific library process to invoke. */
	struct vine_process *library_process;
	
	/* If this is a library process, the links to communicate with the library. */
	struct link *library_read_link;
	struct link *library_write_link;

	/* If this is a library process, the number of functions it is currently running. */
	int functions_running;
	int max_functions_running;
	
	/* expected disk usage by the process. If no cache is used, it is the same as in task. */
	int64_t disk;

	/* disk size and number of files found in the process sandbox. */
	int64_t sandbox_size;
	int64_t sandbox_file_count;

	/* state between complete disk measurements. */
	struct path_disk_size_info *disk_measurement_state;
};

struct vine_process * vine_process_create( struct vine_task *task, vine_process_type_t type );
pid_t vine_process_execute( struct vine_process *p );
int   vine_process_is_complete( struct vine_process *p );
int   vine_process_wait( struct vine_process *p );
void  vine_process_kill( struct vine_process *p );
int   vine_process_kill_and_wait( struct vine_process *p );
void  vine_process_delete( struct vine_process *p );

int   vine_process_execute_and_wait( struct vine_process *p );

void  vine_process_compute_disk_needed( struct vine_process *p );
int   vine_process_measure_disk(struct vine_process *p, int max_time_on_measurement);

#endif
