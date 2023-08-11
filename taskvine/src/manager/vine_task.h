/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TASK_H
#define VINE_TASK_H

/*
This module defines the internal structure and details of a single task.
Note that these details are internal to the manager library,
and are not for public consumption.
End user may only use the API described in taskvine.h
*/

#include "taskvine.h"

#include "list.h"
#include "category.h"

#include <stdint.h>

typedef enum {
      VINE_TASK_TYPE_STANDARD,    /**< A normal task that should be returned to the user. */
      VINE_TASK_TYPE_RECOVERY,    /**< An internally-created recovery task that should not be returned to the user. */
      VINE_TASK_TYPE_LIBRARY,     /**< An internally-created library task that should not be returned to the user. */
} vine_task_type_t;

struct vine_task {
        /***** Fixed properties of task at submit time. ******/

        int task_id;                 /**< A unique task id number. */
	vine_task_type_t type;       /**< The type of the task. */
	char *command_line;          /**< The program(s) to execute, as a shell command line. */
	char *tag;                   /**< An optional user-defined logical name for the task. */
	char *category;              /**< User-provided label for the task. It is expected that all task with the same category will have similar resource usage. See @ref vine_task_set_category. If no explicit category is given, the label "default" is used. **/

	char *monitor_output_directory;	     /**< Custom output directory for the monitoring output files. If NULL, save to directory from @ref vine_enable_monitoring */
	struct vine_file *monitor_snapshot_file;  /**< Filename the monitor checks to produce snapshots. */

	char *needs_library;         /**< If this is a FunctionTask, the name of the library used */
	char *provides_library;      /**< If this is a LibraryTask, the name of the library provided. */

	struct list *input_mounts;    /**< The mounted files expected as inputs. */
	struct list *output_mounts;   /**< The mounted files expected as outputs. */
	struct list *env_list;       /**< Environment variables applied to the task. */
	struct list *feature_list;   /**< User-defined features this task requires. (See vine_worker's --feature option.) */

	category_allocation_t resource_request; /**< See @ref category_allocation_t */
	vine_schedule_t worker_selection_algorithm; /**< How to choose worker to run the task. */
	double priority;             /**< The priority of this task relative to others in the queue: higher number run earlier. */
	int max_retries;             /**< Number of times the task is tried to be executed on some workers until success. If less than one, the task is retried indefinitely. See try_count below.*/
	int64_t min_running_time;    /**< Minimum time (in seconds) the task needs to run. (see vine_worker --wall-time)*/

	/***** Internal state of task as it works towards completion. *****/

	vine_task_state_t state;       /**< Current state of task: READY, RUNNING, etc */
	struct vine_worker_info *worker;    /**< Worker to which this task has been dispatched. */
	int try_count;               /**< The number of times the task has been dispatched to a worker. If larger than max_retries, the task failes with @ref VINE_RESULT_MAX_RETRIES. */
	int exhausted_attempts;      /**< Number of times the task failed given exhausted resources. */
	int workers_slow;            /**< Number of times this task has been terminated for running too long. */

	/***** Results of task once it has reached completion. *****/

	vine_result_t result;          /**< The result of the task (see @ref vine_result_t */
	int exit_code;               /**< The exit code of the command line. */
	char *output;                /**< The standard output of the task. */
	char *addrport;              /**< The address and port of the host on which it ran. */
	char *hostname;              /**< The name of the host on which it ran. */

	/***** Metrics available to the user at completion through vine_task_get_metric.  *****/
	/* All times in microseconds */
	/* A time_when_* refers to an instant in time, otherwise it refers to a length of time. */

	timestamp_t time_when_submitted;    /**< The time at which this task was added to the queue. */
	timestamp_t time_when_done;         /**< The time at which the task is mark as retrieved, after transfering output files and other final processing. */

	timestamp_t time_when_commit_start; /**< The time when the task starts to be transfered to a worker. */
	timestamp_t time_when_commit_end;   /**< The time when the task is completely transfered to a worker. */

	timestamp_t time_when_retrieval;    /**< The time when output files start to be transfered back to the manager. time_done - time_when_retrieval is the time taken to transfer output files. */

    timestamp_t time_workers_execute_last_start;           /**< The time when the last complete execution for this task started at a worker. */
    timestamp_t time_workers_execute_last_end;             /**< The time when the last complete execution for this task ended at a worker. */

	timestamp_t time_workers_execute_last;                 /**< Duration of the last complete execution for this task. */
	timestamp_t time_workers_execute_all;                  /**< Accumulated time for executing the command on any worker, regardless of whether the task completed (i.e., this includes time running on workers that disconnected). */
	timestamp_t time_workers_execute_exhaustion;           /**< Accumulated time spent in attempts that exhausted resources. */
	timestamp_t time_workers_execute_failure;              /**< Accumulated time for runs that terminated in worker failure/disconnection. */

	int64_t bytes_received;                                /**< Number of bytes received since task has last started receiving input data. */
	int64_t bytes_sent;                                    /**< Number of bytes sent since task has last started sending input data. */
	int64_t bytes_transferred;                             /**< Number of bytes transferred since task has last started transferring input data. */

	struct rmsummary *resources_allocated;                 /**< Resources allocated to the task its latest attempt. */
	struct rmsummary *resources_measured;                  /**< When monitoring is enabled, it points to the measured resources used by the task in its latest attempt. */
	struct rmsummary *resources_requested;                 /**< Number of cores, disk, memory, time, etc. the task requires. */
	struct rmsummary *current_resource_box;                /**< Resources allocated to the task on this specific worker. */
		
	int has_fixed_locations;                               /**< Whether at least one file was added with the VINE_FIXED_LOCATION flag. Task fails immediately if no
															 worker can satisfy all the strict inputs of the task. */

	int refcount;                                          /**< Number of remaining references to this object. */
};

/* Add a reference to an existing task object, return the same object. */
struct vine_task * vine_task_clone( struct vine_task *t );

/* Deep-copy an existing task object, return a pointer to a new object. */
struct vine_task * vine_task_copy( const struct vine_task *t );

/* Hard-reset a completed task back to an initial state so that it can be submitted again. */
void vine_task_reset( struct vine_task *t );

/* Soft-reset a not-yet-completed task so that it can be attempted on a different worker. */
void vine_task_clean( struct vine_task *t );

int  vine_task_set_result(struct vine_task *t, vine_result_t new_result);
void vine_task_set_resources(struct vine_task *t, const struct rmsummary *rm);

void vine_task_check_consistency( struct vine_task *t );

const char *vine_task_state_to_string( vine_task_state_t task_state );

struct jx * vine_task_to_jx( struct vine_manager *q, struct vine_task *t );
char * vine_task_to_json(struct vine_task *t);


/** Attach an input or outputs to tasks without declaring files to manager.
 * Only really useful at the worker where tasks are created without a manager. */
int vine_task_add_input_file(struct vine_task *t, const char *local_name, const char *remote_name, vine_mount_flags_t flags);
int vine_task_add_output_file(struct vine_task *t, const char *local_name, const char *remote_name, vine_mount_flags_t flags);
int vine_task_add_input_url(struct vine_task *t, const char *url, const char *remote_name, vine_mount_flags_t flags);
int vine_task_add_input_mini_task(struct vine_task *t, struct vine_task *mini_task, const char *remote_name, vine_mount_flags_t flags);
int vine_task_add_input_buffer(struct vine_task *t, const char *data, int length, const char *remote_name, vine_mount_flags_t flags);
int vine_task_add_empty_dir( struct vine_task *t, const char *remote_name );

#endif
