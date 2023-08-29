/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TASKVINE_H
#define TASKVINE_H

#include <sys/types.h>
#include "timestamp.h"
#include "category.h"
#include "rmsummary.h"

struct vine_manager;
struct vine_task;
struct vine_file;

/** @file taskvine.h The public API for the taskvine distributed application framework.
A taskvine application consists of a manager process and a larger number of worker
processes, typically running in a high performance computing cluster, or a cloud facility.
Both the manager and worker processes run with ordinary user privileges and require
no special capabilities.

From the application perspective, the programmer creates a manager with @ref vine_create,
defines a number of tasks with @ref vine_task_create, submits the tasks to the manager
with @ref vine_submit, and then monitors completion with @ref vine_wait.
Tasks are further described by attaching data objects via @ref vine_task_add_input,
@ref vine_task_add_output and related functions.

The taskvine framework provides a large number of fault tolerance, resource management,
and performance monitoring features that enable the construction of applications that
run reliably on tens of thousands of nodes in the presence of failures and other
expected events.
*/

#define VINE_DEFAULT_PORT 9123               /**< Default taskvine port number. */
#define VINE_RANDOM_PORT  0                  /**< Indicates that any port may be chosen. */
#define VINE_WAIT_FOREVER -1                 /**< Timeout value to wait for a task to complete before returning. */

/** Select optional handling for input and output files: caching, unpacking, watching, etc. **/

typedef enum {
	VINE_TRANSFER_ALWAYS = 0, /**< Always transfer this file when needed. */
	VINE_FIXED_LOCATION  = 1,   /**< Never transfer input files with this flag to a worker for execution. Task won't be dispatched to a worker unless file is already cached there.*/
	VINE_WATCH = 2,           /**< Watch the output file and send back changes as the task runs. */
	VINE_FAILURE_ONLY = 4,    /**< Only return this output file if the task failed.  (Useful for returning large log files.) */
	VINE_SUCCESS_ONLY = 8,    /**< Only return this output file if the task succeeded. */
} vine_mount_flags_t;

/** Control caching and sharing behavior of file objects.
Note that these bit fields overlap.
To see if file should be cached, use: (flags & VINE_CACHE).
To see if file should remain at worker after disconnection, use ((flags & VINE_CACHE_ALWAYS) == VINE_CACHE_ALWAYS).
**/

typedef enum {
	VINE_CACHE_NEVER = 0,  /**< Do not cache file at execution site. (default) */
	VINE_CACHE = 1,        /**< File remains in cache until workflow ends. */
	VINE_CACHE_ALWAYS = 3, /**< File remains in cache until the worker teminates. **/
	VINE_PEER_NOSHARE = 4  /**< Schedule this file to be shared between peers where available. See @ref vine_enable_peer_transfers **/
} vine_file_flags_t;

/** Select overall scheduling algorithm for matching tasks to workers. */

typedef enum {
	VINE_SCHEDULE_UNSET = 0, /**< Internal use only. */
	VINE_SCHEDULE_FCFS,      /**< Select worker on a first-come-first-serve basis. */
	VINE_SCHEDULE_FILES,     /**< Select worker that has the most data required by the task. (default) */
	VINE_SCHEDULE_TIME,      /**< Select worker that has the fastest execution time on previous tasks. */
	VINE_SCHEDULE_RAND,      /**< Select a random worker. */
	VINE_SCHEDULE_WORST      /**< Select the worst fit worker (the worker with more unused resources). */
} vine_schedule_t;

/** Possible outcomes for a task, returned by @ref vine_task_get_result.
These results can be converted to a string with @ref vine_result_string.
*/

typedef enum {
	VINE_RESULT_SUCCESS             = 0,      /**< The task ran successfully, and its Unix exit code is given by @ref vine_task_get_exit_code */
	VINE_RESULT_INPUT_MISSING       = 1,      /**< The task cannot be run due to a missing input file **/
	VINE_RESULT_OUTPUT_MISSING      = 2,      /**< The task ran but failed to generate a specified output file **/
	VINE_RESULT_STDOUT_MISSING      = 4,      /**< The task ran but its stdout has been truncated **/
	VINE_RESULT_SIGNAL              = 1 << 3, /**< The task was terminated with a signal **/
	VINE_RESULT_RESOURCE_EXHAUSTION = 2 << 3, /**< The task used more resources than requested **/
	VINE_RESULT_MAX_END_TIME        = 3 << 3, /**< The task ran after the specified (absolute since epoch) end time. **/
	VINE_RESULT_UNKNOWN             = 4 << 3, /**< The result could not be classified. **/
	VINE_RESULT_FORSAKEN            = 5 << 3, /**< The task failed, but it was not a task error **/
	VINE_RESULT_MAX_RETRIES         = 6 << 3, /**< The task could not be completed successfully in the given number of retries. **/
	VINE_RESULT_MAX_WALL_TIME       = 7 << 3, /**< The task ran for more than the specified time (relative since running in a worker). **/
	VINE_RESULT_RMONITOR_ERROR      = 8 << 3, /**< The task failed because the monitor did not produce a summary report. **/
	VINE_RESULT_OUTPUT_TRANSFER_ERROR = 9 << 3,  /**< The task failed because an output could be transfered to the manager (not enough disk space, incorrect write permissions. */
	VINE_RESULT_FIXED_LOCATION_MISSING = 10 << 3 /**< The task failed because no worker could satisfy the fixed location input file requirements. */
} vine_result_t;

/** Possible states of a task, given by @ref vine_task_state */

typedef enum {
	VINE_TASK_UNKNOWN = 0,       /**< Task has not been submitted to the manager **/
	VINE_TASK_READY,             /**< Task is ready to be run, waiting in manager **/
	VINE_TASK_RUNNING,           /**< Task has been dispatched to some worker **/
	VINE_TASK_WAITING_RETRIEVAL, /**< Task results are available at the worker **/
	VINE_TASK_RETRIEVED,         /**< Task results are available at the manager **/
	VINE_TASK_DONE,              /**< Task is done, and returned through vine_wait >**/
	VINE_TASK_CANCELED,           /**< Task was canceled before completion **/
} vine_task_state_t;

/** Select how to allocate resources for similar tasks with @ref vine_set_category_mode */

typedef enum {
	/** When monitoring is disabled, all tasks run as VINE_ALLOCATION_MODE_FIXED.
	If monitoring is enabled and resource exhaustion occurs for specified
	resources values, then the task permanently fails. */
	VINE_ALLOCATION_MODE_FIXED = CATEGORY_ALLOCATION_MODE_FIXED,

	/** When monitoring is enabled, tasks are tried with maximum specified
	values of cores, memory, disk or gpus until enough statistics are collected.
	Then, further tasks are first tried using the maximum values observed,
	and in case of resource exhaustion, they are retried using the maximum
	specified values. The task permanently fails when there is an exhaustion
	using the maximum values. If no maximum values are specified,
	the task will wait until a larger worker connects. */
	VINE_ALLOCATION_MODE_MAX = CATEGORY_ALLOCATION_MODE_MAX,

	/** As above, but tasks are first tried with an automatically computed
	    allocation to minimize resource waste. */
	VINE_ALLOCATION_MODE_MIN_WASTE = CATEGORY_ALLOCATION_MODE_MIN_WASTE,

	/** As above, but maximizing throughput. */
	VINE_ALLOCATION_MODE_MAX_THROUGHPUT = CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT,

    VINE_ALLOCATION_MODE_GREEDY_BUCKETING = CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING,

    VINE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING = CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING
} vine_category_mode_t;

/** Statistics describing a manager. */

struct vine_stats {
	/* Stats for the current state of workers: */
	int workers_connected;	  /**< Number of workers currently connected to the manager. */
	int workers_init;         /**< Number of workers connected, but that have not send their available resources report yet.*/
	int workers_idle;         /**< Number of workers that are not running a task. */
	int workers_busy;         /**< Number of workers that are running at least one task. */
	int workers_able;         /**< Number of workers on which the largest task can run. */

	/* Cumulative stats for workers: */
	int workers_joined;       /**< Total number of worker connections that were established to the manager. */
	int workers_removed;      /**< Total number of worker connections that were terminated. */
	int workers_released;     /**< Total number of worker connections that were asked by the manager to disconnect. */
	int workers_idled_out;    /**< Total number of worker that disconnected for being idle. */
	int workers_slow;         /**< Total number of workers disconnected for being too slow. (see @ref vine_enable_disconnect_slow_workers) */
	int workers_blocked ;     /**< Total number of workers blocked by the manager. (Includes workers_slow.) */
	int workers_lost;         /**< Total number of worker connections that were unexpectedly lost. (does not include workers_idle_out or workers_slow) */

	/* Stats for the current state of tasks: */
	int tasks_waiting;        /**< Number of tasks waiting to be dispatched. */
	int tasks_on_workers;     /**< Number of tasks currently dispatched to some worker. */
	int tasks_running;        /**< Number of tasks currently executing at some worker. */
	int tasks_with_results;   /**< Number of tasks with retrieved results and waiting to be returned to user. */

	/* Cumulative stats for tasks: */
	int tasks_submitted;           /**< Total number of tasks submitted to the manager. */
	int tasks_dispatched;          /**< Total number of tasks dispatch to workers. */
	int tasks_done;                /**< Total number of tasks completed and returned to user. (includes tasks_failed) */
	int tasks_failed;              /**< Total number of tasks completed and returned to user with result other than VINE_RESULT_SUCCESS. */
	int tasks_cancelled;           /**< Total number of tasks cancelled. */
	int tasks_exhausted_attempts;  /**< Total number of task executions that failed given resource exhaustion. */

	/* All times in microseconds */
	/* A time_when_* refers to an instant in time, otherwise it refers to a length of time. */

	/* Master time statistics: */
	timestamp_t time_when_started; /**< Absolute time at which the manager started. */
	timestamp_t time_send;         /**< Total time spent in sending tasks to workers (tasks descriptions, and input files.). */
	timestamp_t time_receive;      /**< Total time spent in receiving results from workers (output files.). */
	timestamp_t time_send_good;    /**< Total time spent in sending data to workers for tasks with result VINE_RESULT_SUCCESS. */
	timestamp_t time_receive_good; /**< Total time spent in sending data to workers for tasks with result VINE_RESULT_SUCCESS. */
	timestamp_t time_status_msgs;  /**< Total time spent sending and receiving status messages to and from workers, including workers' standard output, new workers connections, resources updates, etc. */
	timestamp_t time_internal;     /**< Total time the manager spents in internal processing. */
	timestamp_t time_polling;      /**< Total time blocking waiting for worker communications (i.e., manager idle waiting for a worker message). */
	timestamp_t time_application;  /**< Total time spent outside vine_wait. */
	timestamp_t time_scheduling;   /**< Total time spend matching tasks to workers. */

	/* Workers time statistics: */
	timestamp_t time_workers_execute;            /**< Total time workers spent executing done tasks. */
	timestamp_t time_workers_execute_good;       /**< Total time workers spent executing done tasks with result VINE_RESULT_SUCCESS. */
	timestamp_t time_workers_execute_exhaustion; /**< Total time workers spent executing tasks that exhausted resources. */

	/* BW statistics */
	int64_t bytes_sent;     /**< Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the manager. */
	int64_t bytes_received; /**< Total number of file bytes (not including protocol control msg bytes) received from the workers by the manager. */
	double  bandwidth;      /**< Average network bandwidth in MB/S observed by the manager when transferring to workers. */

	/* resources statistics */
	int capacity_tasks;     /**< The estimated number of tasks that this manager can effectively support. */
	int capacity_cores;     /**< The estimated number of workers' cores that this manager can effectively support.*/
	int capacity_memory;    /**< The estimated number of workers' MB of RAM that this manager can effectively support.*/
	int capacity_disk;      /**< The estimated number of workers' MB of disk that this manager can effectively support.*/
	int capacity_gpus;      /**< The estimated number of workers' GPUs that this manager can effectively support.*/
	int capacity_instantaneous;      /**< The estimated number of tasks that this manager can support considering only the most recently completed task. */
	int capacity_weighted;  /**< The estimated number of tasks that this manager can support placing greater weight on the most recently completed task. */

	int64_t total_cores;      /**< Total number of cores aggregated across the connected workers. */
	int64_t total_memory;     /**< Total memory in MB aggregated across the connected workers. */
	int64_t total_disk;	      /**< Total disk space in MB aggregated across the connected workers. */
	int64_t total_gpus;       /**< Total number of gpus aggregated across the connected workers. */

	int64_t committed_cores;  /**< Committed number of cores aggregated across the connected workers. */
	int64_t committed_memory; /**< Committed memory in MB aggregated across the connected workers. */
	int64_t committed_disk;	  /**< Committed disk space in MB aggregated across the connected workers. */
	int64_t committed_gpus;   /**< Committed number of gpus aggregated across the connected workers. */

	int64_t max_cores;        /**< The highest number of cores observed among the connected workers. */
	int64_t max_memory;       /**< The largest memory size in MB observed among the connected workers. */
	int64_t max_disk;         /**< The largest disk space in MB observed among the connected workers. */
	int64_t max_gpus;         /**< The highest number of gpus observed among the connected workers. */

	int64_t min_cores;        /**< The lowest number of cores observed among the connected workers. */
	int64_t min_memory;       /**< The smallest memory size in MB observed among the connected workers. */
	int64_t min_disk;         /**< The smallest disk space in MB observed among the connected workers. */
	int64_t min_gpus;         /**< The smallest number of gpus observed among the connected workers. */

	double manager_load;      /**< In the range of [0,1]. If close to 1, then
                                the manager is at full load and spends most
                                of its time sending and receiving taks, and
                                thus cannot accept connections from new
                                workers. If close to 0, the manager is spending
                                most of its time waiting for something to happen. */
};

/** @name Functions - Tasks */

//@{

/** Create a new task object.
Once created and elaborated with functions such as @ref vine_task_add_input
and @ref vine_task_add_output, the task should be passed to @ref vine_submit.
@param full_command The shell command line or coprocess functions to be
executed by the task.  If null, the command will be given later by @ref
vine_task_set_command
@return A new task object, or null if it could not be created.
*/
struct vine_task *vine_task_create(const char *full_command);

/** Delete a task.
This may be called on tasks after they are returned from @ref vine_wait.
@param t The task to delete.
*/
void vine_task_delete(struct vine_task *t);

/** Indicate the command to be executed.
@param t A task object.
@param cmd The command to be executed.  This string will be duplicated by this call, so the argument may be freed or re-used afterward.
*/
void vine_task_set_command( struct vine_task *t, const char *cmd );

/** Set the library name required by this task.
@param t A task object.
@param name The name of the library coprocess name that will be used by this task.
*/
void vine_task_needs_library( struct vine_task *t, const char *name );

/** Set the library name provided by this task.
@param t A task object.
@param name The name of the library coprocess that this task implements.
*/
void vine_task_provides_library( struct vine_task *t, const char *name );


/** Set the number of concurrent functions a library can run.
@param t A task object.
@param nslots The maximum number of concurrent functions this library can run.
*/
void vine_task_set_function_slots( struct vine_task *t, int nslots );


/** Add a general file object as a input to a task.
@param t A task object.
@param f A file object, created by @ref vine_declare_file, @ref vine_declare_url, @ref vine_declare_buffer, @ref vine_declare_mini_task.
@param remote_name The name of the file as it should appear in the task's sandbox.
@param flags May be zero or more @ref vine_mount_flags_t or'd together. See @ref vine_task_add_input.
@return True on success, false on failure.
*/

int vine_task_add_input( struct vine_task *t, struct vine_file *f, const char *remote_name, vine_mount_flags_t flags );

/** Add a general file object as a output of a task.
@param t A task object.
@param f A file object, created by @ref vine_declare_file or @ref vine_declare_buffer.
@param remote_name The name of the file as it will appear in the task's sandbox.
@param flags May be zero or more @ref vine_mount_flags_t or'd together. See @ref vine_task_add_input.
@return True on success, false on failure.
*/

int vine_task_add_output( struct vine_task *t, struct vine_file *f, const char *remote_name, vine_mount_flags_t flags );

/** Specify the number of times this task is retried on worker errors. If less than one, the task is retried indefinitely (this the default). A task that did not succeed after the given number of retries is returned with result VINE_RESULT_MAX_RETRIES.
@param t A task object.
@param max_retries The number of retries.
*/

void vine_task_set_retries( struct vine_task *t, int64_t max_retries );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param memory The amount of disk space required by the task, in megabytes.
*/

void vine_task_set_memory( struct vine_task *t, int64_t memory );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param disk The amount of disk space required by the task, in megabytes.
*/

void vine_task_set_disk( struct vine_task *t, int64_t disk );

/** Specify the number of cores required by a task.
@param t A task object.
@param cores The number of cores required by the task.
*/

void vine_task_set_cores( struct vine_task *t, int cores );

/** Specify the number of gpus required by a task.
@param t A task object.
@param gpus The number of gpus required by the task.
*/

void vine_task_set_gpus( struct vine_task *t, int gpus );

/** Specify the maximum end time allowed for the task (in microseconds since the
Epoch). If less than 1, then no end time is specified (this is the default).
This is useful, for example, when the task uses certificates that expire.
@param t A task object.
@param useconds Number of useconds since the Epoch.
*/

void vine_task_set_time_end( struct vine_task *t, int64_t useconds );

/** Specify the minimum start time allowed for the task (in microseconds since
 the Epoch). The task will only be submitted to workers after the specified time.
 If less than 1, then no minimum start time is specified (this is the default).
@param t A task object.
@param useconds Number of useconds since the Epoch.
*/

void vine_task_set_time_start( struct vine_task *t, int64_t useconds );

/** Specify the maximum time (in seconds) the task is allowed to run in a
worker. This time is accounted since the the moment the task starts to run
in a worker.  If less than 1, then no maximum time is specified (this is the default).
@param t A task object.
@param seconds Maximum number of seconds the task may run in a worker.
*/

void vine_task_set_time_max( struct vine_task *t, int64_t seconds );

/** Specify the minimum time (in seconds) the task is expected to run in a worker.
This time is accounted since the moment the task starts to run in a worker.
If less than 1, then no minimum time is specified (this is the default).
@param t A task object.
@param seconds Minimum number of seconds the task may run in a worker.
*/
void vine_task_set_time_min( struct vine_task *t, int64_t seconds );

/** Attach a user defined string tag to the task.
This field is not interpreted by the manager, but is provided for the user's convenience
in identifying tasks when they complete.
@param t A task object.
@param tag The tag to attach to task t.
*/
void vine_task_set_tag(struct vine_task *t, const char *tag);

/** Label the task with the given category. It is expected that tasks with the same category
have similar resources requirements (e.g. to disconnect slow workers).
@param t A task object.
@param category The name of the category to use.
*/
void vine_task_set_category(struct vine_task *t, const char *category);

/** Label the task with a user-defined feature. The task will only run on a worker that provides (--feature option) such feature.
@param t A task object.
@param name The name of the feature.
*/
void vine_task_add_feature(struct vine_task *t, const char *name);

/** Specify the priority of this task relative to others in the manager.
Tasks with a higher priority value run first. If no priority is given, a task is placed at the end of the ready list, regardless of the priority.
@param t A task object.
@param priority The priority of the task.
*/

void vine_task_set_priority(struct vine_task *t, double priority );

/** Specify an environment variable to be added to the task.
@param t A task object
@param name Name of the variable.
@param value Value of the variable.
*/
void vine_task_set_env_var( struct vine_task *t, const char *name, const char *value );

/** Select the scheduling algorithm for a single task.
To change the scheduling algorithm for all tasks, use @ref vine_set_scheduler instead.
@param t A task object.
@param algorithm The algorithm to use in assigning this task to a worker. For possible values, see @ref vine_schedule_t.
*/
void vine_task_set_scheduler(struct vine_task *t, vine_schedule_t algorithm);

/** Specify a custom name for the monitoring summary. If @ref vine_enable_monitoring is also enabled, the summary is also written to that directory.
@param t A task object.
@param monitor_output Resource summary file.
@return True on success, false on failure.
*/

int vine_task_set_monitor_output(struct vine_task *t, const char *monitor_output);

/** Get the command line of the task.
@param t A task object.
@return The command line set by @ref vine_task_create.
*/

const char * vine_task_get_command( struct vine_task *t );

/** Get the tag associated with the task.
@param t A task object.
@return The tag string set by @ref vine_task_set_tag.
*/

const char * vine_task_get_tag( struct vine_task *t );

/** Get the category associated with the task.
@param t A task object.
@return The category string set by @ref vine_task_set_category.
*/

const char * vine_task_get_category( struct vine_task *t );

/** Get the unique ID of the task.
@param t A task object.
@return The integer task ID assigned at creation time.
*/

int vine_task_get_id( struct vine_task *t );

/** Get the end result of the task.
If the result is @ref VINE_RESULT_SUCCESS, then the
task ran to completion and the exit code of the process
can be obtained from @ref vine_task_get_exit_code.
For any other result, the task could not be run to
completion.  Use @ref vine_result_string to convert the
result code into a readable string.
@param t A task object.
@return The result of the task as a vine_result_t.
*/

vine_result_t vine_task_get_result( struct vine_task *t );

/** Explain result codes from tasks.
@param result Result from a task returned by @ref vine_wait.
@return String representation of task result code.
*/
const char *vine_result_string(vine_result_t result);


/** Get the Unix exit code of the task.
@param t A task object.
@return If the task ran to completion and the result
is @ref VINE_RESULT_SUCCESS, then this function returns
the Unix exit code of the process, which by custom
is zero to indicate success, and non-zero to indicate failure.
*/

int vine_task_get_exit_code( struct vine_task *t );

/** Get the standard output of the task.
@param t A task object.
@return A null-terminated string containing the standard
output of the task.  If the task did not run to completion,
then this function returns null.
*/

const char * vine_task_get_stdout( struct vine_task *t );

/** Get the address and port of the worker on which the task ran.
@param t A task object.
@return A null-terminated string containing the address
and port of the relevant worker. If the task did not run
on a worker,  then this function returns null.
*/

const char * vine_task_get_addrport( struct vine_task *t );

/** Get the hostname of the worker on which the task ran.
@param t A task object.
@return A null-terminated string containing the hostname
of the relevant worker. If the task did not run
on a worker,  then this function returns null.
*/

const char * vine_task_get_hostname( struct vine_task *t );

/** Get a performance metric of a completed task.
@param t A task object.
@param name The name of an integer performance metric:
- "time_when_submitted"
- "time_when_done"
- "time_when_commit_start"
- "time_when_commit_end"
- "time_when_retrieval"
- "time_workers_execute_last"
- "time_workers_execute_all"
- "time_workers_execute_exhaustion"
- "time_workers_execute_failure"
- "bytes_received"
- "bytes_sent"
- "bytes_transferred"
@return The metric value, or zero if an invalid name is given.
*/

int64_t vine_task_get_metric( struct vine_task *t, const char *name );

/** Set the expected resource consumption of a task before execution.
@param t A task object.
@param rm A resource summary object.
*/

void vine_task_set_resources(struct vine_task *t, const struct rmsummary *rm );

/** Get resource information (e.g., cores, memory, and disk) of a completed task.
@param t A task object.
@param name One of: "allocated", "requested", or "measured". For measured resources see @ref vine_enable_monitoring.
@return The metric value, or zero if an invalid name is given.
*/

const struct rmsummary *vine_task_get_resources( struct vine_task *t, const char *name );

/** When monitoring, indicates a json-encoded file that instructs the
monitor to take a snapshot of the task resources. Snapshots appear in the JSON
summary file of the task, under the key "snapshots". Snapshots are taken on
events on files described in the monitor_snapshot_file. The monitor_snapshot_file
is a json encoded file with the following format:

    {
        "FILENAME": {
            "from-start":boolean,
            "from-start-if-truncated":boolean,
            "delete-if-found":boolean,
            "events": [
                {
                    "label":"EVENT_NAME",
                    "on-create":boolean,
                    "on-truncate":boolean,
                    "pattern":"REGEXP",
                    "count":integer
                },
                {
                    "label":"EVENT_NAME",
                    ...
                }
            ]
        },
        "FILENAME": {
            ...
    }

All fields but label are optional.

            from-start:boolean         If FILENAME exits when task starts running, process from line 1. Default: false, as the task may be appending to an already existing file.
            from-start-if-truncated    If FILENAME is truncated, process from line 1. Default: true, to account for log rotations.
            delete-if-found            Delete FILENAME when found. Default: false

            events:
            label        Name that identifies the snapshot. Only alphanumeric, -,
                         and _ characters are allowed.
            on-create    Take a snapshot every time the file is created. Default: false
            on-truncate  Take a snapshot when the file is truncated.    Default: false
            pattern      Take a snapshot when a line matches the regexp pattern.    Default: none
            count        Maximum number of snapshots for this label. Default: -1 (no limit)

For more information, consult the manual of the resource_monitor.

@param t A vine_task object.
@param monitor_snapshot_file A filename.
*/

int vine_task_set_snapshot_file(struct vine_task *t, struct vine_file *monitor_snapshot_file);


/** Adds an execution environment to the task. The environment file specified
is expected to expand to a directory with a bin/run_in_env file that will
wrap the task command (e.g. a poncho, starch file, or any other vine mini_task
that creates such a wrapper). If specified multiple times, environments are
nested in the order given (i.e. first added is the first applied).
@param t A task object.
@param f The environment file.
*/

int vine_task_add_environment(struct vine_task *t, struct vine_file *f);


//@}

/** @name Functions - Files */

//@{

/** Get the contents of a vine file.
Typically used to examine an output buffer returned from a file.
Note that the vfile contents may not be available unless @ref vine_fetch_file
has previously been called on this object.
@param m A manager object
@param f A file object created by @ref vine_declare_buffer.
@return A constant pointer to the buffer contents, or null if not available.
*/
const char * vine_file_contents( struct vine_file *f );

/** Get the length of a vine file.
@param f A file object.
@return The length of the file, or zero if unknown.
*/
size_t vine_file_size( struct vine_file *f );


/** Declare a file object from a local file
@param m A manager object
@param source The path of the file on the local filesystem
@param flags Whether to never cache the file at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transferred among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input, and @ref vine_task_add_output
*/
struct vine_file * vine_declare_file( struct vine_manager *m, const char *source, vine_file_flags_t flags );

/** Declare a file object from a remote URL.
@param m A manager object
@param url The URL address of the object in text form.
@param flags Whether to never cache the file at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transferred among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_url( struct vine_manager *m, const char *url, vine_file_flags_t flags );


/** Create a file object of a remote file accessible from an xrootd server.
@param m A manager object
@param source The URL address of the root file in text form as: "root://XROOTSERVER[:port]//path/to/file"
@param proxy A proxy file object (e.g. from @ref vine_declare_file) of a X509 proxy to use. If NULL, the
environment variable X509_USER_PROXY and the file "$TMPDIR/$UID" are considered
in that order. If no proxy is present, the transfer is tried without authentication.
@param env    If not NULL, an environment file (e.g poncho or starch, see @ref vine_task_add_environment) that contains the xrootd executables. Otherwise assume xrootd is available at the worker.
@param flags Whether to never cache the file at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transferred among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_xrootd( struct vine_manager *m, const char *source, struct vine_file *proxy, struct vine_file *env, vine_file_flags_t flags );


/** Create a file object of a remote file accessible from a chirp server.
@param m A manager object
@param server The chirp server address of the form "hostname[:port"]"
@param source The name of the file in the server
@param ticket If not NULL, a file object that provides a chirp an authentication ticket
@param env    If not NULL, an environment file (e.g poncho or starch, see @ref vine_task_add_environment) that contains the chirp executables. Otherwise assume chirp is available at the worker.
@param flags Whether to never cache the file at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transferred among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_chirp( struct vine_manager *m, const char *server, const char *source, struct vine_file *ticket, struct vine_file *env, vine_file_flags_t flags );


/** Create a scratch file object.
A scratch file has no initial content, but is created
as the output of a task, and may be consumed by other tasks.
@param m A manager object
@return A file object to use in @ref vine_task_add_input, @ref vine_task_add_output
*/
struct vine_file * vine_declare_temp( struct vine_manager *m );


/** Create a file object from a data buffer.
@param m A manager object
@param buffer The contents of the buffer.
@param size The length of the buffer, in bytes.
@param flags Whether to never cache the file at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transferred among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input, and @ref vine_task_add_output
*/
struct vine_file * vine_declare_buffer( struct vine_manager *m, const char *buffer, size_t size, vine_file_flags_t flags );


/** Create a file object representing an empty directory.
This is very occasionally needed for applications that expect
certain directories to exist in the working directory, prior to producing output.
This function does not transfer any data to the task, but just creates
a directory in its working sandbox.  If you want to transfer an entire
directory worth of data to a task, use @ref vine_declare_file and give a
directory name.
@param m A manager object
@return A file object to use in @ref vine_task_add_input, and @ref vine_task_add_output
*/
struct vine_file * vine_declare_empty_dir( struct vine_manager *m );


/** Create a file object produced from a mini-task
Attaches a task definition to produce an input file by running a Unix command.
This mini-task will be run on demand in order to produce the desired input file.
This is useful if an input requires some prior step such as transferring,
renaming, or unpacking to be useful.  A mini-task should be a short-running
activity with minimal resource consumption.
@param m A manager object
@param mini_task The task which produces the file
@param name A descriptive name for the mini-task.
@param flags Whether to never cache the output of the mini task at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). Cache flags can be
or'ed (|) with VINE_PEER_NOSHARE if the file should not be transfered among
workers when peer transfers are enabled (@ref vine_enable_peer_transfers).
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file *vine_declare_mini_task( struct vine_manager *m, struct vine_task *mini_task, const char *name, vine_file_flags_t flags);


/** Create a file object by unpacking a tar archive.
The archive may be compressed in any of the ways supported
by tar, and so this function supports extensions .tar, .tar.gz, .tgz, tar.bz2, and so forth.
@param m A manager object
@param f A file object corresponding to an archive packed by the tar command.
@param flags Whether to never cache the output directory of untar at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). VINE_PEER_NOSHARE
has no meaning for this declaration, as the output directory is never transferred among workers.
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_untar( struct vine_manager *m, struct vine_file *f, vine_file_flags_t flags);


/** Create a file object by unpacking a poncho package
@param m A manager object
@param f A file object corresponding to poncho or conda-pack tarball
@param flags Whether to never cache the expanded poncho environment at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). VINE_PEER_NOSHARE
has no meaning for this declaration, as the expanded environment is never
transferred among workers.
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_poncho( struct vine_manager *m, struct vine_file *f, vine_file_flags_t flags );


/** Create a file object by unpacking a starch package.
@param m A manager object
@param f A file object representing a sfx archive.
@param flags Whether to never cache the expanded starch archive at the workers (VINE_CACHE_NEVER,
the default), to cache it only for the current manager (VINE_CACHE), or to
cache it for the lifetime of the worker (VINE_CACHE_ALWAYS). VINE_PEER_NOSHARE
has no meaning for this declaration, as the expanded starch archive is never
transferred among workers.
@return A file object to use in @ref vine_task_add_input
*/
struct vine_file * vine_declare_starch( struct vine_manager *m, struct vine_file *f, vine_file_flags_t flags );

/** Fetch the contents of a file.
The contents of the given file will be loaded from disk or pulled back from the cluster
and loaded into manager memory.  This is particularly useful for temporary files and mini-tasks
whose contents are not returned to the manager by default.
@param m A manager object
@param f A file object.
@return A pointer to the contents of the file.  This will be freed with the file object.
*/

const char * vine_fetch_file( struct vine_manager *m, struct vine_file *f );

/** Remove a file that is no longer needed.
The given file or directory object is deleted from all worker's caches,
and is no longer available for use as an input file.
Completed tasks waiting for retrieval are not affected.
@param m A manager object
@param f Any file object.
*/
void vine_remove_file(struct vine_manager *m, struct vine_file *f );

//@}

/** @name Functions - Managers */

//@{

/** Create a new manager.
Users may modify the behavior of @ref vine_create by setting the following environmental variables before calling the function:

- <b>VINE_PORT</b>: This sets the default port of the manager (if unset, the default is 9123).
- <b>VINE_LOW_PORT</b>: If the user requests a random port, then this sets the first port number in the scan range (if unset, the default is 1024).
- <b>VINE_HIGH_PORT</b>: If the user requests a random port, then this sets the last port number in the scan range (if unset, the default is 32767).
- <b>VINE_NAME</b>: This sets the project name of the manager, which is reported to a catalog server (by default this is unset).
- <b>VINE_PRIORITY</b>: This sets the priority of the manager, which is used by workers to sort managers such that higher priority managers will be served first (if unset, the default is 10).

If the manager has a project name, then manager statistics and information will be
reported to a catalog server.  To set the catalog server, the user may set
the <b>CATALOG_HOST</b> and <b>CATALOG_PORT</b> environmental variables as described in @ref catalog_query_create.

@param port The port number to listen on.  If zero is specified, then the port stored in the <b>VINE_PORT</b> environment variable is used if available. If it isn't, or if -1 is specified, the first unused port between <b>VINE_LOW_PORT</b> and <b>VINE_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@return A new manager, or null if it could not be created.
*/
struct vine_manager *vine_create(int port);


/** Create a new manager using SSL.
 Like @ref vine_create, but all communications with the manager are encoded
 using TLS with they key and certificate provided. If key or cert are NULL,
 then TLS is not activated.
@param port The port number to listen on.  If zero is specified, then the port stored in the <b>VINE_PORT</b> environment variable is used if available. If it isn't, or if -1 is specified, the first unused port between <b>VINE_LOW_PORT</b> and <b>VINE_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@param key A key in pem format.
@param cert A certificate in pem format.
*/
struct vine_manager *vine_ssl_create(int port, const char *key, const char *cert);

/** Delete a manager.
This function should only be called after @ref vine_empty returns true.
@param m A manager to delete.
*/
void vine_delete(struct vine_manager *m);

/** Submit a task to a manager.
Once a task is submitted to a manager, it is not longer under the user's
control and should not be inspected until returned via @ref vine_wait.
Once returned, it is safe to re-submit the same take object via @ref vine_submit.
@param m A manager object
@param t A task object returned from @ref vine_task_create.
@return An integer task_id assigned to the submitted task.  Zero indicates a failure to submit due to an invalid task description.
*/
int vine_submit(struct vine_manager *m, struct vine_task *t);

/** Indicate the library to be installed on all workers connected to the manager.
The library is expected to run on all workers until they disconnect from the manager.
@param m A manager object
@param t A task object.
@param name The library to be installed
*/
void vine_manager_install_library( struct vine_manager *m, struct vine_task *t, const char *name );

/** Indicate the library to be removed from all connected workers
@param m A manager object
@param name The library to be removed
*/
void vine_manager_remove_library( struct vine_manager *m, const char *name );

/** Wait for a task to complete.
This call will block until either a task has completed, the timeout has expired, or the manager is empty.
If a task has completed, the corresponding task object will be returned by this function.
The caller may examine the task and then dispose of it using @ref vine_task_delete.

If the task ran to completion, then the <tt>result</tt> field will be zero and the <tt>return_status</tt>
field will contain the Unix exit code of the task.
If the task could not, then the <tt>result</tt> field will be non-zero and the
<tt>return_status</tt> field will be undefined.

@param m A manager object
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref VINE_WAIT_FOREVER to block until a task has completed.
@returns A completed task description, or null if the manager is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct vine_task *vine_wait(struct vine_manager *m, int timeout);


/** Wait for a task with a given task to complete.
Similar to @ref vine_wait, but guarantees that the returned task has the specified tag.
@param m A manager object
@param tag The desired tag. If NULL, then tasks are returned regardless of their tag.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref VINE_WAIT_FOREVER to block until a task has completed.
@returns A completed task description, or null if the manager is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct vine_task *vine_wait_for_tag(struct vine_manager *m, const char *tag, int timeout);

/** Wait for a task with a given task_id to complete.
Similar to @ref vine_wait, but guarantees that the returned task has the specified task_id.
@param m A manager object
@param task_id The desired task_id. If -1, then tasks are returned regardless of their task_id.
@param timeout The number of seconds to wait for a completed task before returning. Use an integer time to set the timeout or the constant @ref VINE_WAIT_FOREVER to block until a task has completed.
@returns A completed task description, or null if the manager is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct vine_task *vine_wait_for_task_id(struct vine_manager *m, int task_id, int timeout);

/** Determine whether the manager is 'hungry' for more tasks.
While the manager can handle a very large number of tasks,
it runs most efficiently when the number of tasks is slightly
larger than the number of active workers.  This function gives
the user of a flexible application a hint about whether it would
be better to submit more tasks via @ref vine_submit or wait for some to complete
via @ref vine_wait.
@param m A manager object
@returns The number of additional tasks that can be efficiently submitted,
or zero if the manager has enough to work with right now.
*/
int vine_hungry(struct vine_manager *m);

/** Determine whether the manager is empty.
When all of the desired tasks have been submitted to the manager,
the user should continue to call @ref vine_wait until
this function returns true.
@param m A manager object
@returns True if the manager is completely empty, false otherwise.
*/
int vine_empty(struct vine_manager *m);

/** Get the listening port of the manager.
As noted in @ref vine_create, there are many controls that affect what TCP port the manager will listen on.
Rather than assuming a specific port, the user should simply call this function to determine what port was selected.
@param m A manager object
@return The port the manager is listening on.
*/
int vine_port(struct vine_manager *m);

/** Change the project name for a given manager.
@param m A manager object
@param name The new project name.
*/
void vine_set_name(struct vine_manager *m, const char *name);

/** Get the project name of the manager.
@param m A manager object
@return The project name of the manager.
*/
const char *vine_get_name(struct vine_manager *m);

/** Enables resource monitoring for tasks. The resources measured are available
in the resources_measured member of the respective vine_task.
@param m A manager object
@param watchdog If not 0, kill tasks that exhaust declared resources.
@param time_series If not 0, generate a time series of resources per task in
VINE_RUNTIME_INFO_DIR/vine-logs/time-series/ (WARNING: for long running tasks these
files may reach gigabyte sizes. This function is mostly used for debugging.)
@return 1 on success, 0 if monitoring could not be enabled.
*/
int vine_enable_monitoring(struct vine_manager *m, int watchdog, int time_series);

/** Enable taskvine peer transfers to be scheduled by the manager **/
int vine_enable_peer_transfers(struct vine_manager *m);

/** Disable taskvine peer transfers to be scheduled by the manager **/
int vine_disable_peer_transfers(struct vine_manager *m);

/** Set the minimum task_id of future submitted tasks.
Further submitted tasks are guaranteed to have a task_id larger or equal to
minid.  This function is useful to make task_ids consistent in a workflow that
consists of sequential managers. (Note: This function is rarely used).  If the
minimum id provided is smaller than the last task_id computed, the minimum id
provided is ignored.
@param m A manager object
@param minid Minimum desired task_id
@return Returns the actual minimum task_id for future tasks.
*/
int vine_set_task_id_min(struct vine_manager *m, int minid);

/** Block workers in hostname from working for manager q.
@param m A manager object
@param hostname A string for hostname.
*/
void vine_block_host(struct vine_manager *m, const char *hostname);

/** Block workers in hostname from a manager, but remove block after timeout seconds.
If timeout is less than 1, then the hostname is blocked indefinitely, as
if @ref vine_block_host was called instead.
@param m A manager object
@param hostname A string for hostname.
@param seconds Number of seconds to the hostname will be blocked.
  */
void vine_block_host_with_timeout(struct vine_manager *m, const char *hostname, time_t seconds);


/** Unblock host from a manager.
@param m A manager object
@param hostname A string for hostname.
*/
void vine_unblock_host(struct vine_manager *m, const char *hostname);

/** Unblock all host.
@param m A manager object
*/
void vine_unblock_all(struct vine_manager *m);

/** Get manager statistics (only from manager)
@param m A manager object
@param s A pointer to a buffer that will be filed with statistics
*/
void vine_get_stats(struct vine_manager *m, struct vine_stats *s);

/** Get the task statistics for the given category.
@param m A manager object
@param c A category name
@param s A pointer to a buffer that will be filed with statistics
*/
void vine_get_stats_category(struct vine_manager *m, const char *c, struct vine_stats *s);


/** Get manager information as json
@param m A manager object
@param request One of: manager, tasks, workers, or categories
*/
char *vine_get_status(struct vine_manager *m, const char *request);


/** Summary data for all workers in buffer.
@param m A manager object
@return A null terminated array of struct rmsummary. Each summary s indicates the number of s->workers with a certain number of s->cores, s->memory, and s->disk. The array and summaries need to be freed after use to avoid memory leaks.
*/
struct rmsummary **vine_summarize_workers(struct vine_manager *m);

/** Get the current state of the task.
@param m A manager object
@param task_id The task_id of the task.
@return One of: VINE_TASK(UNKNOWN|READY|RUNNING|RESULTS|RETRIEVED|DONE)
*/
vine_task_state_t vine_task_state(struct vine_manager *m, int task_id);

/** Limit the manager bandwidth when transferring files to and from workers.
@param m A manager object
@param bandwidth The bandwidth limit in bytes per second.
*/
void vine_set_bandwidth_limit(struct vine_manager *m, const char *bandwidth);

/** Get current manager bandwidth.
@param m A manager object
@return The average bandwidth in MB/s measured by the manager.
*/
double vine_get_effective_bandwidth(struct vine_manager *m);

/** Enable disconnect slow workers functionality for a given manager for tasks without
an explicit category. Given the multiplier, disconnect a worker when it is executing a task
with a running time is
larger than the average times the multiplier.  The average is s computed per task
category. The value specified here applies to all the categories for which @ref
vine_enable_disconnect_slow_workers_category was not explicitely called.
@param m A manager object
@param multiplier The multiplier of the average task time at which point to disconnect; Disabled if less than 1.
@returns 0 if activated, 1 if deactivated.
*/
int vine_enable_disconnect_slow_workers(struct vine_manager *m, double multiplier);


/** Enable disconnect slow workers functionality for a given category. As @ref vine_enable_disconnect_slow_workers, but for a single
task category.
(Note: vine_enable_disconnect_slow_workers_category(q, "default", n) is the same as vine_enable_disconnect_slow_workers(q, n).)
@param m A manager object
@param category A category name.
@param multiplier The multiplier of the average task time at which point to disconnect; If less than one (default), use the multiplier of the "default" category.
@returns 0 if activated, 1 if deactivated.
*/
int vine_enable_disconnect_slow_workers_category(struct vine_manager *m, const char *category, double multiplier);


/** Set the draining mode per worker hostname.
If drain_flag is 0, workers at hostname receive tasks as usual.
If drain_flag is not 1, no new tasks are dispatched to workers at hostname,
and if empty they are shutdown.
@param m A manager object
@param hostname The hostname running the worker.
@param drain_flag Draining mode.
*/
int vine_set_draining_by_hostname(struct vine_manager *m, const char *hostname, int drain_flag);

/** Turn on or off first-allocation labeling for a given category. By default, cores, memory, and disk are labeled, and gpus are unlabeled. Turn on/off other specific resources use @ref vine_enable_category_resource
@param m A manager object
@param category A category name.
@param mode     One of @ref vine_category_mode_t.
@returns 1 if mode is valid, 0 otherwise.
*/
int vine_set_category_mode(struct vine_manager *m, const char *category, vine_category_mode_t mode);

/** Turn on or off first-allocation labeling for a given category and resource. This function should be use to fine-tune the defaults from @ref vine_set_category_mode.
@param m A manager object
@param category A category name.
@param resource A resource name.
@param autolabel 0 off, 1 on.
@returns 1 if resource is valid, 0 otherwise.
*/
int vine_enable_category_resource(struct vine_manager *m, const char *category, const char *resource, int autolabel);

/** Change the worker selection algorithm.
This function controls which <b>worker</b> will be selected for a given task.
@param m A manager object
@param algorithm The algorithm to use in assigning a task to a worker. See @ref vine_schedule_t for possible values.
*/
void vine_set_scheduler(struct vine_manager *m, vine_schedule_t algorithm);

/** Change the priority for a given manager.
@param m A manager object
@param priority The new priority of the manager.  Higher priority managers will attract workers first.
*/
void vine_set_priority(struct vine_manager *m, int priority);

/** Specify the number of tasks not yet submitted to the manager.
It is used by vine_factory to determine the number of workers to launch.
If not specified, it defaults to 0.
vine_factory considers the number of tasks as:
num tasks left + num tasks running + num tasks read.
@param m A manager object
@param ntasks Number of tasks yet to be submitted.
*/
void vine_set_tasks_left_count(struct vine_manager *m, int ntasks);

/** Specify the catalog server(s) the manager should report to.
@param m A manager object
@param hosts The catalog servers given as a comma delimited list of hostnames or hostname:port
*/
void vine_set_catalog_servers(struct vine_manager *m, const char *hosts);

/** Cancel a submitted task using its task id and remove it from manager.
@param m A manager object
@param id The task_id returned from @ref vine_submit.
@return The task description of the cancelled task, or null if the task was not found in manager. The returned task must be deleted with @ref vine_task_delete or resubmitted with @ref vine_submit.
*/
struct vine_task *vine_cancel_by_task_id(struct vine_manager *m, int id);

/** Cancel a submitted task using its tag and remove it from manager.
@param m A manager object
@param tag The tag name assigned to task using @ref vine_task_set_tag.
@return The task description of the cancelled task, or null if the task was not found in manager. The returned task must be deleted with @ref vine_task_delete or resubmitted with @ref vine_submit.
*/
struct vine_task *vine_cancel_by_task_tag(struct vine_manager *m, const char *tag);

/** Cancel all submitted tasks and remove them from the manager.
@param m A manager object
@return A struct list of all of the tasks canceled.  Each task must be deleted with @ref vine_task_delete or resubmitted with @ref vine_submit.
*/
struct list * vine_tasks_cancel(struct vine_manager *m);

/** Turn on the debugging log output and send to the named file.
 * (Note it does not need the vine_manager structure, as it is enabled before
 * the manager is created.)
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int vine_enable_debug_log( const char *logfile );

/** Add a performance log file that records cummulative statistics of the connected workers and submitted tasks.
@param m A manager object
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int vine_enable_perf_log(struct vine_manager *m, const char *logfile);

/** Add a log file that records the states of the connected workers and tasks.
@param m A manager object
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int vine_enable_transactions_log(struct vine_manager *m, const char *logfile);

/** Add an output log that produces the taskgraph in Grapvhiz Dot format.
@param m A manager object
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int vine_enable_taskgraph_log(struct vine_manager *m, const char *logfile);

/** Shut down workers connected to the manager. Gives a best effort and then returns the number of workers given the shut down order.
@param m A manager object
@param n The number to shut down. All workers if given "0".
*/
int vine_workers_shutdown(struct vine_manager *m, int n);


/** Add a mandatory password that each worker must present.
@param m A manager object
@param password The password to require.
*/

void vine_set_password( struct vine_manager *m, const char *password );

/** Add a mandatory password file that each worker must present.
@param m A manager object
@param file The name of the file containing the password.
@return True if the password was loaded, false otherwise.
*/

int vine_set_password_file( struct vine_manager *m, const char *file );

/** Change the keepalive interval for a given manager.
@param m A manager object
@param interval The minimum number of seconds to wait before sending new keepalive checks to workers.
*/
void vine_set_keepalive_interval(struct vine_manager *m, int interval);

/** Change the keepalive timeout for identifying dead workers for a given manager.
@param m A manager object
@param timeout The minimum number of seconds to wait for a keepalive response from worker before marking it as dead.
*/
void vine_set_keepalive_timeout(struct vine_manager *m, int timeout);

/** Set the preference for using hostname over IP address to connect.
'by_ip' uses IP addresses from the network interfaces of the manager (standard behavior), 'by_hostname' to use the hostname at the manager, or 'by_apparent_ip' to use the address of the manager as seen by the catalog server.
@param m A manager object
@param preferred_connection An string to indicate using 'by_ip' or a 'by_hostname'.
*/
void vine_set_manager_preferred_connection(struct vine_manager *m, const char *preferred_connection);

/** Tune advanced parameters for manager.
@param m A manager object
@param name The name of the parameter to tune
 - "resource-submit-multiplier" Treat each worker as having ({cores,memory,gpus} * multiplier) when submitting tasks. This allows for tasks to wait at a worker rather than the manager. (default = 1.0)
 - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=10)
 - "transfer-outlier-factor" Transfer that are this many times slower than the average will be terminated.  (default=10x)
 - "default-transfer-rate" The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)
 - "disconnect-slow-workers-factor" Set the multiplier of the average task time at which point to disconnect; deactivated if less than 1. (default=0)
 - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
 - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
 - "short-timeout" Set the minimum timeout when sending a brief message to a single worker. (default=5s)
 - "monitor-interval" Maximum number of seconds between resource monitor measurements. If less than 1, use default (5s). (default=5)
 - "category-steady-n-tasks" Set the number of tasks considered when computing category buckets.
 - "hungry-minimum" Mimimum number of tasks to consider manager not hungry. (default=10)
 - "wait-for-workers" Mimimum number of workers to connect before starting dispatching tasks. (default=0)
 - "attempt-schedule-depth" The amount of tasks to attempt scheduling on each pass of send_one_task in the main loop. (default=100)
 - "wait_retrieve_many" Parameter to alter how vine_wait works. If set to 0, vine_wait breaks out of the while loop whenever a task changes to VINE_TASK_DONE (wait_retrieve_one mode). If set to 1, vine_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
 - "monitor-interval" Parameter to change how frequently the resource monitor records resource consumption of a task in a times series, if this feature is enabled. See @ref vine_enable_monitoring.
@param value The value to set the parameter to.
@return 0 on succes, -1 on failure.
*/
int vine_tune(struct vine_manager *m, const char *name, double value);

/** Sets the maximum resources a task without an explicit category ("default" category).
rm specifies the maximum resources a task in the default category may use.
@param m  Reference to the current manager object.
@param rm Structure indicating maximum values. See @ref rmsummary for possible fields.
*/
void vine_set_resources_max(struct vine_manager *m,  const struct rmsummary *rm);

/** Sets the minimum resources a task without an explicit category ("default" category).
rm specifies the maximum resources a task in the default category may use.
@param m  Reference to the current manager object.
@param rm Structure indicating maximum values. See @ref rmsummary for possible fields.
*/
void vine_set_resources_min(struct vine_manager *m,  const struct rmsummary *rm);

/** Sets the maximum resources a task in the category may use.
@param m         Reference to the current manager object.
@param category  Name of the category.
@param rm Structure indicating minimum values. See @ref rmsummary for possible fields.
*/
void vine_set_category_resources_max(struct vine_manager *m,  const char *category, const struct rmsummary *rm);

/** Sets the minimum resources a task in the category may use.
@param m         Reference to the current manager object.
@param category  Name of the category.
@param rm Structure indicating minimum values. See @ref rmsummary for possible fields.
*/
void vine_set_category_resources_min(struct vine_manager *m,  const char *category, const struct rmsummary *rm);

/** Set the initial guess for resource autolabeling for the given category.
@param m         Reference to the current manager object.
@param category  Name of the category.
@param rm Structure indicating maximum values. Autolabeling available for cores, memory, disk, and gpus
*/
void vine_set_category_first_allocation_guess(struct vine_manager *m,  const char *category, const struct rmsummary *rm);

/** Initialize first value of categories
@param m     Reference to the current manager object.
@param max Structure indicating maximum values. Autolabeling available for cores, memory, disk, and gpus
@param summaries_file JSON file with resource summaries.
*/
void vine_initialize_categories(struct vine_manager *m, struct rmsummary *max, const char *summaries_file);

/** Sets the path where runtime info directories (logs and staging) are created.
@param path A directory
*/
void vine_set_runtime_info_path(const char *path);


//@}

#endif
