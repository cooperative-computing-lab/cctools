/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_H
#define DS_H

/** @file ds_manager.h A manager-worker library.
 The work queue provides an implementation of the manager-worker computing model
 using TCP sockets, Unix applications, and files as intermediate buffers.  A
 manager process uses @ref ds_create to create a queue, then @ref
 ds_submit to submit tasks.  Once tasks are running, call @ref
 ds_wait to wait for completion. A generic worker program, named
 <tt>ds_worker</tt>, can be run on any machine, and simply needs to be
 told the host and port of the manager.
*/

#include <sys/types.h>
#include "timestamp.h"
#include "category.h"
#include "rmsummary.h"

#define DS_DEFAULT_PORT 9123               /**< Default Data Swarm port number. */
#define DS_RANDOM_PORT  0                  /**< Indicates that any port may be chosen. */

#define DS_WAITFORTASK  -1                 /**< Timeout value to wait for a task to complete before returning. */

#define DS_DEFAULT_KEEPALIVE_INTERVAL 120  /**< Default value for Data Swarm keepalive interval in seconds. */
#define DS_DEFAULT_KEEPALIVE_TIMEOUT  30   /**< Default value for Data Swarm keepalive timeout in seconds. */

typedef enum {
	DS_INPUT  = 0,                         /**< Specify an input object. */
	DS_OUTPUT = 1                          /**< Specify an output object. */
} ds_file_type_t;

typedef enum {
	DS_NOCACHE  = 0, /**< Do not cache file at execution site. */
	DS_CACHE    = 1, /**< Cache file at execution site for later use. */
	DS_SYMLINK  = 2, /**< Create a symlink to the file rather than copying it, if possible. */
	DS_WATCH    = 16, /**< Watch the output file and send back changes as the task runs. */
	DS_FAILURE_ONLY = 32,/**< Only return this output file if the task failed.  (Useful for returning large log files.) */
	DS_SUCCESS_ONLY = 64, /**< Only return this output file if the task succeeded. */

	DS_PREEXIST = 4 /**< If the filename already exists on the host, use it in place. (Warning: Internal work queue use only.) */
} ds_file_flags_t;

typedef enum {
	DS_SCHEDULE_UNSET = 0,
	DS_SCHEDULE_FCFS,      /**< Select worker on a first-come-first-serve basis. */
	DS_SCHEDULE_FILES,     /**< Select worker that has the most data required by the task. */
	DS_SCHEDULE_TIME,      /**< Select worker that has the fastest execution time on previous tasks. */
	DS_SCHEDULE_RAND,      /**< Select a random worker. (default) */
	DS_SCHEDULE_WORST      /**< Select the worst fit worker (the worker with more unused resources). */
} ds_schedule_t;


typedef enum {
	DS_RESULT_SUCCESS             = 0,      /**< The task ran successfully **/
	DS_RESULT_INPUT_MISSING       = 1,      /**< The task cannot be run due to a missing input file **/
	DS_RESULT_OUTPUT_MISSING      = 2,      /**< The task ran but failed to generate a specified output file **/
	DS_RESULT_STDOUT_MISSING      = 4,      /**< The task ran but its stdout has been truncated **/
	DS_RESULT_SIGNAL              = 1 << 3, /**< The task was terminated with a signal **/
	DS_RESULT_RESOURCE_EXHAUSTION = 2 << 3, /**< The task used more resources than requested **/
	DS_RESULT_TASK_TIMEOUT        = 3 << 3, /**< The task ran after the specified (absolute since epoch) end time. **/
	DS_RESULT_UNKNOWN             = 4 << 3, /**< The result could not be classified. **/
	DS_RESULT_FORSAKEN            = 5 << 3, /**< The task failed, but it was not a task error **/
	DS_RESULT_MAX_RETRIES         = 6 << 3, /**< The task could not be completed successfully in the given number of retries. **/
	DS_RESULT_TASK_MAX_RUN_TIME   = 7 << 3, /**< The task ran for more than the specified time (relative since running in a worker). **/
	DS_RESULT_DISK_ALLOC_FULL     = 8 << 3, /**< The task filled its loop device allocation but needed more space. **/
	DS_RESULT_RMONITOR_ERROR      = 9 << 3, /**< The task failed because the monitor did not produce a summary report. **/
	DS_RESULT_OUTPUT_TRANSFER_ERROR = 10 << 3  /**< The task failed because an output could be transfered to the manager (not enough disk space, incorrect write permissions. */
} ds_result_t;

typedef enum {
	DS_TASK_UNKNOWN = 0,       /**< There is no such task **/
	DS_TASK_READY,             /**< Task is ready to be run, waiting in queue **/
	DS_TASK_RUNNING,           /**< Task has been dispatched to some worker **/
	DS_TASK_WAITING_RETRIEVAL, /**< Task results are available at the worker **/
	DS_TASK_RETRIEVED,         /**< Task results are available at the manager **/
	DS_TASK_DONE,              /**< Task is done, and returned through ds_wait >**/
	DS_TASK_CANCELED,           /**< Task was canceled before completion **/
} ds_task_state_t;

typedef enum {
	DS_FILE = 1,              /**< File-spec is a regular file **/
	DS_BUFFER,                /**< Data comes from buffer memory **/
	DS_REMOTECMD,             /**< File-spec is a regular file **/
	DS_FILE_PIECE,            /**< File-spec refers to only a part of a file **/
	DS_DIRECTORY,             /**< File-spec is a directory **/
	DS_URL                    /**< File-spec refers to an URL **/
} ds_file_t;


// Here we repeat the category_mode_t declaration but with work_queue names.
// This is needed to generate uniform names in the API and bindings:

typedef enum {
    DS_ALLOCATION_MODE_FIXED          = CATEGORY_ALLOCATION_MODE_FIXED, /**< When monitoring is disabled, all tasks run as
                                                                                  DS_ALLOCATION_MODE_FIXED. If monitoring is enabled and resource
                                                                                  exhaustion occurs for specified resources values, then the task permanently fails. */
    DS_ALLOCATION_MODE_MAX            = CATEGORY_ALLOCATION_MODE_MAX, /**< When monitoring is enabled, tasks are tried with maximum specified values
                                                                                of cores, memory, disk or gpus until enough statistics are collected. Then,
                                                                                further tasks are first tried using the maximum values observed, and in case of
                                                                                resource exhaustion, they are retried using the maximum specified values. The
                                                                                task permanently fails when there is an exhaustion using the maximum values. If
                                                                                no maximum values are specified, the task will wait until a larger worker
                                                                                connects. */
    DS_ALLOCATION_MODE_MIN_WASTE      = CATEGORY_ALLOCATION_MODE_MIN_WASTE, /**< As above, but tasks are first tried with an automatically
                                                                                      computed allocation to minimize resource waste. */
    DS_ALLOCATION_MODE_MAX_THROUGHPUT = CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT /**< As above, but maximizing throughput. */
} ds_category_mode_t;


extern int ds_option_scheduler;	               /**< Initial setting for algorithm to assign tasks to
												 workers upon creating queue . Change prior to
												 calling ds_create, after queue is created
												 this variable is not considered and changes must be
												 made through the API calls.   */

/** A task description.  This structure should only be created with @ref
 * ds_task_create and delete with @ref ds_task_delete.  You may
 * examine (but not modify) this structure once a task has completed.
*/
struct ds_task {
	char *tag;                                        /**< An optional user-defined logical name for the task. */
	char *command_line;                               /**< The program(s) to execute, as a shell command line. */
	ds_schedule_t worker_selection_algorithm; /**< How to choose worker to run the task. */
	char *output;                                     /**< The standard output of the task. */
	struct list *input_files;                         /**< The files to transfer to the worker and place in the executing directory. */
	struct list *output_files;                        /**< The output files (other than the standard output stream) created by the program to be retrieved from the task. */
	struct list *env_list;                            /**< Environment variables applied to the task. */
	int taskid;                                       /**< A unique task id number. */
	int return_status;                                /**< The exit code of the command line. */
	ds_result_t result;                       /**< The result of the task (see @ref ds_result_t */
	char *host;                                       /**< The address and port of the host on which it ran. */
	char *hostname;                                   /**< The name of the host on which it ran. */

	char *coprocess;                                  /**< The name of the coprocess name in the worker that executes this task. For regular tasks it is NULL. */

	char *category;                         /**< User-provided label for the task. It is expected that all task with the same category will have similar resource usage. See @ref ds_task_specify_category. If no explicit category is given, the label "default" is used. **/
	category_allocation_t resource_request; /**< See @ref category_allocation_t */

	double priority;        /**< The priority of this task relative to others in the queue: higher number run earlier. */
	int max_retries;        /**< Number of times the task is tried to be executed on some workers until success. If less than one, the task is retried indefinitely. See try_count below.*/

	int try_count;          /**< The number of times the task has been dispatched to a worker. If larger than max_retries, the task failes with @ref DS_RESULT_MAX_RETRIES. */
	int exhausted_attempts; /**< Number of times the task failed given exhausted resources. */
	int fast_abort_count; /**< Number of times this task has been terminated for running too long. */

	/* All times in microseconds */
	/* A time_when_* refers to an instant in time, otherwise it refers to a length of time. */
	timestamp_t time_when_submitted;    /**< The time at which this task was added to the queue. */
	timestamp_t time_when_done;         /**< The time at which the task is mark as retrieved, after transfering output files and other final processing. */

	int disk_allocation_exhausted;                        /**< Non-zero if a task filled its loop device allocation, zero otherwise. */

	int64_t min_running_time;           /**< Minimum time (in seconds) the task needs to run. (see ds_worker --wall-time)*/

    /**< All fields of the form time_* in microseconds. */
	timestamp_t time_when_commit_start; /**< The time when the task starts to be transfered to a worker. */
	timestamp_t time_when_commit_end;   /**< The time when the task is completely transfered to a worker. */

	timestamp_t time_when_retrieval;    /**< The time when output files start to be transfered back to the manager. time_done - time_when_retrieval is the time taken to transfer output files. */

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
	char *monitor_output_directory;                        /**< Custom output directory for the monitoring output files. If NULL, save to directory from @ref ds_enable_monitoring */

	char *monitor_snapshot_file;                          /**< Filename the monitor checks to produce snapshots. */
	struct list *features;                                /**< User-defined features this task requires. (See ds_worker's --feature option.) */

	/* deprecated fields */
	//int total_submissions;                                 /**< @deprecated Use try_count. */

	timestamp_t time_task_submit;                          /**< @deprecated Use time_when_submitted. */
	timestamp_t time_task_finish;                          /**< @deprecated Use time_when_done. */
	timestamp_t time_committed;                            /**< @deprecated Use time_when_commit_start. */

	timestamp_t time_send_input_start;                     /**< @deprecated Use time_when_commit_start. */
	timestamp_t time_send_input_finish;                    /**< @deprecated Use time_when_commit_end. */
	timestamp_t time_receive_result_start;                 /**< @deprecated Use time_when_retrieval instead. */
	timestamp_t time_receive_result_finish;                /**< @deprecated Use time_when_done instead. */
	timestamp_t time_receive_output_start;                 /**< @deprecated Use time_when_retrieval. */
	timestamp_t time_receive_output_finish;                /**< @deprecated Use time_when_done instead. */

	timestamp_t time_execute_cmd_start;                    /**< @deprecated Use time_when_commit_end instead. */
	timestamp_t time_execute_cmd_finish;                   /**< @deprecated Use time_when_retrieval instead. */

	timestamp_t total_transfer_time;                       /**< @deprecated Use (time_when_commit_end - time_when_commit_start) + (time_when_done - time_when_retrieval). */

	timestamp_t cmd_execution_time;                        /**< @deprecated Use time_workers_execute_last instead. */
	timestamp_t total_cmd_execution_time;                  /**< @deprecated Use time_workers_execute_all instead. */
	timestamp_t total_cmd_exhausted_execute_time;          /**< @deprecated Use time_workers_execute_exhaustion instead. */
	timestamp_t total_time_until_worker_failure;           /**< @deprecated Use time_workers_execute_failure instead. */

	int64_t total_bytes_received;                          /**< @deprecated Use bytes_received instead. */
	int64_t total_bytes_sent;                              /**< @deprecated Use bytes_sent instead. */
	int64_t total_bytes_transferred;                       /**< @deprecated Use bytes_transferred instead. */

	timestamp_t time_app_delay;                            /**< @deprecated The time spent in upper-level application (outside of ds_wait). */
};

/** Statistics describing a work queue. */

struct ds_stats {
	/* Stats for the current state of workers: */
	int workers_connected;	  /**< Number of workers currently connected to the manager. */
	int workers_init;         /**< Number of workers connected, but that have not send their available resources report yet.*/
	int workers_idle;         /**< Number of workers that are not running a task. */
	int workers_busy;         /**< Number of workers that are running at least one task. */
	int workers_able;         /**< Number of workers on which the largest task can run. */

	/* Cumulative stats for workers: */
	int workers_joined;       /**< Total number of worker connections that were established to the manager. */
	int workers_removed;      /**< Total number of worker connections that were released by the manager, idled-out, fast-aborted, or lost. */
	int workers_released;     /**< Total number of worker connections that were asked by the manager to disconnect. */
	int workers_idled_out;    /**< Total number of worker that disconnected for being idle. */
	int workers_fast_aborted; /**< Total number of worker connections terminated for being too slow. (see @ref ds_activate_fast_abort) */
	int workers_blocked ;     /**< Total number of workers blocked by the manager. (Includes workers_fast_aborted.) */
	int workers_lost;         /**< Total number of worker connections that were unexpectedly lost. (does not include idled-out or fast-aborted) */

	/* Stats for the current state of tasks: */
	int tasks_waiting;        /**< Number of tasks waiting to be dispatched. */
	int tasks_on_workers;     /**< Number of tasks currently dispatched to some worker. */
	int tasks_running;        /**< Number of tasks currently executing at some worker. */
	int tasks_with_results;   /**< Number of tasks with retrieved results and waiting to be returned to user. */

	/* Cumulative stats for tasks: */
	int tasks_submitted;           /**< Total number of tasks submitted to the queue. */
	int tasks_dispatched;          /**< Total number of tasks dispatch to workers. */
	int tasks_done;                /**< Total number of tasks completed and returned to user. (includes tasks_failed) */
	int tasks_failed;              /**< Total number of tasks completed and returned to user with result other than WQ_RESULT_SUCCESS. */
	int tasks_cancelled;           /**< Total number of tasks cancelled. */
	int tasks_exhausted_attempts;  /**< Total number of task executions that failed given resource exhaustion. */

	/* All times in microseconds */
	/* A time_when_* refers to an instant in time, otherwise it refers to a length of time. */

	/* Master time statistics: */
	timestamp_t time_when_started; /**< Absolute time at which the manager started. */
	timestamp_t time_send;         /**< Total time spent in sending tasks to workers (tasks descriptions, and input files.). */
	timestamp_t time_receive;      /**< Total time spent in receiving results from workers (output files.). */
	timestamp_t time_send_good;    /**< Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS. */
	timestamp_t time_receive_good; /**< Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS. */
	timestamp_t time_status_msgs;  /**< Total time spent sending and receiving status messages to and from workers, including workers' standard output, new workers connections, resources updates, etc. */
	timestamp_t time_internal;     /**< Total time the queue spents in internal processing. */
	timestamp_t time_polling;      /**< Total time blocking waiting for worker communications (i.e., manager idle waiting for a worker message). */
	timestamp_t time_application;  /**< Total time spent outside ds_wait. */

	/* Workers time statistics: */
	timestamp_t time_workers_execute;            /**< Total time workers spent executing done tasks. */
	timestamp_t time_workers_execute_good;       /**< Total time workers spent executing done tasks with result WQ_RESULT_SUCCESS. */
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

	int64_t committed_cores;  /**< Committed number of cores aggregated across the connected workers. */
	int64_t committed_memory; /**< Committed memory in MB aggregated across the connected workers. */
	int64_t committed_disk;	  /**< Committed disk space in MB aggregated across the connected workers. */

	int64_t max_cores;        /**< The highest number of cores observed among the connected workers. */
	int64_t max_memory;       /**< The largest memory size in MB observed among the connected workers. */
	int64_t max_disk;         /**< The largest disk space in MB observed among the connected workers. */

	int64_t min_cores;        /**< The lowest number of cores observed among the connected workers. */
	int64_t min_memory;       /**< The smallest memory size in MB observed among the connected workers. */
	int64_t min_disk;         /**< The smallest disk space in MB observed among the connected workers. */

    double manager_load;      /**< In the range of [0,1]. If close to 1, then
                                the manager is at full load and spends most
                                of its time sending and receiving taks, and
                                thus cannot accept connections from new
                                workers. If close to 0, the manager is spending
                                most of its time waiting for something to happen. */

	/**< deprecated fields: */
	int total_workers_connected;    /**< @deprecated Use workers_connected instead. */
	int total_workers_joined;       /**< @deprecated Use workers_joined instead. */
	int total_workers_removed;      /**< @deprecated Use workers_removed instead. */
	int total_workers_lost;         /**< @deprecated Use workers_lost instead. */
	int total_workers_idled_out;    /**< @deprecated Use workers_idled_out instead. */
	int total_workers_fast_aborted; /**< @deprecated Use workers_fast_aborted instead. */

	int tasks_complete;             /**< @deprecated Use tasks_with_results. */

	int total_tasks_dispatched;     /**< @deprecated Use tasks_dispatched instead. */
	int total_tasks_complete;       /**< @deprecated Use tasks_done instead. */
	int total_tasks_failed;         /**< @deprecated Use tasks_failed instead. */
	int total_tasks_cancelled;      /**< @deprecated Use tasks_cancelled instead. */
	int total_exhausted_attempts;   /**< @deprecated Use tasks_exhausted_attempts instead. */
	timestamp_t start_time;               /**< @deprecated Use time_when_started. */
	timestamp_t total_send_time;          /**< @deprecated Use time_send.    */
	timestamp_t total_receive_time;       /**< @deprecated Use time_receive. */
	timestamp_t total_good_transfer_time; /**< @deprecated Use time_send_good + time_receive_good. */

	timestamp_t total_execute_time;           /**< @deprecated Use time_workers_execute. */
	timestamp_t total_good_execute_time;      /**< @deprecated Use time_workers_execute_good. */
	timestamp_t total_exhausted_execute_time; /**< @deprecated Use time_workers_execute_exhaustion. */

	int64_t total_bytes_sent;     /**< @deprecated Use bytes_sent. */
	int64_t total_bytes_received; /**< @deprecated Use bytes_received. */

	double capacity; /**< @deprecated Use capacity_cores. */

	double efficiency;      /**< @deprecated. broken. */
	double idle_percentage; /**< @deprecated. */

	int64_t total_gpus;       /**< @deprecated: broken. */
	int64_t committed_gpus;   /**< @deprecated: broken. */
	int64_t max_gpus;         /**< @deprecated: broken. */
	int64_t min_gpus;         /**< @deprecated: broken. */

	int port;                       /**< @deprecated Use @ref ds_port Port of the queue. */
	int priority;                   /**< @deprecated Not used. */
	int workers_ready;              /**< @deprecated Use workers_idle instead. */
	int workers_full;               /**< @deprecated Use workers_busy insead. */
	int total_worker_slots;         /**< @deprecated Use tasks_running instead. */
	int avg_capacity;               /**< @deprecated Use capacity_cores instead. */
	int workers_blacklisted;         /**< @deprecated Use workers_blocked instead. */
};


/* Forward declare the queue's structure. This structure is opaque and defined in work_queue.c */
struct work_queue;


/** @name Functions - Tasks */

//@{

/** Create a new task object.
Once created and elaborated with functions such as @ref ds_task_specify_file
and @ref ds_task_specify_buffer, the task should be passed to @ref ds_submit.
@param full_command The shell command line or coprocess functions to be
executed by the task.  If null, the command will be given later by @ref
ds_task_specify_command
@return A new task object, or null if it could not be created.
*/
struct ds_task *ds_task_create(const char *full_command);

/** Create a copy of a task
Create a functionally identical copy of a @ref ds_task that
can be re-submitted via @ref ds_submit.
@return A new task object
*/
struct ds_task *ds_task_clone(const struct ds_task *task);

/** Indicate the command to be executed.
@param t A task object.
@param cmd The command to be executed.  This string will be duplicated by this call, so the argument may be freed or re-used afterward.
*/
void ds_task_specify_command( struct ds_task *t, const char *cmd );

/** Indicate the command to be executed.
@param t A task object.
@param cmd The coprocess name that will execute the command at the worker. The task
will only be sent to workers running the coprocess.
*/
void ds_task_specify_coprocess( struct ds_task *t, const char *coprocess_name );

/** Add a file to a task.
@param t A task object.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the remote execution site.
@param type Must be one of the following values:
- @ref DS_INPUT to indicate an input file to be consumed by the task
- @ref DS_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
- @ref DS_WATCH indicates that the worker will watch the output file as it is created
and incrementally return the file to the manager as the task runs.  (The frequency of these updates
is entirely dependent upon the system load.  If the manager is busy interacting with many workers,
output updates will be infrequent.)
@return 1 if the task file is successfully specified, 0 if either of @a t,  @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_file(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags);

/** Add a file piece to a task.
@param t A task object.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the remote execution site.
@param start_byte The starting byte offset of the file piece to be transferred.
@param end_byte The ending byte offset of the file piece to be transferred.
@param type Must be one of the following values:
- @ref DS_INPUT to indicate an input file to be consumed by the task
- @ref DS_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file piece is successfully specified, 0 if either of @a t, @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_file_piece(struct ds_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, ds_file_type_t type, ds_file_flags_t flags);

/** Add an input buffer to a task.
@param t A task object.
@param data The data to be passed as an input file.
@param length The length of the buffer, in bytes
@param remote_name The name of the remote file to create.
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file is successfully specified, 0 if either of @a t or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_buffer(struct ds_task *t, const char *data, int length, const char *remote_name, ds_file_flags_t flags);

/** Add a directory to a task.
@param t A task object.
@param local_name The name of the directory on local disk or shared filesystem.  Optional if the directory is empty.
@param remote_name The name of the directory at the remote execution site.
@param type Must be one of the following values:
- @ref DS_INPUT to indicate an input file to be consumed by the task
- @ref DS_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
@param recursive indicates whether just the directory (0) or the directory and all of its contents (1) should be included.
@return 1 if the task directory is successfully specified, 0 if either of @a t,  @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_directory(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags, int recursive);

/** Add a url as an input for a task.
@param t A task object.
@param url The source URL to be accessed to provide the file.
@param remote_name The name of the file as seen by the task.
@param type Must be one of the following values:
- @ref DS_INPUT to indicate an input file to be consumed by the task
- @ref DS_OUTPUT is not currently supported.
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file is successfully specified, 0 if either of @a t or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_url(struct ds_task *t, const char *url, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags);

/** Gets/puts file at remote_name using cmd at worker.
@param t A task object.
@param cmd The shell command to transfer the file. For input files, it should read the contents from remote_name via stdin. For output files, it should write the contents to stdout.
@param remote_name The name of the file as seen by the task.
@param type Must be one of the following values:
- @ref DS_INPUT to indicate an input file to be consumed by the task
- @ref DS_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref ds_file_flags_t or'd together. The most common are:
- @ref DS_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref DS_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file is successfully specified, 0 if either of @a t or @a remote_name is null or @a remote_name is an absolute path.
*/
int ds_task_specify_file_command(struct ds_task *t, const char *cmd, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags);

/** Specify the number of times this task is retried on worker errors. If less than one, the task is retried indefinitely (this the default). A task that did not succeed after the given number of retries is returned with result DS_RESULT_MAX_RETRIES.
@param t A task object.
@param max_retries The number of retries.
*/

void ds_task_specify_max_retries( struct ds_task *t, int64_t max_retries );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param memory The amount of disk space required by the task, in megabytes.
*/

void ds_task_specify_memory( struct ds_task *t, int64_t memory );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param disk The amount of disk space required by the task, in megabytes.
*/

void ds_task_specify_disk( struct ds_task *t, int64_t disk );

/** Specify the number of cores required by a task.
@param t A task object.
@param cores The number of cores required by the task.
*/

void ds_task_specify_cores( struct ds_task *t, int cores );

/** Specify the number of gpus required by a task.
@param t A task object.
@param gpus The number of gpus required by the task.
*/

void ds_task_specify_gpus( struct ds_task *t, int gpus );

/** Specify the maximum end time allowed for the task (in microseconds since the
 * Epoch). If less than 1, then no end time is specified (this is the default).
This is useful, for example, when the task uses certificates that expire.
@param t A task object.
@param useconds Number of useconds since the Epoch.
*/

void ds_task_specify_end_time( struct ds_task *t, int64_t useconds );

/** Specify the minimum start time allowed for the task (in microseconds since the
 * Epoch). If less than 1, then no minimum start time is specified (this is the default).
@param t A task object.
@param useconds Number of useconds since the Epoch.
*/

void ds_task_specify_start_time_min( struct ds_task *t, int64_t useconds );

/** Specify the maximum time (in microseconds) the task is allowed to run in a
 * worker. This time is accounted since the the moment the task starts to run
 * in a worker.  If less than 1, then no maximum time is specified (this is the default).
@param t A task object.
@param useconds Maximum number of seconds the task may run in a worker.
*/

void ds_task_specify_running_time( struct ds_task *t, int64_t useconds );

/** Specify the maximum time (in seconds) the task is allowed to run in a worker.
 * This time is accounted since the moment the task starts to run in a worker.
 * If less than 1, then no maximum time is specified (this is the default).
 * Note: same effect as ds_task_specify_running_time.
@param t A task object.
@param seconds Maximum number of seconds the task may run in a worker.
*/
void ds_task_specify_running_time_max( struct ds_task *t, int64_t seconds );

/** Specify the minimum time (in seconds) the task is expected to run in a worker.
 * This time is accounted since the moment the task starts to run in a worker.
 * If less than 1, then no minimum time is specified (this is the default).
@param t A task object.
@param seconds Minimum number of seconds the task may run in a worker.
*/
void ds_task_specify_running_time_min( struct ds_task *t, int64_t seconds );

/** Attach a user defined string tag to the task.
This field is not interpreted by the work queue, but is provided for the user's convenience
in identifying tasks when they complete.
@param t A task object.
@param tag The tag to attach to task t.
*/
void ds_task_specify_tag(struct ds_task *t, const char *tag);

/** Label the task with the given category. It is expected that tasks with the same category
have similar resources requirements (e.g. for fast abort).
@param t A task object.
@param category The name of the category to use.
*/
void ds_task_specify_category(struct ds_task *t, const char *category);

/** Label the task with a user-defined feature. The task will only run on a worker that provides (--feature option) such feature.
@param t A task object.
@param name The name of the feature.
*/
void ds_task_specify_feature(struct ds_task *t, const char *name);

/** Specify the priority of this task relative to others in the queue.
Tasks with a higher priority value run first. If no priority is given, a task is placed at the end of the ready list, regardless of the priority.
@param t A task object.
@param priority The priority of the task.
*/

void ds_task_specify_priority(struct ds_task *t, double priority );

/**
Specify an environment variable to be added to the task.
@param t A task object
@param name Name of the variable.
@param value Value of the variable.
 */
void ds_task_specify_environment_variable( struct ds_task *t, const char *name, const char *value );

/** Select the scheduling algorithm for a single task.
To change the scheduling algorithm for all tasks, use @ref ds_specify_algorithm instead.
@param t A task object.
@param algorithm The algorithm to use in assigning this task to a worker. For possible values, see @ref ds_schedule_t.
*/
void ds_task_specify_algorithm(struct ds_task *t, ds_schedule_t algorithm);

/** Specify a custom name for the monitoring summary. If @ref ds_enable_monitoring is also enabled, the summary is also written to that directory.
@param t A task object.
@param monitor_output Resource summary file.
*/

void ds_task_specify_monitor_output(struct ds_task *t, const char *monitor_output);

/** Delete a task.
This may be called on tasks after they are returned from @ref ds_wait.
@param t The task to delete.
*/
void ds_task_delete(struct ds_task *t);


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

@param t A work queue task object.
@param monitor_snapshot_file A filename.
@return 1 if the task file is successfully specified, 0 if either of @a t, or @a monitor_snapshot_file is null.
*/

int ds_specify_snapshot_file(struct ds_task *t, const char *monitor_snapshot_file);


//@}

/** @name Functions - Queues */

//@{

/** Create a new work queue.
Users may modify the behavior of @ref ds_create by setting the following environmental variables before calling the function:

- <b>DS_PORT</b>: This sets the default port of the queue (if unset, the default is 9123).
- <b>DS_LOW_PORT</b>: If the user requests a random port, then this sets the first port number in the scan range (if unset, the default is 1024).
- <b>DS_HIGH_PORT</b>: If the user requests a random port, then this sets the last port number in the scan range (if unset, the default is 32767).
- <b>DS_NAME</b>: This sets the project name of the queue, which is reported to a catalog server (by default this is unset).
- <b>DS_PRIORITY</b>: This sets the priority of the queue, which is used by workers to sort managers such that higher priority managers will be served first (if unset, the default is 10).

If the queue has a project name, then queue statistics and information will be
reported to a catalog server.  To specify the catalog server, the user may set
the <b>CATALOG_HOST</b> and <b>CATALOG_PORT</b> environmental variables as described in @ref catalog_query_create.

@param port The port number to listen on.  If zero is specified, then the port stored in the <b>DS_PORT</b> environment variable is used if available. If it isn't, or if -1 is specified, the first unused port between <b>DS_LOW_PORT</b> and <b>DS_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@return A new work queue, or null if it could not be created.
*/
struct ds_manager *ds_create(int port);


/** Create a new work queue using SSL.
 Like @ref ds_create, but all communications with the queue are encoded
 using TLS with they key and certificate provided. If key or cert are NULL,
 then TLS is not activated.
@param port The port number to listen on.  If zero is specified, then the port stored in the <b>DS_PORT</b> environment variable is used if available. If it isn't, or if -1 is specified, the first unused port between <b>DS_LOW_PORT</b> and <b>DS_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@param key A key in pem format.
@param cert A certificate in pem format.
*/
struct ds_manager *ds_ssl_create(int port, const char *key, const char *cert);

/** Enables resource monitoring on the give work queue.
It generates a resource summary per task, which is written to the given
directory. It also creates all_summaries-PID.log, that consolidates all
summaries into a single. If monitor_output_dirname is NULL, ds_task is
updated with the resources measured, and no summary file is kept unless
explicitely given by ds_task's monitor_output_file.
@param q A work queue object.
@param monitor_output_directory The name of the output directory. If NULL,
summaries are kept only when monitor_output_directory is specify per task, but
resources_measured from ds_task is updated.  @return 1 on success, 0 if
@param watchdog if not 0, kill tasks that exhaust declared resources.
@return 1 on success, o if monitoring was not enabled.
*/
int ds_enable_monitoring(struct ds_manager *q, char *monitor_output_directory, int watchdog);

/** Enables resource monitoring on the give work queue.
As @ref ds_enable_monitoring, but it generates a time series and a
monitor debug file (WARNING: for long running tasks these files may reach
gigabyte sizes. This function is mostly used for debugging.) @param q A work
queue object.
@param monitor_output_directory The name of the output directory.
@param watchdog if not 0, kill tasks that exhaust declared resources.
@return 1 on success, 0 if monitoring was not enabled.
*/
int ds_enable_monitoring_full(struct ds_manager *q, char *monitor_output_directory, int watchdog);

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's
control and should not be inspected until returned via @ref ds_wait.
Once returned, it is safe to re-submit the same take object via @ref ds_submit.
@param q A work queue object.
@param t A task object returned from @ref ds_task_create.
@return An integer taskid assigned to the submitted task.
*/
int ds_submit(struct ds_manager *q, struct ds_task *t);


/** Set the minimum taskid of future submitted tasks.
Further submitted tasks are guaranteed to have a taskid larger or equal to
minid.  This function is useful to make taskids consistent in a workflow that
consists of sequential managers. (Note: This function is rarely used).  If the
minimum id provided is smaller than the last taskid computed, the minimum id
provided is ignored.
@param q A work queue object.
@param minid Minimum desired taskid
@return Returns the actual minimum taskid for future tasks.
*/
int ds_specify_min_taskid(struct ds_manager *q, int minid);

/** Block workers in hostname from working for queue q.
@param q A work queue object.
@param hostname A string for hostname.
*/
void ds_block_host(struct ds_manager *q, const char *hostname);

/** Block workers in hostname from a queue, but remove block after timeout seconds.
  If timeout is less than 1, then the hostname is blocked indefinitely, as
  if @ref ds_block_host was called instead.
  @param q A work queue object.
  @param hostname A string for hostname.
  @param seconds Number of seconds to the hostname will be blocked.
  */
void ds_block_host_with_timeout(struct ds_manager *q, const char *hostname, time_t seconds);


/** Unblock host from a queue.
@param q A work queue object.
@param hostname A string for hostname.
*/
void ds_unblock_host(struct ds_manager *q, const char *hostname);

/** Unblock all host.
@param q A work queue object.
*/
void ds_unblock_all(struct ds_manager *q);

/** Invalidate cached file.
The file or directory with the given local name specification is deleted from
the workers' cache, so that a newer version may be used. Any running task using
the file is canceled and resubmitted. Completed tasks waiting for retrieval are
not affected.
(Currently anonymous buffers and file pieces cannot be deleted once cached in a worker.)
@param q A work queue object.
@param local_name The name of the file on local disk or shared filesystem, or uri.
@param type One of:
- @ref DS_FILE
- @ref DS_DIRECTORY
- @ref DS_URL
*/
void ds_invalidate_cached_file(struct ds_manager *q, const char *local_name, ds_file_t type);


/** Wait for a task to complete.
This call will block until either a task has completed, the timeout has expired, or the queue is empty.
If a task has completed, the corresponding task object will be returned by this function.
The caller may examine the task and then dispose of it using @ref ds_task_delete.

If the task ran to completion, then the <tt>result</tt> field will be zero and the <tt>return_status</tt>
field will contain the Unix exit code of the task.
If the task could not, then the <tt>result</tt> field will be non-zero and the
<tt>return_status</tt> field will be undefined.

@param q A work queue object.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref DS_WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct ds_task *ds_wait(struct ds_manager *q, int timeout);


/** Wait for a task with a given task to complete.
Similar to @ref ds_wait, but guarantees that the returned task has the specified tag.
@param q A work queue object.
@param tag The desired tag. If NULL, then tasks are returned regardless of their tag.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref DS_WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct ds_task *ds_wait_for_tag(struct ds_manager *q, const char *tag, int timeout);

/** Determine whether the queue is 'hungry' for more tasks.
While the Data Swarm can handle a very large number of tasks,
it runs most efficiently when the number of tasks is slightly
larger than the number of active workers.  This function gives
the user of a flexible application a hint about whether it would
be better to submit more tasks via @ref ds_submit or wait for some to complete
via @ref ds_wait.
@param q A work queue object.
@returns The number of additional tasks that can be efficiently submitted,
or zero if the queue has enough to work with right now.
*/
int ds_hungry(struct ds_manager *q);

/** Determine whether the queue is empty.
When all of the desired tasks have been submitted to the queue,
the user should continue to call @ref ds_wait until
this function returns true.
@param q A work queue object.
@returns True if the queue is completely empty, false otherwise.
*/
int ds_empty(struct ds_manager *q);

/** Get the listening port of the queue.
As noted in @ref ds_create, there are many controls that affect what TCP port the queue will listen on.
Rather than assuming a specific port, the user should simply call this function to determine what port was selected.
@param q A work queue object.
@return The port the queue is listening on.
*/
int ds_port(struct ds_manager *q);

/** Get queue statistics (only from manager).
@param q A work queue object.
@param s A pointer to a buffer that will be filed with statistics.
*/
void ds_get_stats(struct ds_manager *q, struct ds_stats *s);

/** Get statistics of the manager queue together with foremen information.
@param q A work queue object.
@param s A pointer to a buffer that will be filed with statistics.
*/
void ds_get_stats_hierarchy(struct ds_manager *q, struct ds_stats *s);

/** Get the task statistics for the given category.
@param q A work queue object.
@param c A category name.
@param s A pointer to a buffer that will be filed with statistics.
*/
void ds_get_stats_category(struct ds_manager *q, const char *c, struct ds_stats *s);


/** Summary data for all workers in buffer.
@param q A work queue object.
@return A null terminated array of struct rmsummary. Each summary s indicates the number of s->workers with a certain number of s->cores, s->memory, and s->disk. The array and summaries need to be freed after use to avoid memory leaks.
*/
struct rmsummary **ds_workers_summary(struct ds_manager *q);

/** Get the current state of the task.
@param q A work queue object.
@param taskid The taskid of the task.
@return One of: DS_TASK(UNKNOWN|READY|RUNNING|RESULTS|RETRIEVED|DONE)
*/
ds_task_state_t ds_task_state(struct ds_manager *q, int taskid);

/** Limit the queue bandwidth when transferring files to and from workers.
@param q A work queue object.
@param bandwidth The bandwidth limit in bytes per second.
*/
void ds_set_bandwidth_limit(struct ds_manager *q, const char *bandwidth);

/** Get current queue bandwidth.
@param q A work queue object.
@return The average bandwidth in MB/s measured by the manager.
*/
double ds_get_effective_bandwidth(struct ds_manager *q);

/** Summarize workers.
This function summarizes the workers currently connected to the manager,
indicating how many from each worker pool are attached.
@param q A work queue object.
@return A newly allocated string describing the distribution of workers by pool.  The caller must release this string via free().
*/
char * ds_get_worker_summary( struct ds_manager *q );

/** Turn on or off fast abort functionality for a given queue for tasks without
an explicit category. Given the multiplier, abort a task which running time is
larger than the average times the multiplier.  Fast-abort is computed per task
category. The value specified here applies to all the categories for which @ref
ds_activate_fast_abort_category was not explicitely called.
@param q A work queue object.
@param multiplier The multiplier of the average task time at which point to abort; if less than zero, fast_abort is deactivated (the default).
@returns 0 if activated, 1 if deactivated.
*/
int ds_activate_fast_abort(struct ds_manager *q, double multiplier);


/** Turn on or off fast abort functionality for a given category. Given the
multiplier, abort a task which running time is larger than the average times the
multiplier.  The value specified here applies only to tasks in the given category.
(Note: ds_activate_fast_abort_category(q, "default", n) is the same as ds_activate_fast_abort(q, n).)
@param q A work queue object.
@param category A category name.
@param multiplier The multiplier of the average task time at which point to abort; if zero, fast_abort is deactivated. If less than zero (default), use the fast abort of the "default" category.
@returns 0 if activated, 1 if deactivated.
*/
int ds_activate_fast_abort_category(struct ds_manager *q, const char *category, double multiplier);


/** Set the draining mode per worker hostname.
	If drain_flag is 0, workers at hostname receive tasks as usual.
    If drain_flag is not 1, no new tasks are dispatched to workers at hostname,
    and if empty they are shutdown.
  @param q A work queue object.
  @param hostname The hostname running the worker.
  @param drain_flag Draining mode.
  */
int ds_specify_draining_by_hostname(struct ds_manager *q, const char *hostname, int drain_flag);

/** Turn on or off first-allocation labeling for a given category. By default, cores, memory, and disk are labeled, and gpus are unlabeled. Turn on/off other specific resources use @ref ds_enable_category_resource
@param q A work queue object.
@param category A category name.
@param mode     One of @ref ds_category_mode_t.
@returns 1 if mode is valid, 0 otherwise.
*/
int ds_specify_category_mode(struct ds_manager *q, const char *category, ds_category_mode_t mode);

/** Turn on or off first-allocation labeling for a given category and resource. This function should be use to fine-tune the defaults from @ref ds_specify_category_mode.
@param q A work queue object.
@param category A category name.
@param resource A resource name.
@param autolabel 0 off, 1 on.
@returns 1 if resource is valid, 0 otherwise.
*/
int ds_enable_category_resource(struct ds_manager *q, const char *category, const char *resource, int autolabel);

/** Change the worker selection algorithm.
This function controls which <b>worker</b> will be selected for a given task.
@param q A work queue object.
@param algorithm The algorithm to use in assigning a task to a worker. See @ref ds_schedule_t for possible values.
*/
void ds_specify_algorithm(struct ds_manager *q, ds_schedule_t algorithm);

/** Get the project name of the queue.
@param q A work queue object.
@return The project name of the queue.
*/
const char *ds_name(struct ds_manager *q);

/** Change the project name for a given queue.
@param q A work queue object.
@param name The new project name.
*/
void ds_specify_name(struct ds_manager *q, const char *name);

/** Change the debug log path for a given queue (used by TLQ).
@param q A work queue object.
@param path The debug log path.
*/
void ds_specify_debug_path(struct ds_manager *q, const char *path);

/** Change the home host and port for a given queue (used by TLQ).
@param q A work queue object.
@param port New local port for the TLQ URL.
*/
void ds_specify_tlq_port(struct ds_manager *q, int port);

/** Change the priority for a given queue.
@param q A work queue object.
@param priority The new priority of the queue.  Higher priority managers will attract workers first.
*/
void ds_specify_priority(struct ds_manager *q, int priority);

/** Specify the number of tasks not yet submitted to the queue.
	It is used by ds_factory to determine the number of workers to launch.
	If not specified, it defaults to 0.
	ds_factory considers the number of tasks as:
	num tasks left + num tasks running + num tasks read.
  @param q A work queue object.
  @param ntasks Number of tasks yet to be submitted.
  */
void ds_specify_num_tasks_left(struct ds_manager *q, int ntasks);

/** Specify the catalog server the manager should report to.
@param q A work queue object.
@param hostname The catalog server's hostname.
@param port The port the catalog server is listening on.
*/
void ds_specify_catalog_server(struct ds_manager *q, const char *hostname, int port);

/** Specify the catalog server(s) the manager should report to.
@param q A work queue object.
@param hosts The catalog servers given as a comma delimited list of hostnames or hostname:port
*/
void ds_specify_catalog_servers(struct ds_manager *q, const char *hosts);

/** Cancel a submitted task using its task id and remove it from queue.
@param q A work queue object.
@param id The taskid returned from @ref ds_submit.
@return The task description of the cancelled task, or null if the task was not found in queue. The returned task must be deleted with @ref ds_task_delete or resubmitted with @ref ds_submit.
*/
struct ds_task *ds_cancel_by_taskid(struct ds_manager *q, int id);

/** Cancel a submitted task using its tag and remove it from queue.
@param q A work queue object.
@param tag The tag name assigned to task using @ref ds_task_specify_tag.
@return The task description of the cancelled task, or null if the task was not found in queue. The returned task must be deleted with @ref ds_task_delete or resubmitted with @ref ds_submit.
*/
struct ds_task *ds_cancel_by_tasktag(struct ds_manager *q, const char *tag);

/** Cancel all submitted tasks and remove them from the queue.
@param q A work queue object.
@return A struct list of all of the tasks canceled.  Each task must be deleted with @ref ds_task_delete or resubmitted with @ref ds_submit.
*/
struct list * ds_cancel_all_tasks(struct ds_manager *q);

/** Shut down workers connected to the work_queue system. Gives a best effort and then returns the number of workers given the shut down order.
@param q A work queue object.
@param n The number to shut down. All workers if given "0".
*/
int ds_shut_down_workers(struct ds_manager *q, int n);

/** Delete a work queue.
This function should only be called after @ref ds_empty returns true.
@param q A work queue to delete.
*/
void ds_delete(struct ds_manager *q);

/** Add a log file that records cummulative statistics of the connected workers and submitted tasks.
@param q A work queue object.
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int ds_specify_log(struct ds_manager *q, const char *logfile);

/** Add a log file that records the states of the connected workers and tasks.
@param q A work queue object.
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int ds_specify_transactions_log(struct ds_manager *q, const char *logfile);

/** Add a mandatory password that each worker must present.
@param q A work queue object.
@param password The password to require.
*/

void ds_specify_password( struct ds_manager *q, const char *password );

/** Add a mandatory password file that each worker must present.
@param q A work queue object.
@param file The name of the file containing the password.
@return True if the password was loaded, false otherwise.
*/

int ds_specify_password_file( struct ds_manager *q, const char *file );

/** Change the keepalive interval for a given queue.
@param q A work queue object.
@param interval The minimum number of seconds to wait before sending new keepalive checks to workers.
*/
void ds_specify_keepalive_interval(struct ds_manager *q, int interval);

/** Change the keepalive timeout for identifying dead workers for a given queue.
@param q A work queue object.
@param timeout The minimum number of seconds to wait for a keepalive response from worker before marking it as dead.
*/
void ds_specify_keepalive_timeout(struct ds_manager *q, int timeout);

/** Set the preference for using hostname over IP address to connect.
'by_ip' uses IP addresses from the network interfaces of the manager (standard behavior), 'by_hostname' to use the hostname at the manager, or 'by_apparent_ip' to use the address of the manager as seen by the catalog server.
@param q A work queue object.
@param preferred_connection An string to indicate using 'by_ip' or a 'by_hostname'.
*/
void ds_manager_preferred_connection(struct ds_manager *q, const char *preferred_connection);

/** Tune advanced parameters for work queue.
@param q A work queue object.
@param name The name of the parameter to tune
 - "resource-submit-multiplier" Treat each worker as having ({cores,memory,gpus} * multiplier) when submitting tasks. This allows for tasks to wait at a worker rather than the manager. (default = 1.0)
 - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=10)
 - "foreman-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)
 - "transfer-outlier-factor" Transfer that are this many times slower than the average will be aborted.  (default=10x)
 - "default-transfer-rate" The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)
 - "fast-abort-multiplier" Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)
 - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
 - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
 - "short-timeout" Set the minimum timeout when sending a brief message to a single worker. (default=5s)
 - "long-timeout" Set the minimum timeout when sending a brief message to a foreman. (default=1h)
 - "category-steady-n-tasks" Set the number of tasks considered when computing category buckets.
 - "hungry-minimum" Mimimum number of tasks to consider queue not hungry. (default=10)
 - "wait-for-workers" Mimimum number of workers to connect before starting dispatching tasks. (default=0)
 - "wait_retrieve_many" Parameter to alter how ds_wait works. If set to 0, ds_wait breaks out of the while loop whenever a task changes to DS_TASK_DONE (wait_retrieve_one mode). If set to 1, ds_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
@param value The value to set the parameter to.
@return 0 on succes, -1 on failure.
*/
int ds_tune(struct ds_manager *q, const char *name, double value);

/** Sets the maximum resources a task without an explicit category ("default" category).
rm specifies the maximum resources a task in the default category may use.
@param q  Reference to the current work queue object.
@param rm Structure indicating maximum values. See @ref rmsummary for possible fields.
*/
void ds_specify_max_resources(struct ds_manager *q,  const struct rmsummary *rm);

/** Sets the minimum resources a task without an explicit category ("default" category).
rm specifies the maximum resources a task in the default category may use.
@param q  Reference to the current work queue object.
@param rm Structure indicating maximum values. See @ref rmsummary for possible fields.
*/
void ds_specify_min_resources(struct ds_manager *q,  const struct rmsummary *rm);

/** Sets the maximum resources a task in the category may use.
@param q         Reference to the current work queue object.
@param category  Name of the category.
@param rm Structure indicating minimum values. See @ref rmsummary for possible fields.
*/
void ds_specify_category_max_resources(struct ds_manager *q,  const char *category, const struct rmsummary *rm);

/** Sets the minimum resources a task in the category may use.
@param q         Reference to the current work queue object.
@param category  Name of the category.
@param rm Structure indicating minimum values. See @ref rmsummary for possible fields.
*/
void ds_specify_category_min_resources(struct ds_manager *q,  const char *category, const struct rmsummary *rm);

/** Set the initial guess for resource autolabeling for the given category.
@param q         Reference to the current work queue object.
@param category  Name of the category.
@param rm Structure indicating maximum values. Autolabeling available for cores, memory, disk, and gpus
*/
void ds_specify_category_first_allocation_guess(struct ds_manager *q,  const char *category, const struct rmsummary *rm);

/** Initialize first value of categories
@param q     Reference to the current work queue object.
@param max Structure indicating maximum values. Autolabeling available for cores, memory, disk, and gpus
@param summaries_file JSON file with resource summaries.
*/
void ds_initialize_categories(struct ds_manager *q, struct rmsummary *max, const char *summaries_file);


/** Explain result codes from tasks.
@param result Result from a task returned by @ref ds_wait.
@return String representation of task result code.
*/
const char *ds_result_str(ds_result_t result);

//@}

/** @name Functions - Deprecated */

//@{

#define DS_TASK_ORDER_FIFO 0  /**< Retrieve tasks based on first-in-first-out order. */
#define DS_TASK_ORDER_LIFO 1  /**< Retrieve tasks based on last-in-first-out order. */

/** Specified how the submitted tasks should be ordered. It does not have any effect now.
@param q A work queue object.
@param order The ordering to use for dispatching submitted tasks:
- @ref DS_TASK_ORDER_LIFO
- @ref DS_TASK_ORDER_FIFO
*/
void ds_specify_task_order(struct ds_manager *q, int order);


#define DS_MANAGER_MODE_STANDALONE 0 /**< Data Swarm manager does not report to the catalog server. */
#define DS_MANAGER_MODE_CATALOG 1    /**< Data Swarm manager reports to catalog server. */

/** Specify the manager mode for a given queue.
@param q A work queue object.
@param mode
- @ref DS_MANAGER_MODE_STANDALONE - standalone mode. In this mode the manager would not report its information to a catalog server;
- @ref DS_MANAGER_MODE_CATALOG - catalog mode. In this mode the manager report itself to a catalog server where workers get managers' information and select a manager to serve.
@deprecated Enabled automatically when @ref ds_specify_name is used.
*/
void ds_specify_manager_mode(struct ds_manager *q, int mode);


/** Change whether to estimate manager capacity for a given queue.
@param q A work queue object.
@param estimate_capacity_on if the value of this parameter is 1, then work queue should estimate the manager capacity. If the value is 0, then work queue would not estimate its manager capacity.
@deprecated This feature is always enabled.
*/
void ds_specify_estimate_capacity_on(struct ds_manager *q, int estimate_capacity_on);

/** Add an input buffer to a task.
@param t The task to which to add parameters
@param buf A pointer to the data buffer to send to the worker to be available to the commands.
@param length The number of bytes of data in the buffer
@param rname The name of the file in which to store the buffer data on the worker
@return 1 if the input buffer is successfully specified, 0 if either of @a t or @a rname is null or @a rname is an absolute path.
@deprecated Use @ref ds_task_specify_buffer instead.
*/
int ds_task_specify_input_buf(struct ds_task *t, const char *buf, int length, const char *rname);

/** Add an input file to a task.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@return 1 if the input file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref ds_task_specify_file instead.
*/
int ds_task_specify_input_file(struct ds_task *t, const char *fname, const char *rname);

/** Add an input file to a task, without caching.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@return 1 if the input file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref ds_task_specify_file instead.
*/
int ds_task_specify_input_file_do_not_cache(struct ds_task *t, const char *fname, const char *rname);

/** Add an output file to a task.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@return 1 if the output file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref ds_task_specify_file instead.
*/
int ds_task_specify_output_file(struct ds_task *t, const char *rname, const char *fname);

/** Add an output file to a task without caching.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@return 1 if the output file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref ds_task_specify_file instead.
*/
int ds_task_specify_output_file_do_not_cache(struct ds_task *t, const char *rname, const char *fname);

/** Generate a worker-level unique filename to indicate a disk allocation being full.
 @param pwd Base pathname.
 @param taskid id of the task to generate the filename.
 @return The string corresponding to the filename.
*/
char *ds_generate_disk_alloc_full_filename(char *pwd, int taskid);


/** Same as ds_task_specify_environment_variable, but with a typo in environment
 */
void ds_task_specify_enviroment_variable( struct ds_task *t, const char *name, const char *value );

//@}

// Renames for backwards compatibility
#define ds_master_preferred_connection ds_manager_preferred_connection
#define ds_specify_master_mode         ds_specify_manager_mode
#define ds_blacklist_add               ds_block_host
#define ds_blacklist_add_with_timeout  ds_block_host_with_timeout
#define ds_blacklist_remove            ds_unblock_host
#define ds_blacklist_clear             ds_unblock_all

#endif
