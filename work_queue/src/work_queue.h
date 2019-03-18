/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_H
#define WORK_QUEUE_H

/** @file work_queue.h A master-worker library.
 The work queue provides an implementation of the master-worker computing model
 using TCP sockets, Unix applications, and files as intermediate buffers.  A
 master process uses @ref work_queue_create to create a queue, then @ref
 work_queue_submit to submit tasks.  Once tasks are running, call @ref
 work_queue_wait to wait for completion. A generic worker program, named
 <tt>work_queue_worker</tt>, can be run on any machine, and simply needs to be
 told the host and port of the master.
*/

#include <sys/types.h>
#include "timestamp.h"
#include "category.h"
#include "rmsummary.h"

#define WORK_QUEUE_DEFAULT_PORT 9123               /**< Default Work Queue port number. */
#define WORK_QUEUE_RANDOM_PORT  0                  /**< Indicates that any port may be chosen. */

#define WORK_QUEUE_WAITFORTASK  -1                 /**< Timeout value to wait for a task to complete before returning. */

#define WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL 120  /**< Default value for Work Queue keepalive interval in seconds. */
#define WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT  30   /**< Default value for Work Queue keepalive timeout in seconds. */

typedef enum {
	WORK_QUEUE_INPUT  = 0,                         /**< Specify an input object. */
	WORK_QUEUE_OUTPUT = 1                          /**< Specify an output object. */
} work_queue_file_type_t;

typedef enum {
	WORK_QUEUE_NOCACHE  = 0, /**< Do not cache file at execution site. */
	WORK_QUEUE_CACHE    = 1, /**< Cache file at execution site for later use. */
	WORK_QUEUE_SYMLINK  = 2, /**< Create a symlink to the file rather than copying it, if possible. */
	WORK_QUEUE_PREEXIST = 4, /**< If the filename already exists on the host, use it in place. */
	WORK_QUEUE_THIRDGET = 8, /**< Access the file on the client from a shared filesystem */
	WORK_QUEUE_THIRDPUT = 8, /**< Access the file on the client from a shared filesystem (same as WORK_QUEUE_THIRDGET, included for readability) */
	WORK_QUEUE_WATCH    = 16 /**< Watch the output file and send back changes as the task runs. */
} work_queue_file_flags_t;

typedef enum {
	WORK_QUEUE_SCHEDULE_UNSET = 0,
	WORK_QUEUE_SCHEDULE_FCFS,      /**< Select worker on a first-come-first-serve basis. */
	WORK_QUEUE_SCHEDULE_FILES,     /**< Select worker that has the most data required by the task. */
	WORK_QUEUE_SCHEDULE_TIME,      /**< Select worker that has the fastest execution time on previous tasks. */
	WORK_QUEUE_SCHEDULE_RAND,      /**< Select a random worker. (default) */
	WORK_QUEUE_SCHEDULE_WORST      /**< Select the worst fit worker (the worker with more unused resources). */
} work_queue_schedule_t;


typedef enum {
	WORK_QUEUE_RESULT_SUCCESS             = 0,      /**< The task ran successfully **/
	WORK_QUEUE_RESULT_INPUT_MISSING       = 1,      /**< The task cannot be run due to a missing input file **/
	WORK_QUEUE_RESULT_OUTPUT_MISSING      = 2,      /**< The task ran but failed to generate a specified output file **/
	WORK_QUEUE_RESULT_STDOUT_MISSING      = 4,      /**< The task ran but its stdout has been truncated **/
	WORK_QUEUE_RESULT_SIGNAL              = 1 << 3, /**< The task was terminated with a signal **/
	WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION = 2 << 3, /**< The task used more resources than requested **/
	WORK_QUEUE_RESULT_TASK_TIMEOUT        = 3 << 3, /**< The task ran after the specified (absolute since epoch) end time. **/
	WORK_QUEUE_RESULT_UNKNOWN             = 4 << 3, /**< The result could not be classified. **/
	WORK_QUEUE_RESULT_FORSAKEN            = 5 << 3, /**< The task failed, but it was not a task error **/
	WORK_QUEUE_RESULT_MAX_RETRIES         = 6 << 3, /**< The task could not be completed successfully in the given number of retries. **/
	WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME   = 7 << 3, /**< The task ran for more than the specified time (relative since running in a worker). **/
	WORK_QUEUE_RESULT_DISK_ALLOC_FULL     = 8 << 3  /**< The task filled its loop device allocation but needed more space. **/
} work_queue_result_t;

typedef enum {
	WORK_QUEUE_TASK_UNKNOWN = 0,       /**< There is no such task **/
	WORK_QUEUE_TASK_READY,             /**< Task is ready to be run, waiting in queue **/
	WORK_QUEUE_TASK_RUNNING,           /**< Task has been dispatched to some worker **/
	WORK_QUEUE_TASK_WAITING_RETRIEVAL, /**< Task results are available at the worker **/
	WORK_QUEUE_TASK_RETRIEVED,         /**< Task results are available at the master **/
	WORK_QUEUE_TASK_DONE,              /**< Task is done, and returned through work_queue_wait >**/
	WORK_QUEUE_TASK_CANCELED,           /**< Task was canceled before completion **/
} work_queue_task_state_t;

typedef enum {
	WORK_QUEUE_FILE = 1,              /**< File-spec is a regular file **/
	WORK_QUEUE_BUFFER,                /**< Data comes from buffer memory **/
	WORK_QUEUE_REMOTECMD,             /**< File-spec is a regular file **/
	WORK_QUEUE_FILE_PIECE,            /**< File-spec refers to only a part of a file **/
	WORK_QUEUE_DIRECTORY,             /**< File-spec is a directory **/
	WORK_QUEUE_URL                    /**< File-spec refers to an URL **/
} work_queue_file_t;


extern int wq_option_scheduler;	               /**< Initial setting for algorithm to assign tasks to
												 workers upon creating queue . Change prior to
												 calling work_queue_create, after queue is created
												 this variable is not considered and changes must be
												 made through the API calls.   */

/** A task description.  This structure should only be created with @ref
 * work_queue_task_create and delete with @ref work_queue_task_delete.  You may
 * examine (but not modify) this structure once a task has completed.
*/
struct work_queue_task {
	char *tag;                                        /**< An optional user-defined logical name for the task. */
	char *command_line;                               /**< The program(s) to execute, as a shell command line. */
	work_queue_schedule_t worker_selection_algorithm; /**< How to choose worker to run the task. */
	char *output;                                     /**< The standard output of the task. */
	struct list *input_files;                         /**< The files to transfer to the worker and place in the executing directory. */
	struct list *output_files;                        /**< The output files (other than the standard output stream) created by the program to be retrieved from the task. */
	struct list *env_list;                            /**< Environment variables applied to the task. */
	int taskid;                                       /**< A unique task id number. */
	int return_status;                                /**< The exit code of the command line. */
	work_queue_result_t result;                       /**< The result of the task (see @ref work_queue_result_t */
	char *host;                                       /**< The address and port of the host on which it ran. */
	char *hostname;                                   /**< The name of the host on which it ran. */

	char *category;                         /**< User-provided label for the task. It is expected that all task with the same category will have similar resource usage. See @ref work_queue_task_specify_category. If no explicit category is given, the label "default" is used. **/
	category_allocation_t resource_request; /**< See @ref category_allocation_t */

	double priority;        /**< The priority of this task relative to others in the queue: higher number run earlier. */
	int max_retries;        /**< Number of times the task is tried to be executed on some workers until success. If less than one, the task is retried indefinitely. See try_count below.*/

	int try_count;          /**< The number of times the task has been dispatched to a worker. If larger than max_retries, the task failes with @ref WORK_QUEUE_RESULT_MAX_RETRIES. */
	int exhausted_attempts; /**< Number of times the task failed given exhausted resources. */

	/* All times in microseconds */
	/* A time_when_* refers to an instant in time, otherwise it refers to a length of time. */
	timestamp_t time_when_submitted;    /**< The time at which this task was added to the queue. */
	timestamp_t time_when_done;         /**< The time at which the task is mark as retrieved, after transfering output files and other final processing. */

	int disk_allocation_exhausted;                        /**< Non-zero if a task filled its loop device allocation, zero otherwise. */

	timestamp_t time_when_commit_start; /**< The time when the task starts to be transfered to a worker. */
	timestamp_t time_when_commit_end;   /**< The time when the task is completely transfered to a worker. */

	timestamp_t time_when_retrieval;    /**< The time when output files start to be transfered back to the master. time_done - time_when_retrieval is the time taken to transfer output files. */

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
	char *monitor_output_directory;                        /**< Custom output directory for the monitoring output files. If NULL, save to directory from @ref work_queue_enable_monitoring */

	char *monitor_snapshot_file;                          /**< Filename the monitor checks to produce snapshots. */
	struct list *features;                                /**< User-defined features this task requires. (See work_queue_worker's --feature option.) */

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

	timestamp_t time_app_delay;                            /**< @deprecated The time spent in upper-level application (outside of work_queue_wait). */
};

/** Statistics describing a work queue. */

struct work_queue_stats {
	/* Stats for the current state of workers: */
	int workers_connected;	  /**< Number of workers currently connected to the master. */
	int workers_init;         /**< Number of workers connected, but that have not send their available resources report yet.*/
	int workers_idle;         /**< Number of workers that are not running a task. */
	int workers_busy;         /**< Number of workers that are running at least one task. */
	int workers_able;         /**< Number of workers on which the largest task can run. */

	/* Cumulative stats for workers: */
	int workers_joined;       /**< Total number of worker connections that were established to the master. */
	int workers_removed;      /**< Total number of worker connections that were released by the master, idled-out, fast-aborted, or lost. */
	int workers_released;     /**< Total number of worker connections that were asked by the master to disconnect. */
	int workers_idled_out;    /**< Total number of worker that disconnected for being idle. */
	int workers_fast_aborted; /**< Total number of worker connections terminated for being too slow. (see @ref work_queue_activate_fast_abort) */
	int workers_blacklisted ; /**< Total number of workers blacklisted by the master. (Includes workers_fast_aborted.) */
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
	timestamp_t time_when_started; /**< Absolute time at which the master started. */
	timestamp_t time_send;         /**< Total time spent in sending tasks to workers (tasks descriptions, and input files.). */
	timestamp_t time_receive;      /**< Total time spent in receiving results from workers (output files.). */
	timestamp_t time_send_good;    /**< Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS. */
	timestamp_t time_receive_good; /**< Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS. */
	timestamp_t time_status_msgs;  /**< Total time spent sending and receiving status messages to and from workers, including workers' standard output, new workers connections, resources updates, etc. */
	timestamp_t time_internal;     /**< Total time the queue spents in internal processing. */
	timestamp_t time_polling;      /**< Total time blocking waiting for worker communications (i.e., master idle waiting for a worker message). */
	timestamp_t time_application;  /**< Total time spent outside work_queue_wait. */

	/* Workers time statistics: */
	timestamp_t time_workers_execute;            /**< Total time workers spent executing done tasks. */
	timestamp_t time_workers_execute_good;       /**< Total time workers spent executing done tasks with result WQ_RESULT_SUCCESS. */
	timestamp_t time_workers_execute_exhaustion; /**< Total time workers spent executing tasks that exhausted resources. */

	/* BW statistics */
	int64_t bytes_sent;     /**< Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the master. */
	int64_t bytes_received; /**< Total number of file bytes (not including protocol control msg bytes) received from the workers by the master. */
	double  bandwidth;      /**< Average network bandwidth in MB/S observed by the master when transferring to workers. */

	/* resources statistics */
	int capacity_tasks;     /**< The estimated number of tasks that this master can effectively support. */
	int capacity_cores;     /**< The estimated number of workers' cores that this master can effectively support.*/
	int capacity_memory;    /**< The estimated number of workers' MB of RAM that this master can effectively support.*/
	int capacity_disk;      /**< The estimated number of workers' MB of disk that this master can effectively support.*/
	int capacity_instantaneous;      /**< The estimated number of tasks that this master can support considering only the most recently completed task. */
	int capacity_weighted;  /**< The estimated number of tasks that this master can support placing greater weight on the most recently completed task. */

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

	int port;                       /**< @deprecated Use @ref work_queue_port Port of the queue. */
	int priority;                   /**< @deprecated Not used. */
	int workers_ready;              /**< @deprecated Use workers_idle instead. */
	int workers_full;               /**< @deprecated Use workers_busy insead. */
	int total_worker_slots;         /**< @deprecated Use tasks_running instead. */
	int avg_capacity;               /**< @deprecated Use capacity_cores instead. */
};


/** @name Functions - Tasks */

//@{

/** Create a new task object.
Once created and elaborated with functions such as @ref work_queue_task_specify_file
and @ref work_queue_task_specify_buffer, the task should be passed to @ref work_queue_submit.
@param full_command The shell command line to be executed by the task.  If null,
the command will be given later by @ref work_queue_task_specify_command
@return A new task object, or null if it could not be created.
*/
struct work_queue_task *work_queue_task_create(const char *full_command);

/** Create a copy of a task
Create a functionally identical copy of a @ref work_queue_task that
can be re-submitted via @ref work_queue_submit.
@return A new task object
*/
struct work_queue_task *work_queue_task_clone(const struct work_queue_task *task);

/** Indicate the command to be executed.
@param t A task object.
@param cmd The command to be executed.  This string will be duplicated by this call, so the argument may be freed or re-used afterward.
*/
void work_queue_task_specify_command( struct work_queue_task *t, const char *cmd );

/** Add a file to a task.
@param t A task object.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the remote execution site.
@param type Must be one of the following values:
- @ref WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- @ref WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref work_queue_file_flags_t or'd together. The most common are:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
- @ref WORK_QUEUE_WATCH indicates that the worker will watch the output file as it is created
and incrementally return the file to the master as the task runs.  (The frequency of these updates
is entirely dependent upon the system load.  If the master is busy interacting with many workers,
output updates will be infrequent.)
@return 1 if the task file is successfully specified, 0 if either of @a t,  @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t flags);

/** Add a file piece to a task.
@param t A task object.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the remote execution site.
@param start_byte The starting byte offset of the file piece to be transferred.
@param end_byte The ending byte offset of the file piece to be transferred.
@param type Must be one of the following values:
- @ref WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- @ref WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref work_queue_file_flags_t or'd together. The most common are:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file piece is successfully specified, 0 if either of @a t, @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int work_queue_task_specify_file_piece(struct work_queue_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, work_queue_file_type_t type, work_queue_file_flags_t flags);

/** Add an input buffer to a task.
@param t A task object.
@param data The data to be passed as an input file.
@param length The length of the buffer, in bytes
@param remote_name The name of the remote file to create.
@param flags	May be zero to indicate no special handling or any of @ref work_queue_file_flags_t or'd together. The most common are:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
@return 1 if the task file is successfully specified, 0 if either of @a t or @a remote_name is null or @a remote_name is an absolute path.
*/
int work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, work_queue_file_flags_t);

/** Add a directory to a task.
@param t A task object.
@param local_name The name of the directory on local disk or shared filesystem.  Optional if the directory is empty.
@param remote_name The name of the directory at the remote execution site.
@param type Must be one of the following values:
- @ref WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- @ref WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of @ref work_queue_file_flags_t or'd together. The most common are:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
@param recursive indicates whether just the directory (0) or the directory and all of its contents (1) should be included.
@return 1 if the task directory is successfully specified, 0 if either of @a t,  @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
*/
int work_queue_task_specify_directory(struct work_queue_task *t, const char *local_name, const char *remote_name, work_queue_file_type_t type, work_queue_file_flags_t, int recursive);

/** Specify the number of times this task is retried on worker errors. If less than one, the task is retried indefinitely (this the default). A task that did not succeed after the given number of retries is returned with result WORK_QUEUE_RESULT_MAX_RETRIES.
@param t A task object.
@param max_retries The number of retries.
*/

void work_queue_task_specify_max_retries( struct work_queue_task *t, int64_t max_retries );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param disk The amount of disk space required by the task, in megabytes.
*/

void work_queue_task_specify_memory( struct work_queue_task *t, int64_t memory );

/** Specify the amount of disk space required by a task.
@param t A task object.
@param disk The amount of disk space required by the task, in megabytes.
*/

void work_queue_task_specify_disk( struct work_queue_task *t, int64_t disk );

/** Specify the number of cores required by a task.
@param t A task object.
@param cores The number of cores required by the task.
*/

void work_queue_task_specify_cores( struct work_queue_task *t, int cores );

/** Specify the number of gpus required by a task.
@param t A task object.
@param gpus The number of gpus required by the task.
*/

void work_queue_task_specify_gpus( struct work_queue_task *t, int gpus );

/** Specify the maximum end time allowed for the task (in microseconds since the
 * Epoch). If less than 1, then no end time is specified (this is the default).
This is useful, for example, when the task uses certificates that expire.
@param t A task object.
@param seconds Number of seconds since the Epoch.
*/

void work_queue_task_specify_end_time( struct work_queue_task *t, int64_t useconds );

/** Specify the maximum time (in microseconds) the task is allowed to run in a
 * worker. This time is accounted since the the moment the task starts to run
 * in a worker.  If less than 1, then no maximum time is specified (this is the default).
@param t A task object.
@param seconds Maximum number of seconds the task may run in a worker.
*/

void work_queue_task_specify_running_time( struct work_queue_task *t, int64_t useconds );

/** Attach a user defined string tag to the task.
This field is not interpreted by the work queue, but is provided for the user's convenience
in identifying tasks when they complete.
@param t A task object.
@param tag The tag to attach to task t.
*/
void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag);

/** Label the task with the given category. It is expected that tasks with the same category
have similar resources requirements (e.g. for fast abort).
@param q A work queue object.
@param t A task object.
@param category The name of the category to use.
*/
void work_queue_task_specify_category(struct work_queue_task *t, const char *category);

/** Label the task with a user-defined feature. The task will only run on a worker that provides (--feature option) such feature.
@param q A work queue object.
@param t A task object.
@param feature The name of the feature.
*/
void work_queue_task_specify_feature(struct work_queue_task *t, const char *name);

/** Specify the priority of this task relative to others in the queue.
Tasks with a higher priority value run first. If no priority is given, a task is placed at the end of the ready list, regardless of the priority.
@param t A task object.
@param priority The priority of the task.
*/

void work_queue_task_specify_priority(struct work_queue_task *t, double priority );

/**
Specify an environment variable to be added to the task.
@param t A task object
@param name Name of the variable.
@param value Value of the variable.
 */
void work_queue_task_specify_enviroment_variable( struct work_queue_task *t, const char *name, const char *value );

/** Select the scheduling algorithm for a single task.
To change the scheduling algorithm for all tasks, use @ref work_queue_specify_algorithm instead.
@param t A task object.
@param algorithm The algorithm to use in assigning this task to a worker. For possible values, see @ref work_queue_schedule_t.
*/
void work_queue_task_specify_algorithm(struct work_queue_task *t, work_queue_schedule_t algorithm);

/** Specify a custom name for the monitoring summary. If @ref work_queue_enable_monitoring is also enabled, the summary is also written to that directory.
@param t A task object.
@param monitor_output Resource summary file.
*/

void work_queue_task_specify_monitor_output(struct work_queue_task *t, const char *monitor_output);

/** Delete a task.
This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task to delete.
*/
void work_queue_task_delete(struct work_queue_task *t);


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

int work_queue_specify_snapshot_file(struct work_queue_task *t, const char *monitor_snapshot_file);


//@}

/** @name Functions - Queues */

//@{

/** Create a new work queue.
Users may modify the behavior of @ref work_queue_create by setting the following environmental variables before calling the function:

- <b>WORK_QUEUE_PORT</b>: This sets the default port of the queue (if unset, the default is 9123).
- <b>WORK_QUEUE_LOW_PORT</b>: If the user requests a random port, then this sets the first port number in the scan range (if unset, the default is 1024).
- <b>WORK_QUEUE_HIGH_PORT</b>: If the user requests a random port, then this sets the last port number in the scan range (if unset, the default is 32767).
- <b>WORK_QUEUE_NAME</b>: This sets the project name of the queue, which is reported to a catalog server (by default this is unset).
- <b>WORK_QUEUE_PRIORITY</b>: This sets the priority of the queue, which is used by workers to sort masters such that higher priority masters will be served first (if unset, the default is 10).

If the queue has a project name, then queue statistics and information will be
reported to a catalog server.  To specify the catalog server, the user may set
the <b>CATALOG_HOST</b> and <b>CATALOG_PORT</b> environmental variables as described in @ref catalog_query_create.

@param port The port number to listen on.  If zero is specified, then the port stored in the <b>WORK_QUEUE_PORT</b> environment variable is used if available. If it isn't, or if -1 is specified, the first unused port between <b>WORK_QUEUE_LOW_PORT</b> and <b>WORK_QUEUE_HIGH_PORT</b> (1024 and 32767 by default) is chosen.
@return A new work queue, or null if it could not be created.
*/
struct work_queue *work_queue_create(int port);

/** Enables resource monitoring on the give work queue.
It generates a resource summary per task, which is written to the given
directory. It also creates all_summaries-PID.log, that consolidates all
summaries into a single. If monitor_output_dirname is NULL, work_queue_task is
updated with the resources measured, and no summary file is kept unless
explicitely given by work_queue_task's monitor_output_file.
@param q A work queue object.
@param monitor_output_dirname The name of the output directory. If NULL,
summaries are kept only when monitor_output_directory is specify per task, but
resources_measured from work_queue_task is updated.  @return 1 on success, 0 if
@param watchdog if not 0, kill tasks that exhaust declared resources.
monitoring was not enabled.
*/
int work_queue_enable_monitoring(struct work_queue *q, char *monitor_output_directory, int watchdog);

/** Enables resource monitoring on the give work queue.
As @ref work_queue_enable_monitoring, but it generates a time series and a
monitor debug file (WARNING: for long running tasks these files may reach
gigabyte sizes. This function is mostly used for debugging.) @param q A work
queue object.
@param monitor_output_dirname The name of the output directory.
@param watchdog if not 0, kill tasks that exhaust declared resources.
@return 1 on success, 0 if monitoring was not enabled.
*/
int work_queue_enable_monitoring_full(struct work_queue *q, char *monitor_output_directory, int watchdog);

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's
control and should not be inspected until returned via @ref work_queue_wait.
Once returned, it is safe to re-submit the same take object via @ref work_queue_submit.
@param q A work queue object.
@param t A task object returned from @ref work_queue_task_create.
@return An integer taskid assigned to the submitted task.
*/
int work_queue_submit(struct work_queue *q, struct work_queue_task *t);


/** Set the minimum taskid of future submitted tasks.
Further submitted tasks are guaranteed to have a taskid larger or equal to
minid.  This function is useful to make taskids consistent in a workflow that
consists of sequential masters. (Note: This function is rarely used).  If the
minimum id provided is smaller than the last taskid computed, the minimum id
provided is ignored.
@param q A work queue object.
@param minid Minimum desired taskid
@return Returns the actual minimum taskid for future tasks.
*/
int work_queue_specify_min_taskid(struct work_queue *q, int minid);

/** Blacklist hostname from a queue.
@param q A work queue object.
@param hostname A string for hostname.
*/
void work_queue_blacklist_add(struct work_queue *q, const char *hostname);

/** Blacklist hostname from a queue. Remove from blacklist in timeout seconds.
  If timeout is less than 1, then the hostname is blacklisted indefinitely, as
  if @ref work_queue_blacklist_add was called instead.
  @param q A work queue object.
  @param hostname A string for hostname.
  @param seconds Number of seconds to the hostname will be in the blacklist.
  */
void work_queue_blacklist_add_with_timeout(struct work_queue *q, const char *hostname, time_t seconds);


/** Unblacklist host from a queue.
@param q A work queue object.
@param hostname A string for hostname.
*/
void work_queue_blacklist_remove(struct work_queue *q, const char *hostname);


/** Clear blacklist of a queue.
@param q A work queue object.
*/
void work_queue_blacklist_clear(struct work_queue *q);

/** Invalidate cached file.
The file or directory with the given local name specification is deleted from
the workers' cache, so that a newer version may be used. Any running task using
the file is canceled and resubmitted. Completed tasks waiting for retrieval are
not affected.
(Currently anonymous buffers and file pieces cannot be deleted once cached in a worker.)
@param q A work queue object.
@param local_name The name of the file on local disk or shared filesystem, or uri.
@param type One of:
- @ref WORK_QUEUE_FILE
- @ref WORK_QUEUE_DIRECTORY
- @ref WORK_QUEUE_URL
*/
void work_queue_invalidate_cached_file(struct work_queue *q, const char *local_name, work_queue_file_t type);


/** Wait for a task to complete.
This call will block until either a task has completed, the timeout has expired, or the queue is empty.
If a task has completed, the corresponding task object will be returned by this function.
The caller may examine the task and then dispose of it using @ref work_queue_task_delete.

If the task ran to completion, then the <tt>result</tt> field will be zero and the <tt>return_status</tt>
field will contain the Unix exit code of the task.
If the task could not, then the <tt>result</tt> field will be non-zero and the
<tt>return_status</tt> field will be undefined.

@param q A work queue object.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref WORK_QUEUE_WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty, or the timeout was reached without a completed task, or there is completed child process (call @ref process_wait to retrieve the status of the completed child process).
*/
struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout);

/** Determine whether the queue is 'hungry' for more tasks.
While the Work Queue can handle a very large number of tasks,
it runs most efficiently when the number of tasks is slightly
larger than the number of active workers.  This function gives
the user of a flexible application a hint about whether it would
be better to submit more tasks via @ref work_queue_submit or wait for some to complete
via @ref work_queue_wait.
@param q A work queue object.
@returns The number of additional tasks that can be efficiently submitted,
or zero if the queue has enough to work with right now.
*/
int work_queue_hungry(struct work_queue *q);

/** Determine whether the queue is empty.
When all of the desired tasks have been submitted to the queue,
the user should continue to call @ref work_queue_wait until
this function returns true.
@param q A work queue object.
@returns True if the queue is completely empty, false otherwise.
*/
int work_queue_empty(struct work_queue *q);

/** Get the listening port of the queue.
As noted in @ref work_queue_create, there are many controls that affect what TCP port the queue will listen on.
Rather than assuming a specific port, the user should simply call this function to determine what port was selected.
@param q A work queue object.
@return The port the queue is listening on.
*/
int work_queue_port(struct work_queue *q);

/** Get queue statistics (only from master).
@param q A work queue object.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s);

/** Get statistics of the master queue together with foremen information.
@param q A work queue object.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats_hierarchy(struct work_queue *q, struct work_queue_stats *s);

/** Get the task statistics for the given category.
@param q A work queue object.
@param c A category name.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats_category(struct work_queue *q, const char *c, struct work_queue_stats *s);


/** Get the current state of the task.
@param q A work queue object.
@param taskid The taskid of the task.
@return One of: WORK_QUEUE_TASK(UNKNOWN|READY|RUNNING|RESULTS|RETRIEVED|DONE)
*/
work_queue_task_state_t work_queue_task_state(struct work_queue *q, int taskid);

/** Limit the queue bandwidth when transferring files to and from workers.
@param q A work queue object.
@param bandwidth The bandwidth limit in bytes per second.
*/
void work_queue_set_bandwidth_limit(struct work_queue *q, const char *bandwidth);

/** Get current queue bandwidth.
@param q A work queue object.
@return The average bandwidth in MB/s measured by the master.
*/
double work_queue_get_effective_bandwidth(struct work_queue *q);

/** Summarize workers.
This function summarizes the workers currently connected to the master,
indicating how many from each worker pool are attached.
@param q A work queue object.
@return A newly allocated string describing the distribution of workers by pool.  The caller must release this string via free().
*/
char * work_queue_get_worker_summary( struct work_queue *q );

/** Turn on or off fast abort functionality for a given queue for tasks without
an explicit category. Given the multiplier, abort a task which running time is
larger than the average times the multiplier.  Fast-abort is computed per task
category. The value specified here applies to all the categories for which @ref
work_queue_activate_fast_abort_category was not explicitely called.
@param q A work queue object.
@param multiplier The multiplier of the average task time at which point to abort; if less than zero, fast_abort is deactivated (the default).
@returns 0 if activated, 1 if deactivated.
*/
int work_queue_activate_fast_abort(struct work_queue *q, double multiplier);


/** Turn on or off fast abort functionality for a given category. Given the
multiplier, abort a task which running time is larger than the average times the
multiplier.  The value specified here applies only to tasks in the given category.
(Note: work_queue_activate_fast_abort_category(q, "default", n) is the same as work_queue_activate_fast_abort(q, n).)
@param q A work queue object.
@param category A category name.
@param multiplier The multiplier of the average task time at which point to abort; if zero, fast_abort is deactivated. If less than zero (default), use the fast abort of the "default" category.
@returns 0 if activated, 1 if deactivated.
*/
int work_queue_activate_fast_abort_category(struct work_queue *q, const char *category, double multiplier);

/** Turn on or off first-allocation labeling for a given category. By default, only cores, memory, and disk are labeled. Turn on/off other specific resources use @ref work_queue_specify_category_autolabel_resource.
@param q A work queue object.
@param category A category name.
@param mode     One of @ref category_mode_t.
@returns 1 if mode is valid, 0 otherwise.
*/
int work_queue_specify_category_mode(struct work_queue *q, const char *category, category_mode_t mode);

/** Turn on or off first-allocation labeling for a given category and resource. This function should be use to fine-tune the defaults from @ref work_queue_specify_category_mode.
@param q A work queue object.
@param category A category name.
@param resource A resource name.
@param autolabel 0 off, 1 on.
@returns 1 if resource is valid, 0 otherwise.
*/
int work_queue_enable_category_resource(struct work_queue *q, const char *category, const char *resource, int autolabel);

/** Change the worker selection algorithm.
This function controls which <b>worker</b> will be selected for a given task.
@param q A work queue object.
@param algorithm The algorithm to use in assigning a task to a worker. See @ref work_queue_schedule_t for possible values.
*/
void work_queue_specify_algorithm(struct work_queue *q, work_queue_schedule_t algorithm);

/** Get the project name of the queue.
@param q A work queue object.
@return The project name of the queue.
*/
const char *work_queue_name(struct work_queue *q);

/** Change the project name for a given queue.
@param q A work queue object.
@param name The new project name.
*/
void work_queue_specify_name(struct work_queue *q, const char *name);

/** Change the priority for a given queue.
@param q A work queue object.
@param priority The new priority of the queue.  Higher priority masters will attract workers first.
*/
void work_queue_specify_priority(struct work_queue *q, int priority);

/** Specify the number of tasks not yet submitted to the queue.
	It is used by work_queue_factory to determine the number of workers to launch.
	If not specified, it defaults to 0.
	work_queue_factory considers the number of tasks as:
	num tasks left + num tasks running + num tasks read.
  @param q A work queue object.
  @param ntasks Number of tasks yet to be submitted.
  */
void work_queue_specify_num_tasks_left(struct work_queue *q, int ntasks);

/** Specify the catalog server the master should report to.
@param q A work queue object.
@param hostname The catalog server's hostname.
@param port The port the catalog server is listening on.
*/
void work_queue_specify_catalog_server(struct work_queue *q, const char *hostname, int port);

/** Specify the catalog server(s) the master should report to.
@param q A work queue object.
@param hosts The catalog servers given as a comma delimited list of hostnames or hostname:port
*/
void work_queue_specify_catalog_servers(struct work_queue *q, const char *hosts);

/** Cancel a submitted task using its task id and remove it from queue.
@param q A work queue object.
@param id The taskid returned from @ref work_queue_submit.
@return The task description of the cancelled task, or null if the task was not found in queue. The returned task must be deleted with @ref work_queue_task_delete or resubmitted with @ref work_queue_submit.
*/
struct work_queue_task *work_queue_cancel_by_taskid(struct work_queue *q, int id);

/** Cancel a submitted task using its tag and remove it from queue.
@param q A work queue object.
@param tag The tag name assigned to task using @ref work_queue_task_specify_tag.
@return The task description of the cancelled task, or null if the task was not found in queue. The returned task must be deleted with @ref work_queue_task_delete or resubmitted with @ref work_queue_submit.
*/
struct work_queue_task *work_queue_cancel_by_tasktag(struct work_queue *q, const char *tag);

/** Cancel all submitted tasks and remove them from the queue.
@param q A work queue object.
@return A @ref list of all of the tasks submitted to q.  Each task must be deleted with @ref work_queue_task_delete or resubmitted with @ref work_queue_submit.
*/
struct list * work_queue_cancel_all_tasks(struct work_queue *q);

/** Shut down workers connected to the work_queue system. Gives a best effort and then returns the number of workers given the shut down order.
@param q A work queue object.
@param n The number to shut down. All workers if given "0".
*/
int work_queue_shut_down_workers(struct work_queue *q, int n);

/** Delete a work queue.
This function should only be called after @ref work_queue_empty returns true.
@param q A work queue to delete.
*/
void work_queue_delete(struct work_queue *q);

/** Add a log file that records cummulative statistics of the connected workers and submitted tasks.
@param q A work queue object.
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int work_queue_specify_log(struct work_queue *q, const char *logfile);

/** Add a log file that records the states of the connected workers and tasks.
@param q A work queue object.
@param logfile The filename.
@return 1 if logfile was opened, 0 otherwise.
*/
int work_queue_specify_transactions_log(struct work_queue *q, const char *logfile);

/** Add a mandatory password that each worker must present.
@param q A work queue object.
@param password The password to require.
*/

void work_queue_specify_password( struct work_queue *q, const char *password );

/** Add a mandatory password file that each worker must present.
@param q A work queue object.
@param file The name of the file containing the password.
@return True if the password was loaded, false otherwise.
*/

int work_queue_specify_password_file( struct work_queue *q, const char *file );

/** Change the keepalive interval for a given queue.
@param q A work queue object.
@param interval The minimum number of seconds to wait before sending new keepalive checks to workers.
*/
void work_queue_specify_keepalive_interval(struct work_queue *q, int interval);

/** Change the keepalive timeout for identifying dead workers for a given queue.
@param q A work queue object.
@param timeout The minimum number of seconds to wait for a keepalive response from worker before marking it as dead.
*/
void work_queue_specify_keepalive_timeout(struct work_queue *q, int timeout);

/** Set the preference for using hostname over IP address to connect.
'by_ip' uses IP address (standard behavior), or 'by_hostname' to use the hostname at the master.
@param q A work queue object.
@param preferred_connection An string to indicate using 'by_ip' or a 'by_hostname'.
*/
void work_queue_master_preferred_connection(struct work_queue *q, const char *preferred_connection);

/** Tune advanced parameters for work queue.
@param q A work queue object.
@param name The name of the parameter to tune
 - "asynchrony-multiplier" Treat each worker as having (actual_cores * multiplier) total cores. (default = 1.0)
 - "asynchrony-modifier" Treat each worker as having an additional "modifier" cores. (default=0)
 - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=10)
 - "foreman-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)
 - "transfer-outlier-factor" Transfer that are this many times slower than the average will be aborted.  (default=10x)
 - "default-transfer-rate" The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)
 - "fast-abort-multiplier" Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)
 - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
 - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
@param value The value to set the parameter to.
@return 0 on succes, -1 on failure.
*/
int work_queue_tune(struct work_queue *q, const char *name, double value);

/** Sets the maximum resources a task without an explicit category ("default" category).
rm specifies the maximum resources a task in the default category may use.
@param q  Reference to the current work queue object.
@param rm Structure indicating maximum values. See @rmsummary for possible fields.
*/
void work_queue_specify_max_resources(struct work_queue *q,  const struct rmsummary *rm);

/** Sets the maximum resources a task in the category may use.
@param q         Reference to the current work queue object.
@param category  Name of the category.
@param rm Structure indicating maximum values. See @rmsummary for possible fields.
*/
void work_queue_specify_category_max_resources(struct work_queue *q,  const char *category, const struct rmsummary *rm);

/** Set the initial guess for resource autolabeling for the given category.
@param q         Reference to the current work queue object.
@param category  Name of the category.
@param rm Structure indicating maximum values. See @rmsummary for possible fields.
*/
void work_queue_specify_category_first_allocation_guess(struct work_queue *q,  const char *category, const struct rmsummary *rm);

/** Initialize first value of categories
@param q     Reference to the current work queue object.
@param rm Structure indicating maximum overall values. See @rmsummary for possible fields.
@param filename JSON file with resource summaries.
*/
void work_queue_initialize_categories(struct work_queue *q, struct rmsummary *max, const char *summaries_file);


//@}

/** @name Functions - Deprecated */

//@{

#define WORK_QUEUE_TASK_ORDER_FIFO 0  /**< Retrieve tasks based on first-in-first-out order. */
#define WORK_QUEUE_TASK_ORDER_LIFO 1  /**< Retrieve tasks based on last-in-first-out order. */

/** Specified how the submitted tasks should be ordered. It does not have any effect now.
@param q A work queue object.
@param order The ordering to use for dispatching submitted tasks:
- @ref WORK_QUEUE_TASK_ORDER_LIFO
- @ref WORK_QUEUE_TASK_ORDER_FIFO
*/
void work_queue_specify_task_order(struct work_queue *q, int order);


#define WORK_QUEUE_MASTER_MODE_STANDALONE 0 /**< Work Queue master does not report to the catalog server. */
#define WORK_QUEUE_MASTER_MODE_CATALOG 1    /**< Work Queue master reports to catalog server. */

/** Specify the master mode for a given queue.
@param q A work queue object.
@param mode
- @ref WORK_QUEUE_MASTER_MODE_STANDALONE - standalone mode. In this mode the master would not report its information to a catalog server;
- @ref WORK_QUEUE_MASTER_MODE_CATALOG - catalog mode. In this mode the master report itself to a catalog server where workers get masters' information and select a master to serve.
@deprecated Enabled automatically when @ref work_queue_specify_name is used.
*/
void work_queue_specify_master_mode(struct work_queue *q, int mode);


/** Change whether to estimate master capacity for a given queue.
@param q A work queue object.
@param estimate_capacity_on if the value of this parameter is 1, then work queue should estimate the master capacity. If the value is 0, then work queue would not estimate its master capacity.
@deprecated This feature is always enabled.
*/
void work_queue_specify_estimate_capacity_on(struct work_queue *q, int estimate_capacity_on);

/** Add an input buffer to a task.
@param t The task to which to add parameters
@param buf A pointer to the data buffer to send to the worker to be available to the commands.
@param length The number of bytes of data in the buffer
@param rname The name of the file in which to store the buffer data on the worker
@return 1 if the input buffer is successfully specified, 0 if either of @a t or @a rname is null or @a rname is an absolute path.
@deprecated Use @ref work_queue_task_specify_buffer instead.
*/
int work_queue_task_specify_input_buf(struct work_queue_task *t, const char *buf, int length, const char *rname);

/** Add an input file to a task.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@return 1 if the input file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref work_queue_task_specify_file instead.
*/
int work_queue_task_specify_input_file(struct work_queue_task *t, const char *fname, const char *rname);

/** Add an input file to a task, without caching.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@return 1 if the input file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref work_queue_task_specify_file instead.
*/
int work_queue_task_specify_input_file_do_not_cache(struct work_queue_task *t, const char *fname, const char *rname);

/** Add an output file to a task.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@return 1 if the output file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref work_queue_task_specify_file instead.
*/
int work_queue_task_specify_output_file(struct work_queue_task *t, const char *rname, const char *fname);

/** Add an output file to a task without caching.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@return 1 if the output file is successfully specified, 0 if either of @a t, @a fname, or @a rname is null or @a rname is an absolute path.
@deprecated See @ref work_queue_task_specify_file instead.
*/
int work_queue_task_specify_output_file_do_not_cache(struct work_queue_task *t, const char *rname, const char *fname);

/** Generate a worker-level unique filename to indicate a disk allocation being full.
 @param p The process for which we generate a unique disk allocation filename.
 @return The string corresponding to the filename.
*/
char *work_queue_generate_disk_alloc_full_filename(char *pwd, int taskid);

//@}

#endif
