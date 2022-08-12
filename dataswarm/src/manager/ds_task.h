/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_TASK_H
#define DS_TASK_H

#include "dataswarm.h"

#include "list.h"
#include "category.h"

#include <stdint.h>

struct ds_task {
	char *tag;                                        /**< An optional user-defined logical name for the task. */
	char *command_line;                               /**< The program(s) to execute, as a shell command line. */
	ds_schedule_t worker_selection_algorithm; /**< How to choose worker to run the task. */
	char *output;                                     /**< The standard output of the task. */
	struct list *input_files;                         /**< The files to transfer to the worker and place in the executing directory. */
	struct list *output_files;                        /**< The output files (other than the standard output stream) created by the program to be retrieved from the task. */
	struct list *env_list;                            /**< Environment variables applied to the task. */
	int taskid;                                       /**< A unique task id number. */
	int exit_code;                                   /**< The exit code of the command line. */
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
};

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
This field is not interpreted by the manager, but is provided for the user's convenience
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

@param t A ds_task object.
@param monitor_snapshot_file A filename.
@return 1 if the task file is successfully specified, 0 if either of @a t, or @a monitor_snapshot_file is null.
*/

int ds_task_specify_snapshot_file(struct ds_task *t, const char *monitor_snapshot_file);

#endif
