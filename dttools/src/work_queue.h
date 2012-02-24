/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef WORK_QUEUE_H
#define WORK_QUEUE_H

/** @file work_queue.h A master-worker library.
 The work queue provides an implementation of the master-worker computing
model using TCP sockets, Unix applications, and files as intermediate buffers.
A master process uses @ref work_queue_create to create a queue, then
@ref work_queue_submit to submit tasks.  Once tasks are running, call
@ref work_queue_wait to wait for completion.  The generic <tt>worker</tt>
program can be run on any machine, and simply needs to be told the host
and port of the master.
*/

#include "timestamp.h"

#define WORK_QUEUE_SWITCH_UNSPECIFIED -1
#define WORK_QUEUE_SWITCH_OFF 0
#define WORK_QUEUE_SWITCH_ON  1

#define WORK_QUEUE_DEFAULT_PORT 9123 /**< Default Work Queue port number. */
#define WORK_QUEUE_RANDOM_PORT -1    /**< Indicate to Work Queue to choose a random open port. */
#define WORK_QUEUE_LINE_MAX 1024

#define WORK_QUEUE_WAITFORTASK -1   /**< Wait for a task to complete before returning. */

#define WORK_QUEUE_RETURN_STATUS_UNSET -1

#define WORK_QUEUE_RESULT_UNSET 0
#define WORK_QUEUE_RESULT_INPUT_FAIL 1
#define WORK_QUEUE_RESULT_INPUT_MISSING 2
#define WORK_QUEUE_RESULT_FUNCTION_FAIL 4
#define WORK_QUEUE_RESULT_OUTPUT_FAIL 8
#define WORK_QUEUE_RESULT_OUTPUT_MISSING 16
#define WORK_QUEUE_RESULT_LINK_FAIL 32

#define WORK_QUEUE_SCHEDULE_UNSET 0
#define WORK_QUEUE_SCHEDULE_FCFS 1	/**< Select worker on a first-come-first-serve basis. */
#define WORK_QUEUE_SCHEDULE_FILES 2	/**< Select worker that has the most files required by task. */
#define WORK_QUEUE_SCHEDULE_TIME 3	/**< Select worker that has has best execution time. */
#define WORK_QUEUE_SCHEDULE_DEFAULT 3	/**< Default algorithm (@ref WORK_QUEUE_SCHEDULE_TIME). */
#define WORK_QUEUE_SCHEDULE_PREFERRED_HOSTS 4 /**< Select worker from set of preferred hosts. */
#define WORK_QUEUE_SCHEDULE_RAND 5	/**< Select a random worker. */
#define WORK_QUEUE_SCHEDULE_MAX 5

#define WORK_QUEUE_INPUT  0	/**< Specify an input object. */
#define WORK_QUEUE_OUTPUT 1	/**< Specify an output object. */

#define WORK_QUEUE_NOCACHE 0	/**< Do not cache file at execution site. */
#define WORK_QUEUE_CACHE 1	/**< Cache file at execution site for later use. */
#define WORK_QUEUE_SYMLINK 2	/**< Create a symlink to the file rather than copying it, if possible. */
#define WORK_QUEUE_PREEXIST 4
#define WORK_QUEUE_THIRDGET 8	/**< Access the file on the client from a shared filesystem */
#define WORK_QUEUE_THIRDPUT 8	/**< Access the file on the client from a shared filesystem (included for readability) */

#define WORK_QUEUE_MASTER_MODE_STANDALONE 0 /**< Work Queue master does not report to the catalog server. */
#define WORK_QUEUE_MASTER_MODE_CATALOG 1    /**< Work Queue master reports to catalog server. */
#define WORK_QUEUE_NAME_MAX 256
#define WORK_QUEUE_MASTER_PRIORITY_MAX 100
#define WORK_QUEUE_MASTER_PRIORITY_DEFAULT 10
#define WORK_QUEUE_WORKER_MODE_SHARED 0	    /**< Work Queue master accepts workers in shared or non-exclusive mode. */
#define WORK_QUEUE_WORKER_MODE_EXCLUSIVE 1  /**< Work Queue master only accepts workers that have a preference for it. */

#define WORK_QUEUE_WAIT_UNSPECIFIED -1
#define WORK_QUEUE_WAIT_FCFS 0				/**< First come first serve. */
#define WORK_QUEUE_WAIT_FAST_DISPATCH 1		/**< Dispatch task to new workers first. */
#define WORK_QUEUE_WAIT_ADAPTIVE 2			/**< If master is busy, do not use new workers. */

#define WORK_QUEUE_APP_TIME_OUTLIER_MULTIPLIER 10

#define WORK_QUEUE_CAPACITY_TOLERANCE_MAX 1000
#define WORK_QUEUE_CAPACITY_TOLERANCE_DEFAULT 1

#define WORK_QUEUE_WORKERS_NO_LIMIT -1

#define WORK_QUEUE_FS_CMD 1
#define WORK_QUEUE_FS_PATH 2
#define WORK_QUEUE_FS_SYMLINK 3

extern double wq_option_fast_abort_multiplier; /**< Initial setting for fast abort multiplier upon creating queue. Turned off if less than 0. Change prior to calling work_queue_create, after queue is created this variable is not considered and changes must be made through the API calls. */
extern int wq_option_scheduler;	/**< Initial setting for algorithm to assign tasks to workers upon creating queue . Change prior to calling work_queue_create, after queue is created this variable is not considered and changes must be made through the API calls.   */

/** A task description.  This structure should only be created with @ref work_queue_task_create and delete with @ref work_queue_task_delete.  You may examine (but not modify) this structure once a task has completed.
*/
struct work_queue_task {
	char *tag;			/**< An optional user-defined logical name for the task. */
	char *command_line;		/**< The program(s) to execute, as a shell command line. */
	int worker_selection_algorithm;		  /**< How to choose worker to run the task. */
	char *output;			/**< The standard output of the task. */
	struct list *input_files;	/**< The files to transfer to the worker and place in the executing directory. */
	struct list *output_files;	/**< The output files (other than the standard output stream) created by the program expected to be retrieved from the task. */
	char *preferred_host;		/**< The hostname where the task should preferrentially be run. */
	int taskid;			/**< A unique task id number. */
	int status;	/**< Current status of the task. */
	int return_status;		/**< The exit code of the command line. */
	int result;			/**< The result of the task (successful, failed return_status, missing input file, missing output file). */
	char *host;			/**< The name of the host on which it ran. */

	timestamp_t time_task_submit;	/**< The time at which this task was submitted */
	timestamp_t time_task_finish;	/**< The time at which this task was finished */
	timestamp_t time_app_delay;	 /**< time spent in upper-level application (outside of work_queue_wait)>*/
	timestamp_t time_send_input_start;	/**< The time at which it started to transfer input files. */
	timestamp_t time_send_input_finish;	/**< The time at which it finished transferring input files. */
	timestamp_t time_execute_cmd_start;		    /**< The time at which the task began. */
	timestamp_t time_execute_cmd_finish;		/**< The time at which the task finished (discovered by the master). */
	timestamp_t time_receive_output_start;	/**< The time at which it started to transfer output files. */
	timestamp_t time_receive_output_finish;	/**< The time at which it finished transferring output files. */

	INT64_T total_bytes_transferred;/**< Number of bytes transferred since task has last started transferring input data. */
	timestamp_t total_transfer_time;    /**< Time comsumed in microseconds for transferring total_bytes_transferred. */
	timestamp_t cmd_execution_time;	   /**< Time spent in microseconds for executing the command on the worker. */
};

/** Statistics describing a work queue. */

struct work_queue_stats {
	int port;
	int priority;
	int workers_init;		/**< Number of workers initializing. */
	int workers_ready;		/**< Number of workers ready for tasks. */
	int workers_busy;		/**< Number of workers running tasks. */
	int tasks_running;		/**< Number of tasks currently running. */
	int tasks_waiting;		/**< Number of tasks waiting for a CPU. */
	int tasks_complete;		/**< Number of tasks waiting to be returned to user. */
	int total_tasks_dispatched;	/**< Total number of tasks dispatch to workers. */
	int total_tasks_complete;	/**< Total number of tasks returned complete. */
	int total_workers_joined;	/**< Total number of times a worker joined the queue. */
	int total_workers_removed;	/**< Total number of times a worker was removed from the queue. */
	INT64_T total_bytes_sent;   /**< Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the master. */
	INT64_T total_bytes_received;	/**< Total number of file bytes (not including protocol control msg bytes) received from the workers by the master. */
	timestamp_t start_time;     /**<The time at which the master started. */
	timestamp_t total_send_time;/**<Total time in microseconds spent in sending data to workers. */
	timestamp_t total_receive_time;
				    /**<Total time in microseconds spent in receiving data from workers. */
	double efficiency;
	double idle_percentage;
	int capacity;
	int avg_capacity;
	int total_workers_connected;
	int excessive_workers_removed;
};

/** @name Functions - Tasks */

//@{

/** Create a new task specification.  Once created, the task may be passed to @ref work_queue_submit.
@param full_command The shell command line to be executed by the task.
*/
struct work_queue_task *work_queue_task_create(const char *full_command);

/** Add a file to a task.
@param t The task to which to add a file.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the execution site.
@param type Must be one of the following values:
- WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling, or any of the or'd together:
		@ref WORK_QUEUE_NOCACHE,
		@ref WORK_QUEUE_CACHE,
		@ref WORK_QUEUE_SYMLINK,
		@ref WORK_QUEUE_THIRDGET,
		@ref WORK_QUEUE_THIRDPUT.
*/
void work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags);

/** Add an input buffer to a task.
@param t The task to which to add a file.
@param data The contents of the buffer to pass as input.
@param length The length of the buffer, in bytes
@param remote_name The name of the remote file to create.
@param flags May take the same values as in @ref work_queue_task_specify_file.
*/
void work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags);

/** Add a file created or handled by an arbitrary command to a task (eg: wget, ftp, chirp_get|put).
@param t The task to which to add a file.
@param remote_name The name of the file at the execution site.
@param cmd The command to run on the remote node to retrieve or store the file.
@param type	Must be one of the following values:
		@ref WORK_QUEUE_INPUT or
		@ref WORK_QUEUE_OUTPUT.
@param flags	May be zero to indicate no special handling, or any of the following or'd together:
		@ref WORK_QUEUE_NOCACHE,
		@ref WORK_QUEUE_CACHE.
*/
void work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags);

/** Attach a user defined logical name to the task.
This field is not interpreted by the work queue, but simply maintained to help the user track tasks.
@param t The task to which to add parameters
@param tag The tag to attach to task t.
*/
void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. 
@param t The task to which to add parameters
@param alg The algorithm to use in assigning a task to a worker. Valid possibilities are defined in this file as "WORK_QUEUE_SCHEDULE_X" values.
*/
int work_queue_task_specify_algorithm(struct work_queue_task *t, int alg);

/** Indicate that the task would be optimally run on a given host.
@param t The task to which to add parameters
@param hostname The hostname to which this task would optimally be sent.
*/
void work_queue_task_specify_preferred_host(struct work_queue_task *t, const char *hostname);

/** Delete a task specification.  This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task specification to delete.
*/
void work_queue_task_delete(struct work_queue_task *t);

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

@param port The port number to listen on.  If zero is specified, then the default is chosen, and if -1 is specified, a random port is chosen.  
@return A new work queue, or null if it could not be created.
*/
struct work_queue *work_queue_create(int port);

/** Submit a job to a work queue.
It is safe to re-submit a task returned by @ref work_queue_wait.
@param q A work queue returned from @ref work_queue_create.
@param t A task description returned from @ref work_queue_task_create.
*/
void work_queue_submit(struct work_queue *q, struct work_queue_task *t);

/** Wait for tasks to complete.  This call will block until the timeout has elapsed.
@param q The work queue to wait on.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant @ref WORK_QUEUE_WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty or the timeout was reached without a completed task.  The returned task must be deleted with @ref work_queue_task_delete or resubmitted with @ref work_queue_submit.
*/
struct work_queue_task *work_queue_wait(struct work_queue *q, int timeout);

/** Determine whether the queue can support more tasks. Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
@param q A pointer to the queue to query.
*/
int work_queue_hungry(struct work_queue *q);

/** Determine whether there are any known tasks queued, running, or waiting to be collected. Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
@param q A pointer to the queue to query.
*/
int work_queue_empty(struct work_queue *q);

/** Get the listening port of the queue.
@param q The work queue of interest.
@return The port the queue is listening on.
*/
int work_queue_port(struct work_queue *q);

/** Get the project name of the queue.
@param q The work queue of interest.
@return The project name of the queue.
*/
const char *work_queue_name(struct work_queue *q);

/** Get queue statistics.
@param q The queue to query.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s);

/** Turn on or off fast abort functionality for a given queue.
@param q A pointer to the queue to modify.
@param multiplier The multiplier of the average task time at which point to abort; if negative (and by default) fast_abort is deactivated.
@returns 0 if activated or deactivated with an appropriate multiplier, 1 if deactivated due to inappropriate multiplier.
*/
int work_queue_activate_fast_abort(struct work_queue *q, double multiplier);

/** Change the worker selection algorithm for a given queue.
@param q A pointer to the queue to modify.
@param alg The algorithm to use in assigning a task to a worker. Valid possibilities are defined in this file as "WORK_QUEUE_SCHEDULE_X" values.
*/
int work_queue_specify_algorithm(struct work_queue *q, int alg);

/** Change the project name for a given queue.
@param q A pointer to the queue to modify.
@param name The new project name.
*/
int work_queue_specify_name(struct work_queue *q, const char *name);

/** Change the priority for a given queue.
@param q A pointer to the queue to modify.
@param priority An integer that presents the priorty of this work queue master. The higher the value, the higher the priority.
@return The priority that has been set.
*/
int work_queue_specify_priority(struct work_queue *q, int priority);

/** Specify the master mode for a given queue. 
@param q A pointer to the queue to modify.
@param mode 
<b>mode == @ref WORK_QUEUE_MASTER_MODE_STANDALONE</b>: standalone mode. In this mode the master would not report its information to a catalog server; 
<b>mode == @ref WORK_QUEUE_MASTER_MODE_CATALOG</b>: catalog mode. In this mode the master report itself to a catalog server where workers get masters' information and select a master to serve.
@return The mode that has been set.
*/
int work_queue_specify_master_mode(struct work_queue *q, int mode);

/** Specify the worker mode for a given queue. 
@param q A pointer to the queue to modify.
@param mode 
<b>mode == @ref WORK_QUEUE_WORKER_MODE_SHARED</b>: shared mode. In this mode the master would accept connections from shared workers;
<b>mode == @ref WORK_QUEUE_WORKER_MODE_EXCLUSIVE</b>: exclusive mode. In this mode the master would only accept workers that have specified a preference on it, which are the workers started with "-N name" where name is the name of the queue. 
@return The mode that has been set.
*/
int work_queue_specify_worker_mode(struct work_queue *q, int mode);

/** Shut down workers connected to the work_queue system. Gives a best effort and then returns the number of workers given the shut down order.
@param q A pointer to the queue to query.
@param n The number to shut down. All workers if given "0".
*/
int work_queue_shut_down_workers(struct work_queue *q, int n);

/** Delete a work queue.
@param q The work queue to delete.
*/
void work_queue_delete(struct work_queue *q);

//@}

/** @name Functions - Deprecated */

//@{

/** Add an input buffer to a task.
@param t The task to which to add parameters
@param buf A pointer to the data buffer to send to the worker to be available to the commands.
@param length The number of bytes of data in the buffer
@param rname The name of the file in which to store the buffer data on the worker
@deprecated Use @ref work_queue_task_specify_buffer instead.
*/
void work_queue_task_specify_input_buf(struct work_queue_task *t, const char *buf, int length, const char *rname);

/** Add an input file to a task.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@deprecated See @ref work_queue_task_specify_file instead.
*/
void work_queue_task_specify_input_file(struct work_queue_task *t, const char *fname, const char *rname);

/** Add an input file to a task, without caching.
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker.
@deprecated See @ref work_queue_task_specify_file instead.
*/
void work_queue_task_specify_input_file_do_not_cache(struct work_queue_task *t, const char *fname, const char *rname);

/** Add an output file to a task.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@deprecated See @ref work_queue_task_specify_file instead.
*/
void work_queue_task_specify_output_file(struct work_queue_task *t, const char *rname, const char *fname);

/** Add an output file to a task without caching.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
@deprecated See @ref work_queue_task_specify_file instead.
*/
void work_queue_task_specify_output_file_do_not_cache(struct work_queue_task *t, const char *rname, const char *fname);

//@}

#endif
