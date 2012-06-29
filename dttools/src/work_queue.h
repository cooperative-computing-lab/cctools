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

#define WORK_QUEUE_DEFAULT_PORT 9123  /**< Default Work Queue port number. */
#define WORK_QUEUE_RANDOM_PORT  0    /**< Indicates that any port number may be chosen. */
#define WORK_QUEUE_WAITFORTASK  -1    /**< Wait for a task to complete before returning. */

#define WORK_QUEUE_SCHEDULE_UNSET 0
#define WORK_QUEUE_SCHEDULE_FCFS	 1 /**< Select worker on a first-come-first-serve basis. */
#define WORK_QUEUE_SCHEDULE_FILES	 2 /**< Select worker that has the most data required by the task. */
#define WORK_QUEUE_SCHEDULE_TIME	 3 /**< Select worker that has the fastest execution time on previous tasks. */
#define WORK_QUEUE_SCHEDULE_RAND	 4 /**< Select a random worker. */

#define WORK_QUEUE_TASK_ORDER_FIFO 0  /**< Retrieve tasks based on first-in-first-out order. */
#define WORK_QUEUE_TASK_ORDER_LIFO 1  /**< Retrieve tasks based on last-in-first-out order. */
 
#define WORK_QUEUE_INPUT  0	/**< Specify an input object. */
#define WORK_QUEUE_OUTPUT 1	/**< Specify an output object. */

#define WORK_QUEUE_NOCACHE 0	/**< Do not cache file at execution site. */
#define WORK_QUEUE_CACHE 1	/**< Cache file at execution site for later use. */
#define WORK_QUEUE_SYMLINK 2	/* Create a symlink to the file rather than copying it, if possible. */
#define WORK_QUEUE_PREEXIST 4   /* If the filename already exists on the host, use it in place. */
#define WORK_QUEUE_THIRDGET 8	/* Access the file on the client from a shared filesystem */
#define WORK_QUEUE_THIRDPUT 8	/* Access the file on the client from a shared filesystem (included for readability) */

#define WORK_QUEUE_MASTER_MODE_STANDALONE 0 /**< Work Queue master does not report to the catalog server. */
#define WORK_QUEUE_MASTER_MODE_CATALOG 1    /**< Work Queue master reports to catalog server. */

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
	int taskid;			/**< A unique task id number. */
	int return_status;		/**< The exit code of the command line. */
	int result;			/**< The result of the task (successful, failed return_status, missing input file, missing output file). */
	char *host;			/**< The address and port of the host on which it ran. */
	char *hostname;			/**< The name of the host on which it ran. */		

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
	int workers_cancelling;	/**< Number of workers aborting their tasks. */
	int tasks_running;		/**< Number of tasks currently running. */
	int tasks_waiting;		/**< Number of tasks waiting for a CPU. */
	int tasks_complete;		/**< Number of tasks waiting to be returned to user. */
	int total_tasks_dispatched;	/**< Total number of tasks dispatch to workers. */
	int total_tasks_complete;	/**< Total number of tasks returned complete. */
	int total_workers_joined;	/**< Total number of times a worker joined the queue. */
	int total_workers_removed;	/**< Total number of times a worker was removed from the queue. */
	INT64_T total_bytes_sent;	/**< Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the master. */
	INT64_T total_bytes_received;	/**< Total number of file bytes (not including protocol control msg bytes) received from the workers by the master. */
	timestamp_t start_time;		/**< Absolute time at which the master started. */
	timestamp_t total_send_time;	/**< Total time in microseconds spent in sending data to workers. */
	timestamp_t total_receive_time;	/**< Total time in microseconds spent in receiving data from workers. */
	double efficiency;
	double idle_percentage;
	int capacity;
	int avg_capacity;
	int total_workers_connected;
};


/** @name Functions - Tasks */

//@{

/** Create a new task object.
Once created and elaborated with functions such as @ref work_queue_task_specify_file
and @ref work_queue_task_specify_buffer, the task should be passed to @ref work_queue_submit.
@param full_command The shell command line to be executed by the task.
@return A new task object.
*/
struct work_queue_task *work_queue_task_create(const char *full_command);

/** Add a file to a task.
@param t A task object.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the remote execution site.
@param type Must be one of the following values:
- @ref WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- @ref WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of the following or'd together:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
*/
void work_queue_task_specify_file(struct work_queue_task *t, const char *local_name, const char *remote_name, int type, int flags);

/** Add an input buffer to a task.
@param t A task object.
@param data The data to be passed as an input file.
@param length The length of the buffer, in bytes
@param remote_name The name of the remote file to create.
@param flags	May be zero to indicate no special handling or any of the following or'd together:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
*/
void work_queue_task_specify_buffer(struct work_queue_task *t, const char *data, int length, const char *remote_name, int flags);

/* Add a file created or handled by an arbitrary command to a task (eg: wget, ftp, chirp_get|put).
@param t A task object.
@param remote_name The name of the file at the execution site.
@param cmd The command to run on the remote node to retrieve or store the file.
@param type Must be one of the following values:
- @ref WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- @ref WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags	May be zero to indicate no special handling or any of the following or'd together:
- @ref WORK_QUEUE_CACHE indicates that the file should be cached for later tasks. (recommended)
- @ref WORK_QUEUE_NOCACHE indicates that the file should not be cached for later tasks.
*/
void work_queue_task_specify_file_command(struct work_queue_task *t, const char *remote_name, const char *cmd, int type, int flags);

/** Attach a user defined string tag to the task.
This field is not interpreted by the work queue, but is provided for the user's convenience
in identifying tasks when they complete.
@param t A task object.
@param tag The tag to attach to task t.
*/
void work_queue_task_specify_tag(struct work_queue_task *t, const char *tag);

/** Select the scheduling algorithm for a single task.
To change the scheduling algorithm for all tasks, use @ref work_queue_specify_algorithm instead.
@param t A task object.
@param algo The algorithm to use in assigning this task to a worker:
- @ref WORK_QUEUE_SCHEDULE_FCFS	 - Select worker on a first-come-first-serve basis.
- @ref WORK_QUEUE_SCHEDULE_FILES - Select worker that has the most data required by the task.
- @ref WORK_QUEUE_SCHEDULE_TIME	 - Select worker that has the fastest execution time on previous tasks.
- @ref WORK_QUEUE_SCHEDULE_RAND	 - Select a random worker.
*/
void work_queue_task_specify_algorithm(struct work_queue_task *t, int algo );

/** Delete a task.
This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task to delete.
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

/** Submit a task to a queue.
Once a task is submitted to a queue, it is not longer under the user's
control and should not be inspected until returned via @ref work_queue_wait.
Once returned, it is safe to re-submit the same take object via @ref work_queue_submit.
@param q A work queue object.
@param t A task object returned from @ref work_queue_task_create.
@return An integer taskid assigned to the submitted task. 
*/
int work_queue_submit(struct work_queue *q, struct work_queue_task *t);

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
@returns A completed task description, or null if the queue is empty or the timeout was reached without a completed task.
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

/** Get the project name of the queue.
@param q A work queue object.
@return The project name of the queue.
*/
const char *work_queue_name(struct work_queue *q);

/** Get queue statistics.
@param q A work queue object.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats(struct work_queue *q, struct work_queue_stats *s);

/** Summarize workers.
This function summarizes the workers currently connected to the master,
indicating how many from each worker pool are attached.
@param q A work queue object.
@return A newly allocated string describing the distribution of workers by pool.  The caller must release this string via free().
*/
char * work_queue_get_worker_summary( struct work_queue *q );

/** Turn on or off fast abort functionality for a given queue.
@param q A work queue object.
@param multiplier The multiplier of the average task time at which point to abort; if negative (and by default) fast_abort is deactivated.
@returns 0 if activated or deactivated with an appropriate multiplier, 1 if deactivated due to inappropriate multiplier.
*/
int work_queue_activate_fast_abort(struct work_queue *q, double multiplier);

/** Change the worker selection algorithm.
Note that this function controls which <b>worker</b> will be selected
for a given task while @ref work_queue_specify_task_order controls which <b>task</b>
will be executed next.
@param q A work queue object.
@param algo The algorithm to use in assigning a task to a worker:
- @ref WORK_QUEUE_SCHEDULE_FCFS	 - Select worker on a first-come-first-serve basis.
- @ref WORK_QUEUE_SCHEDULE_FILES - Select worker that has the most data required by the task.
- @ref WORK_QUEUE_SCHEDULE_TIME	 - Select worker that has the fastest execution time on previous tasks.
- @ref WORK_QUEUE_SCHEDULE_RAND	 - Select a random worker.
*/
void work_queue_specify_algorithm(struct work_queue *q, int algo);

/** Specify how the submitted tasks should be ordered.
Note that this function controls which <b>task</b> to execute next,
while @ref work_queue_specify_algorithm controls which <b>worker</b>
it should be assigned to.
@param q A work queue object.
@param order The ordering to use for dispatching submitted tasks:
- @ref WORK_QUEUE_TASK_ORDER_LIFO
- @ref WORK_QUEUE_TASK_ORDER_FIFO
*/
void work_queue_specify_task_order(struct work_queue *q, int order);

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

/** Specify the master mode for a given queue. 
@param q A work queue object.
@param mode 
- @ref WORK_QUEUE_MASTER_MODE_STANDALONE - standalone mode. In this mode the master would not report its information to a catalog server; 
- @ref WORK_QUEUE_MASTER_MODE_CATALOG - catalog mode. In this mode the master report itself to a catalog server where workers get masters' information and select a master to serve.
*/
void work_queue_specify_master_mode(struct work_queue *q, int mode);

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

/** Add a log file that records the states of the connected workers and submitted tasks.
@param q A work queue object.
@param logfile The filename.
*/
void work_queue_specify_log(struct work_queue *q, const char *logfile);

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
