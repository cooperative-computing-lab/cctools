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

#define WORK_QUEUE_DEFAULT_PORT 9123
#define WORK_QUEUE_LINE_MAX 1024

#define WAITFORTASK -1

#define WQ_RESULT_UNSET 0
#define WQ_RESULT_INPUT_FAIL 1
#define WQ_RESULT_FUNCTION_FAIL 2
#define WQ_RESULT_OUTPUT_FAIL 3

#define WORK_QUEUE_CHOOSE_HOST_UNSET 0 // default setting for task.
#define WORK_QUEUE_CHOOSE_HOST_BY_FCFS 1
#define WORK_QUEUE_CHOOSE_HOST_BY_FILES 2
#define WORK_QUEUE_CHOOSE_HOST_BY_TIME 3
#define WORK_QUEUE_CHOOSE_HOST_MAX 3
#define WORK_QUEUE_CHOOSE_HOST_DEFAULT 1 // default setting for queue.


/** A task description.  This structure should only be created with @ref work_queue_task_create and delete with @ref work_queue_task_delete.  You may examine (but not modify) this structure once a task has completed.
*/
struct work_queue_task {
	char *tag;
        char *command_line;		/**< The program(s) to execute, as a shell command line. */
	int worker_selection_algorithm;           /**< How to choose worker to run the task. */
	char *output;			/**< The standard output of the task. */
	struct list * input_files;      /**< The files to transfer to the worker and place in the executing directory. */
	struct list * output_files;	/**< The output files (other than the standard output stream) created by the program expected to be retrieved from the task. */
	int taskid;			/**< A unique task id number. */
	int return_status;		/**< The exit code of the command line. */
	int result;			/**< The result of the task (successful, failed return_status, missing input file, missing output file). */
	char* host;			/**< The name of the host on which it ran. */
	timestamp_t submit_time;	/**< The time the task was submitted. */
	timestamp_t start_time;		/**< The time at which the task began. */
	timestamp_t finish_time;	/**< The time at which it completed. */
    	INT64_T total_bytes_transfered;
	timestamp_t total_transfer_time;
};

/** Statistics describing a work queue. */

struct work_queue_stats {
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
};

/** Create a new work queue.
@param port The port number to listen on, or zero to choose a default.  The default port is 9123, but can be overridden by the environment variable WORK_QUEUE_PORT.
@param stoptime The time at which to return null if not yet able to be created.
@return A new work queue, or null if it could not be created.
*/
struct work_queue * work_queue_create( int port , time_t stoptime);

/** Delete a work queue.
@param q The work queue to delete.
*/
void work_queue_delete( struct work_queue *q );

/** Get queue statistics.
@param q The queue to query.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats( struct work_queue *q, struct work_queue_stats *s );

/** Turn on fast abort functionality for a given queue.
@param q A pointer to the queue to modify.
@param multiplier The multiplier of the average task time at which point to abort.
@returns 0 if activated with appropriate multiplier, 1 if activated with the default multiplier.
*/
int work_queue_activate_fast_abort(struct work_queue* q, double multiplier);

/** Wait for tasks to complete.  This call will block until the timeout has elapsed.
@param q The work queue to wait on.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty or the timeout was reached without a completed task.
*/
struct work_queue_task * work_queue_wait( struct work_queue *q, int timeout );

/** Submit a job to a work queue.
@param q A work queue returned from @ref work_queue_create.
@param t A task description returned from @ref work_queue_task_create.
*/
void work_queue_submit( struct work_queue *q, struct work_queue_task *t );

/** Delete a task specification.  This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task specification to delete.
*/
void work_queue_task_delete( struct work_queue_task *t );

/** Create a new task specification.  Once created, the task may be passed to @ref work_queue_submit.
@param full_command The shell command line to be executed by the task.
*/
struct work_queue_task * work_queue_task_create( const char* full_command);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. 
@param t The task to which to add parameters
@param tag The tag to attatch to tast t.
*/
INT64_T work_queue_task_specify_tag( struct work_queue_task* t, const char* tag);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. 
@param t The task to which to add parameters
@param alg The algorithm to use in assigning a task to a worker. Valid possibilities are defined in this file as "CHOOSE_HOST_BY" values.
*/
INT64_T work_queue_task_specify_algorithm( struct work_queue_task* t, int alg);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. 
@param t The task to which to add parameters
@param buf A pointer to the data buffer to send to the worker to be available to the commands.
@param length The number of bytes of data in the buffer
@param rname The name of the file in which to store the buffer data on the worker
*/
INT64_T work_queue_task_specify_input_buf( struct work_queue_task* t, const char* buf, int length, const char* rname);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. 
@param t The task to which to add parameters
@param fname The name of the data file to send to the worker to be available to the commands.
@param rname The name of the file in which to store the buffer data on the worker
*/
INT64_T work_queue_task_specify_input_file( struct work_queue_task* t, const char* fname, const char* rname);

/** Further define a task specification.  Once completed, the task may be passed to @ref work_queue_submit. If no file is defined, the program will have default (no) output files retrieved.
@param t The task to which to add parameters
@param rname The name of a file created by the program when it runs.
@param fname The name of the file local target for copying rname back.
*/
INT64_T work_queue_task_specify_output_file( struct work_queue_task* t, const char* rname, const char* fname);

/** Determine whether the queue can support more tasks. Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
@param q A pointer to the queue to query.
*/
int work_queue_hungry (struct work_queue* q);

/** Determine whether there are any known tasks queued, running, or waiting to be collected. Returns 1 if so and 0 if "empty".
@param q A pointer to the queue to query.
*/
int work_queue_empty (struct work_queue* q);

/** Shut down workers connected to the work_queue system. Gives a best effort and then returns the number of workers given the shut down order.
@param q A pointer to the queue to query.
@param n The number to shut down. All workers if given "0".
*/
int work_queue_shut_down_workers (struct work_queue* q, int n);

/** Delete files stored on the workers.
@param q A pointer to the queue to query.
@param exceptions A list of files to keep, null if empty.
*/
//int work_queue_delete_local_state (struct work_queue* q, struct task_file* exceptions);

/** Resubmit a completed task with the exact same specification, for instance, if it failed and you want to retry it.
@param q A pointer to the queue to query.
@param t A fully defined task struct to resubmit.
*/
//int work_queue_resubmit (struct work_queue* q, struct work_queue_task* t);

#endif
