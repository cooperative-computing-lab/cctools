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

#define WORK_QUEUE_LINE_MAX 1024


/** A file for input to a task. */
struct task_file {
    short fname_or_literal; //0 or 1
    int length;
    void* payload;
    char* remote_name;
};

/** A task description.  This structure should only be created with @ref work_queue_task_create and delete with @ref work_queue_task_delete.  You may examine (but not modify) this structure once a task has completed.
*/
struct work_queue_task {
	char *tag;			/**< The user-defined description of the task. */
	char *command;			/**< The command line to execute. */
	char *output;			/**< The standard output of the task. */
	struct task_file* input_files;  /**< The input files to send to the task. */
	int num_inputs;         	/**< The number of input files to send. */
	char *output_files;		/**< The output files to retrieve from the task. */
	int result;			/**< The exit code of the command line. */
	int taskid;			/**< A unique task id number. */
	int priority;			/**< The relative priority of the job, higher is better. */
	char host[32];			/**< The name of the host on which it ran. */
	timestamp_t start_time;		/**< The time at which the task began. */
	timestamp_t finish_time;	/**< The time at which it completed. */
    	int total_bytes_transfered;
	timestamp_t total_transfer_time;
};

/** Statistics describing a work queue. */

struct work_queue_stats {
	int workers_init;		/** Number of workers initializing. */
	int workers_ready;		/** Number of workers ready for tasks. */
	int workers_busy;		/** Number of workers running tasks. */
	int tasks_running;		/** Number of tasks currently running. */
	int tasks_waiting;		/** Number of tasks waiting for a CPU. */
	int tasks_complete;		/** Number of tasks waiting to be returned to user. */
	int total_tasks_dispatched;	/** Total number of tasks dispatch to workers. */
	int total_tasks_complete;	/** Total number of tasks returned complete. */
	int total_workers_joined;	/** Total number of times a worker joined the queue. */
	int total_workers_removed;	/** Total number of times a worker was removed from the queue. */
};

/** Create a new work queue.
@param port The port number to listen on, or zero to choose a default.
@return A new work queue, or zero if it could not be created.
*/
struct work_queue * work_queue_create( int port );

/** Delete a work queue.
@param q The work queue to delete.
*/
void work_queue_delete( struct work_queue *q );

/** Submit a job to a work queue.
@param q A work queue returned from @ref work_queue_create.
@param t A task description returned from @ref work_queue_task_create.
*/
void work_queue_submit( struct work_queue *q, struct work_queue_task *t );

/** Wait for tasks to complete.  This call will block indefinitely until a task completes.
@param q The work queue to wait on.
@returns A completed task description, or null if the queue is empty.
*/
struct work_queue_task * work_queue_wait( struct work_queue *q );

/** Wait for tasks to complete.  This call will block indefinitely until a task completes.
@param q The work queue to wait on.
@returns A completed task description, or null if the queue is empty.
*/
struct work_queue_task * work_queue_wait_time( struct work_queue *q , time_t stoptime);

/** Create a new task specification.  Once created, the task may be passed to @ref work_queue_submit.
@param tag A free-form description of the task, for the caller's record keeping.
@param command The command line to be executed.
@param input_files The files that must be transferred to the remote host before execution, including the executable.  If the files should have a different name locally than remotely, specify this by stating localname=remotename.
@param output_files The files that must be returned upon completion, separated by commas.If the files should have a different name locally than remotely, specify this by stating localname=remotename.
@param priority An integer describing the relative priority of the job, where higher numbers will be executed sooner.
*/
struct work_queue_task * work_queue_task_create( const char *tag, const char *command, const struct task_file *input_files, const int num_inputs, const char *output_files, int priority );

/** Delete a task specification.  This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task specification to delete.
*/
void work_queue_task_delete( struct work_queue_task *t );

/** Get queue statistics.
@param q The queue to query.
@param s A pointer to a buffer that will be filed with statistics.
*/
void work_queue_get_stats( struct work_queue *q, struct work_queue_stats *s );

#endif
