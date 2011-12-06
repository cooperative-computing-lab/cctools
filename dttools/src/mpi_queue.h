/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MPI_QUEUE_H
#define MPI_QUEUE_H

/** @file mpi_queue.h An MPI implementation of Work Queue. */

#include "timestamp.h"


#define MPI_QUEUE_LINE_MAX 256
#define MPI_QUEUE_DEFAULT_PORT 9123

#define MPI_QUEUE_WAITFORTASK -1

#define MPI_QUEUE_RETURN_STATUS_UNSET -1
#define MPI_QUEUE_RESULT_UNSET -1

#define MPI_QUEUE_INPUT  0
#define MPI_QUEUE_OUTPUT 1

/** A task description.  This structure should only be created with @ref mpi_queue_task_create and delete with @ref mpi_queue_task_delete.  You may examine (but not modify) this structure once a task has completed.
*/
struct mpi_queue_task {
	char *tag;			/**< An optional user-defined logical name for the task. */
	char *command_line;		/**< The program(s) to execute, as a shell command line. */
	char *output;			/**< The standard output of the task. */
	struct list *input_files;	/**< The files to transfer to the worker and place in the executing directory. */
	struct list *output_files;	/**< The output files (other than the standard output stream) created by the program expected to be retrieved from the task. */
	int taskid;			/**< A unique task id number. */
	int status;			/**< Current status of the task. */
	int return_status;		/**< The exit code of the command line. */
	int result;			/**< The result of the task (successful, failed return_status, missing input file, missing output file). */

	timestamp_t submit_time;		/**< The time the task was submitted. */
	timestamp_t start_time;		/**< The time at which the task began. */
	timestamp_t finish_time;		/**< The time at which it completed. */
	timestamp_t transfer_start_time;	/**< The time at which it started to transfer input files. */
	timestamp_t computation_time;	/**< The time of executing the command. */

	INT64_T total_bytes_transferred;	/**< Number of bytes transferred since task has last started transferring input data. */
	timestamp_t total_transfer_time;	/**< Time comsumed in microseconds for transferring total_bytes_transferred. */
};


/** Create a new task specification.  Once created, the task may be passed to @ref mpi_queue_submit.
@param full_command The shell command line to be executed by the task.
@return A new @ref mpi_queue_task structure.
*/
struct mpi_queue_task *mpi_queue_task_create(const char *full_command);

/** Add a file to a task.
@param t The task to which to add a file.
@param name The name of the file on local disk or shared filesystem.
@param type Must be one of the following values:
- MPI_QUEUE_INPUT to indicate an input file to be consumed by the task
- MPI_QUEUE_OUTPUT to indicate an output file to be produced by the task
*/
void mpi_queue_task_specify_file(struct mpi_queue_task *t, const char *name, int type);


/** Attach a user defined logical name to the task.
This field is not interpreted by the work queue, but simply maintained to help the user track tasks.
@param t The task to which to add parameters
@param tag The tag to attach to task t.
*/
void mpi_queue_task_specify_tag(struct mpi_queue_task *t, const char *tag);


/** Delete a task specification.  This may be called on tasks after they are returned from @ref mpi_queue_wait.
@param t The task specification to delete.
*/
void mpi_queue_task_delete(struct mpi_queue_task *t);


/** Create a new work queue.
Users may modify the behavior of @ref mpi_queue_create by setting the following environmental variables before calling the function:

- <b>MPI_QUEUE_PORT</b>: This sets the default port of the queue (if unset, the default is 9123).
- <b>MPI_QUEUE_LOW_PORT</b>: If the user requests a random port, then this sets the first port number in the scan range (if unset, the default is 1024).
- <b>MPI_QUEUE_HIGH_PORT</b>: If the user requests a random port, then this sets the last port number in the scan range (if unset, the default is 32767).
- <b>MPI_QUEUE_NAME</b>: This sets the project name of the queue, which is reported to a catalog server (by default this is unset).  
- <b>MPI_QUEUE_PRIORITY</b>: This sets the priority of the queue, which is used by workers to sort masters such that higher priority masters will be served first (if unset, the default is 10).

If the queue has a project name, then queue statistics and information will be
reported to a catalog server.  To specify the catalog server, the user may set
the <b>CATALOG_HOST</b> and <b>CATALOG_PORT</b> environmental variables as described in @ref catalog_query_create.

@param port The port number to listen on.  If zero is specified, then the default is chosen, and if -1 is specified, a random port is chosen.  
@return A new work queue, or null if it could not be created.
*/
struct mpi_queue *mpi_queue_create(int port);

/** Submit a job to a work queue.
It is safe to re-submit a task returned by @ref mpi_queue_wait.
@param q A work queue returned from @ref mpi_queue_create.
@param t A task description returned from @ref mpi_queue_task_create.
*/
void mpi_queue_submit(struct mpi_queue *q, struct mpi_queue_task *t);

/** Wait for tasks to complete.  This call will block until the timeout has elapsed.
@param q The work queue to wait on.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty or the timeout was reached without a completed task.  The returned task must be deleted with @ref mpi_queue_task_delete or resubmitted with @ref mpi_queue_submit.
*/
struct mpi_queue_task *mpi_queue_wait(struct mpi_queue *q, int timeout);

/** Determine whether there are any known tasks queued, running, or waiting to be collected. Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
@param q A pointer to the queue to query.
*/
int mpi_queue_empty(struct mpi_queue *q);

/** Get the listening port of the queue.
@param q The work queue of interest.
@return The port the queue is listening on.
*/
int mpi_queue_port(struct mpi_queue *q);

/** Delete a work queue.
@param q The work queue to delete.
*/
void mpi_queue_delete(struct mpi_queue *q);

#endif

