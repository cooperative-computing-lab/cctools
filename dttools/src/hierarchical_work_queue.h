/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HIERARCHICAL_WORK_QUEUE_H
#define HIERARCHICAL_WORK_QUEUE_H

/** @file work_queue.h A master-worker library.
 The work queue provides an implementation of the master-worker computing
model using TCP sockets, Unix applications, and files as intermediate buffers.
A master process uses @ref work_queue_create to create a queue, then
@ref work_queue_submit to submit tasks.  Once tasks are running, call
@ref work_queue_wait to wait for completion.  The generic <tt>worker</tt>
program can be run on any machine, and simply needs to be told the host
and port of the master.
*/

#include "file_cache.h"
#include "itable.h"
#include "timestamp.h"
#include "worker_comm.h"

#include <stdio.h>

#define WORK_QUEUE_DEFAULT_PORT 9123
#define WORK_QUEUE_LINE_MAX 1024

#define WORK_QUEUE_WAITFORTASK -1

#define WORK_QUEUE_RETURN_STATUS_UNSET -1

#define WORK_QUEUE_RESULT_UNSET 0
#define WORK_QUEUE_RESULT_INPUT_FAIL 1
#define WORK_QUEUE_RESULT_INPUT_MISSING 2
#define WORK_QUEUE_RESULT_FUNCTION_FAIL 4
#define WORK_QUEUE_RESULT_OUTPUT_FAIL 8
#define WORK_QUEUE_RESULT_OUTPUT_MISSING 16
#define WORK_QUEUE_RESULT_LINK_FAIL 32


/*
enum worker_op_type {
	WORKER_OP_ROLE,
	WORKER_OP_WORKDIR,
	WORKER_OP_CLEAR_CACHE,
	WORKER_OP_COMM_INTERFACE,
	WORKER_OP_RESULTS,

	WORKER_OP_FILE,
	WORKER_OP_FILE_CHECK,
	WORKER_OP_FILE_PUT,
	WORKER_OP_FILE_GET,

	WORKER_OP_JOB_DIRMAP,
	WORKER_OP_JOB_REQUIRES,
	WORKER_OP_JOB_GENERATES,
	WORKER_OP_JOB_CMD,
	WORKER_OP_JOB_CLOSE,
	WORKER_OP_JOB_CANCEL
};
*/

#define WORKER_OP_ROLE			 1
#define WORKER_OP_WORKDIR		 2
#define WORKER_OP_CLEAR_CACHE		 3
#define WORKER_OP_COMM_INTERFACE	 4
#define WORKER_OP_RESULTS		 5

#define WORKER_OP_FILE			 6
#define WORKER_OP_FILE_CHECK		 7
#define WORKER_OP_FILE_PUT		 8
#define WORKER_OP_FILE_GET		 9

#define WORKER_OP_JOB_DIRMAP		10
#define WORKER_OP_JOB_REQUIRES		11
#define WORKER_OP_JOB_GENERATES	12
#define WORKER_OP_JOB_CMD		13
#define WORKER_OP_JOB_CLOSE		14
#define WORKER_OP_JOB_CANCEL		15

#define WORKER_ROLE_WORKER			0x01
#define WORKER_ROLE_FOREMAN			0x02

#define WORKER_FILES_INPUT			0x01
#define WORKER_FILES_OUTPUT			0x02

#define WORKER_FILE_INCOMPLETE			0x01
#define WORKER_FILE_NORMAL			0x02
#define WORKER_FILE_REMOTE			0x03

#define WORKER_FILE_FLAG_NOCLOBBER		0x01
#define WORKER_FILE_FLAG_REMOTEFS		0x02
#define WORKER_FILE_FLAG_CACHEABLE		0x04
#define WORKER_FILE_FLAG_MISSING		0x08
#define WORKER_FILE_FLAG_OPTIONAL		0x10
#define WORKER_FILE_FLAG_IGNORE		0x20

#define WORKER_JOB_STATUS_READY		0x01
#define WORKER_JOB_STATUS_MISSING_FILE		0x02
#define WORKER_JOB_STATUS_FAILED_SYMLINK	0x03
#define WORKER_JOB_STATUS_COMPLETE		0x04

#define WORKER_JOB_OUTPUT_STDOUT		0x01
#define WORKER_JOB_OUTPUT_STDERR		0x02
#define WORKER_JOB_OUTPUT_COMBINED		0x03


#define WORKER_STATE_AVAILABLE			0x01
#define WORKER_STATE_BUSY			0x02
#define WORKER_STATE_UNRESPONSNIVE		0x03



struct worker_file {
	int id;
	int type;
	char *filename;
	int flags;
	char *payload;
	int size;
	char label[WORK_QUEUE_LINE_MAX];
};


struct worker_job {
	int id;
	char *command;
	int commandlength;
	char *dirmap;
	int dirmaplength;
	char *tag;
	int options;

	int status;
	int exit_code;
	int result;


	int output_streams;
	char *stdout_buffer;
	int stdout_buffersize;
	char *stderr_buffer;
	int stderr_buffersize;

	struct list *input_files;
	struct list *output_files;
	
	pid_t pid;
	FILE *out;
	int out_fd;
	FILE *err;
	int err_fd;
	
	timestamp_t submit_time;
	timestamp_t start_time;
	timestamp_t finish_time;
};

struct worker {
	int workerid;
	char hostname[WORK_QUEUE_LINE_MAX];
	int cores;
	int open_cores;
	UINT64_T ram;
	UINT64_T disk;
	
	int state;
	int role;
	struct itable *jobids;
	struct worker_comm *comm;
};

/** @name Functions - Tasks */

//@{

/** Create a new task specification.  Once created, the task may be passed to @ref work_queue_submit.
@param full_command The shell command line to be executed by the task.
*/
struct worker_job *hierarchical_work_queue_job_create(const char *full_command);

/** Add a file to a task.
@param t The task to which to add a file.
@param local_name The name of the file on local disk or shared filesystem.
@param remote_name The name of the file at the execution site.
@param type Must be one of the following values:
- WORK_QUEUE_INPUT to indicate an input file to be consumed by the task
- WORK_QUEUE_OUTPUT to indicate an output file to be produced by the task
@param flags May be zero to indicate no special handling, or any of the following or'd together:
- WORK_QUEUE_CACHEABLE - The file may be cached at the execution site for later use.
- WORK_QUEUE_SYMLINK - Create a symlink to the file rather than copying it, if possible.
- WORK_QUEUE_THIRDGET - Access the file on the client from a shared filesystem.
- WORK_QUEUE_THIRDPUT - Access the file on the client from a shared filesystem (included for readability).
*/
void hierarchical_work_queue_job_specify_file(struct worker_job *j, const char *local_name, const char *remote_name, int type, int flags);

/** Add an input buffer to a task.
@param t The task to which to add a file.
@param data The contents of the buffer to pass as input.
@param length The length of the buffer, in bytes
@param remote_name The name of the remote file to create.
@param flags May take the same values as in @ref work_queue_task_specify_file.
*/
void hierarchical_work_queue_job_specify_buffer(struct worker_job *j, const char *data, int length, const char *remote_name, int flags);

/** Attach a user defined logical name to the task.
This field is not interpreted by the work queue, but simply maintained to help the user track tasks.
@param t The task to which to add parameters
@param tag The tag to attach to task t.
*/
void hierarchical_work_queue_job_specify_tag(struct worker_job *j, const char *tag);

/** Delete a task specification.  This may be called on tasks after they are returned from @ref work_queue_wait.
@param t The task specification to delete.
*/
void hierarchical_work_queue_job_delete(struct worker_job *j);

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
struct hierarchical_work_queue *hierarchical_work_queue_create(int mode, int port, const char *file_cache_path, int timeout);

/** Submit a job to a work queue.
It is safe to re-submit a task returned by @ref work_queue_wait.
@param q A work queue returned from @ref work_queue_create.
@param t A task description returned from @ref work_queue_task_create.
*/
void hierarchical_work_queue_submit(struct hierarchical_work_queue *q, struct worker_job *j);

/** Wait for tasks to complete.  This call will block until the timeout has elapsed.
@param q The work queue to wait on.
@param timeout The number of seconds to wait for a completed task before returning.  Use an integer time to set the timeout or the constant WAITFORTASK to block until a task has completed.
@returns A completed task description, or null if the queue is empty or the timeout was reached without a completed task.  The returned task must be deleted with @ref work_queue_task_delete or resubmitted with @ref work_queue_submit.
*/
struct worker_job *hierarchical_work_queue_wait(struct hierarchical_work_queue *q);

/** Determine whether there are any known tasks queued, running, or waiting to be collected. Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
@param q A pointer to the queue to query.
*/
int hierarchical_work_queue_empty(struct hierarchical_work_queue *q);


/** Change the project name for a given queue.
@param q A pointer to the queue to modify.
@param name The new project name..
*/
int hierarchical_work_queue_specify_name(struct hierarchical_work_queue *q, const char *name);

/** Specify the master mode for a given queue. 
@param q A pointer to the queue to modify.
@param mode 
<b>mode == 0</b>: standalone mode. In this mode the master would not report its information to a catalog server; 
<b>mode == 1</b>: catalog mode. In this mode the master report itself to a catalog server where workers get masters' information and select a master to serve.
@return The mode that has been set.
*/
void hierarchical_work_queue_specify_interface(struct hierarchical_work_queue *q, int mode, int port);

/** Shut down workers connected to the work_queue system. Gives a best effort and then returns the number of workers given the shut down order.
@param q A pointer to the queue to query.
@param n The number to shut down. All workers if given "0".
*/
int hierarchical_work_queue_shut_down_workers(struct hierarchical_work_queue *q, int n);

/** Delete a work queue.
@param q The work queue to delete.
*/
void hierarchical_work_queue_delete(struct hierarchical_work_queue *q);

//@}



void worker_job_check_files(struct worker_job *job, struct file_cache *file_store, int filetype);
void worker_job_send_result(struct worker_comm *comm, struct worker_job *job);
struct worker_job * worker_job_receive_result(struct worker_comm *comm, struct itable *jobs);
int worker_job_send_files(struct worker_comm *comm, struct list *input_files, struct list *output_files, struct file_cache *file_store);
int worker_job_fetch_files(struct worker_comm *comm, struct list *files, struct file_cache *file_store);


#endif
