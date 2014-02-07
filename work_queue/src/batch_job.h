/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_JOB_H
#define BATCH_JOB_H

#include <sys/stat.h>

#include <inttypes.h>
#include <stdint.h>
#include <time.h>

/** @file batch_job.h Batch job submission.
This module implements batch job submission to multiple systems,
including Condor, SGE, Work Queue, Xgrid, and local Unix processes.
This simplifies the construction
of parallel abstractions that need a simple form of parallel process execution.
*/

/** An integer type indicating a unique batch job number.*/
typedef int64_t batch_job_id_t;
#define PRIbjid  PRId64
#define SCNbjid  SCNd64

/** Indicates which type of batch submission to use. */
/* Must be kept in sync with batch_job_subsystems. */
typedef enum {
	BATCH_QUEUE_TYPE_LOCAL,	              /**< Batch jobs will run as local processes. */
	BATCH_QUEUE_TYPE_CONDOR,              /**< Batch jobs will be sent to Condor pool. */
	BATCH_QUEUE_TYPE_SGE,	              /**< Batch jobs will be sent to Sun Grid Engine. */
	BATCH_QUEUE_TYPE_MOAB,                /**< Batch jobs will be sent to the Moab Workload Manager. */
	BATCH_QUEUE_TYPE_TORQUE,              /**< Batch jobs will be send to the Torque Scheduler. */
	BATCH_QUEUE_TYPE_CLUSTER,             /**< Batch jobs will be sent to a user-defined cluster manager. */
	BATCH_QUEUE_TYPE_WORK_QUEUE,          /**< Batch jobs will be sent to the Work Queue. */
	BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS, /**< Batch jobs will be sent to the Work Queue, all files are stored on a shared filesystem. */
	BATCH_QUEUE_TYPE_XGRID,	              /**< Batch jobs will be sent to the Xgrid. */
	BATCH_QUEUE_TYPE_HADOOP,              /**< Batch jobs will be sent to Hadoop. */
	BATCH_QUEUE_TYPE_MPI_QUEUE,           /**< Batch jobs will be sent to the MPI Queue. */
	BATCH_QUEUE_TYPE_CHIRP,               /**< Batch jobs will be sent to Chirp. */
	BATCH_QUEUE_TYPE_UNKNOWN = -1         /**< An invalid batch queue type. */
} batch_queue_type_t;

/** Describes a batch job when it has completed. */
struct batch_job_info {
	time_t submitted;    /**< Time the job was submitted to the system. */
	time_t started;      /**< Time the job actually began executing. */
	time_t finished;     /**< Time at which the job actually completed. */
	int exited_normally; /**< Non-zero if the job ran to completion, zero otherwise. */
	int exit_code;       /**< The result code of the job, if it exited normally. */
	int exit_signal;     /**< The signal by which the job was killed, if it exited abnormally. */
};

/** Create a new batch queue.
@param type The type of the queue.
@return A new batch queue object on success, null on failure.
*/
struct batch_queue *batch_queue_create(batch_queue_type_t type);

/** Submit a simple batch job.
@param q The queue to submit to.
@param cmdline The command line to execute.  This line will be interpreted by the shell, so it may include output redirection, multiple commands, pipes, and so forth.
@param input_files A comma separated list of all input files that will be required by the job.  Null pointer is equivalent to empty string.  This must also include the executable and any dependent programs.
@param output_files A comma separated list of all output files to retrieve from the job.  Null pointer is equivalent to empty string.
@return On success, returns a unique identifier for the batch job.  On failure, returns a negative number.
*/
batch_job_id_t batch_job_submit_simple(struct batch_queue *q, const char *cmdline, const char *input_files, const char *output_files);

/** Submit a batch job.
@param q The queue to submit to.
@param cmd The command to execute.
@param args The command line arguments.
@param infile The standard input file.
@param outfile The standard output file.
@param errfile The standard error file.
@param extra_input_files A comma separated list of extra input files that will be required by the job.  Null pointer is equivalent to empty string.
@param extra_output_files A comma separated list of extra output files to retrieve from the job.  Null pointer is equivalent to empty string.
@return On success, returns a unique positive jobid.  Zero or a negative number indicates a failure to submit this job.
*/
batch_job_id_t batch_job_submit(struct batch_queue *q, const char *cmd, const char *args, const char *infile, const char *outfile, const char *errfile, const char *extra_input_files, const char *extra_output_files);

/** Wait for any batch job to complete.
Blocks until a batch job completes.
@param q The queue to wait on.
@param info Pointer to a @ref batch_job_info structure that will be filled in with the details of the completed job.
@return If greater than zero, indicates the jobid of the completed job.
If equal to zero, there were no more jobs to wait for.
If less than zero, the operation was interrupted by a system event, but may be tried again.
*/
batch_job_id_t batch_job_wait(struct batch_queue *q, struct batch_job_info *info);

/** Wait for any batch job to complete, with a timeout.
Blocks until a batch job completes or the current time exceeds stoptime.
@param q The queue to wait on.
@param info Pointer to a @ref batch_job_info structure that will be filled in with the details of the completed job.
@param stoptime An absolute time at which to stop waiting.  If less than or equal to the current time,
then this function will check for a complete job but will not block.
@return If greater than zero, indicates the jobid of the completed job.
If equal to zero, there were no more jobs to wait for.
If less than zero, the operation timed out or was interrupted by a system event, but may be tried again.
*/
batch_job_id_t batch_job_wait_timeout(struct batch_queue *q, struct batch_job_info *info, time_t stoptime);

/** Remove a batch job.
This call will start the removal process.
You must still call @ref batch_job_wait to wait for the removal to complete.
@param q The queue to remove from.
@param jobid The job to be removed.
@return Greater than zero if the job exists and was removed, zero otherwise.
*/
int batch_job_remove(struct batch_queue *q, batch_job_id_t jobid);

int batch_fs_chdir (struct batch_queue *q, const char *path);
int batch_fs_getcwd (struct batch_queue *q, char *buf, size_t size);
int batch_fs_mkdir (struct batch_queue *q, const char *path, mode_t mode, int recursive);
int batch_fs_putfile (struct batch_queue *q, const char *lpath, const char *rpath);
int batch_fs_stat (struct batch_queue *q, const char *path, struct stat *buf);
int batch_fs_unlink (struct batch_queue *q, const char *path);

/** Converts a string into a batch queue type.
@param str A string listing all of the known batch queue types (which changes over time.)
@return The batch queue type corresponding to the string, or BATCH_QUEUE_TYPE_UNKNOWN if the string is invalid.
*/
batch_queue_type_t batch_queue_type_from_string(const char *str);

/** Converts a batch queue type to a string.
@param t A @ref batch_queue_type_t.
@return A string corresponding to the batch queue type.
*/
const char *batch_queue_type_to_string(batch_queue_type_t t);

/** Set the log file used by the batch queue.
This is an optional call that will only affect batch queue types
that use an internal logfile; currently only Condor.
@param q The batch queue to adjust.
@param logfile Name of the logfile to use.
*/
void batch_queue_set_logfile(struct batch_queue *q, const char *logfile);

/** Add extra options to pass to the underlying batch system.
This call specifies additional options to be passed to the batch system each
time a job is submitted.  It may be called once to apply to all subsequent
jobs, or it may be called before each submission.  If the queue type
is @ref BATCH_QUEUE_TYPE_CONDOR, the options must be valid submit file
properties like <tt>requirements = (Memory>100)</tt>.
If the batch queue type is @ref BATCH_QUEUE_TYPE_SGE, the extra text will be added as options to
the <tt>qsub</tt> command.  This call has no effect on other queue types.
@param q The batch queue to adjust.
@param what The key for option.
@param value The value of the option.
*/
void batch_queue_set_option(struct batch_queue *q, const char *what, const char *value);

/** Get batch queue options.
This call returns the additional options to be passed to the batch system each
time a job is submitted.
@param q The batch queue.
@param what The option key.
@return The option value.
*/
const char *batch_queue_get_option(struct batch_queue *q, const char *what);

/** Get batch queue type.
This call returns the type of the batch queue.
@param q The batch queue.
@return The type of the batch queue, defined when it was created.
*/
batch_queue_type_t batch_queue_get_type(struct batch_queue *q);

/** Delete a batch queue.
Note that this function just destroys the internal data structures,
it does not abort running jobs.  To properly clean up running jobs,
you must call @ref batch_job_wait until it returns zero, or
call @ref batch_job_remove on all runnings jobs.
@param q The queue to delete.
*/
void batch_queue_delete(struct batch_queue *q);

/** Returns the list of queue types supported by this module.
Useful for including in help-option outputs.
@return A static string listing the types of queues supported.
*/
const char *batch_queue_type_string();

/** Returns the port number of the batch queue.
@param q The batch queue of interest.
@return The port number in use, or zero if not applicable.
*/
int batch_queue_port(struct batch_queue *q);

#endif
