/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_JOB_H
#define BATCH_JOB_H

#include <sys/stat.h>

#include <inttypes.h>
#include <stdint.h>
#include <time.h>

struct batch_queue;
struct batch_task;
struct batch_file;

#include "batch_task.h"
#include "batch_file.h"
#include "batch_job_info.h"

#include "jx.h"
#include "rmsummary.h"

/** @file batch_queue.h Batch job submission.
This module implements batch job submission to multiple systems,
including local processes, HTCondor, TaskVine, Work Queue, SGE, PBS, Amazon EC2, and others.
This simplifies the construction
of parallel abstractions that need a simple form of parallel process execution.
*/

/** An integer type indicating a unique batch job number.*/
typedef int64_t batch_queue_id_t;
#define PRIbjid  PRId64
#define SCNbjid  SCNd64

/** Indicates which type of batch submission to use. */
/* Must be kept in sync with batch_queue_subsystems. */
typedef enum {
	BATCH_QUEUE_TYPE_LOCAL,	              /**< Batch jobs will run as local processes. */
	BATCH_QUEUE_TYPE_CONDOR,              /**< Batch jobs will be sent to Condor pool. */
	BATCH_QUEUE_TYPE_AMAZON,              /**< Batch jobs will be sent spun up Amazon ec2 instances */
	BATCH_QUEUE_TYPE_LAMBDA,              /**< Batch jobs will be executed by an Amazon Lambda function with S3 objects */
        BATCH_QUEUE_TYPE_AMAZON_BATCH,        /**< Batch jobs will be sent to Amazon Batch System */
	BATCH_QUEUE_TYPE_SGE,	              /**< Batch jobs will be sent to Sun Grid Engine. */
	BATCH_QUEUE_TYPE_MOAB,                /**< Batch jobs will be sent to the Moab Workload Manager. */
	BATCH_QUEUE_TYPE_PBS,                 /**< Batch jobs will be send to the PBS Scheduler. */
	BATCH_QUEUE_TYPE_LSF,		      /**< Batch jobs will be sent to LSF. */
	BATCH_QUEUE_TYPE_TORQUE,              /**< Batch jobs will be send to the Torque Scheduler. */
	BATCH_QUEUE_TYPE_SLURM,               /**< Batch jobs will be send to the SLURM Scheduler. */
	BATCH_QUEUE_TYPE_CLUSTER,             /**< Batch jobs will be sent to a user-defined cluster manager. */
	BATCH_QUEUE_TYPE_WORK_QUEUE,          /**< Batch jobs will be sent to the Work Queue. */
	BATCH_QUEUE_TYPE_CHIRP,               /**< Batch jobs will be sent to Chirp. */
	BATCH_QUEUE_TYPE_MESOS,               /**< Batch jobs will be sent to Mesos. */
	BATCH_QUEUE_TYPE_K8S,                 /**< Batch jobs will be sent to kubernetes. */
	BATCH_QUEUE_TYPE_DRYRUN,              /**< Batch jobs will not actually run. */
        BATCH_QUEUE_TYPE_MPI,                 /**< Batch jobs distributed within an MPI program. */
	BATCH_QUEUE_TYPE_VINE,                /**< Batch jobs executed via TaskVine. */
	BATCH_QUEUE_TYPE_UNKNOWN = -1         /**< An invalid batch queue type. */
} batch_queue_type_t;

/** Create a new batch queue.
@param type The type of the queue.
@param ssl_key_file The location of the queue manager's ssl key file, if it has one.
@param ssl_key_file The location of the queue manager's ssl certiciate file, if it has one.
@return A new batch queue object on success, null on failure.
*/
struct batch_queue *batch_queue_create(batch_queue_type_t type, const char *ssl_key_file, const char *ssl_cert_file );

/** Submit a batch job.
@param q The queue to submit to.
@param task The job description to submit.
@param resources The computational resources needed by the job.
@return On success, returns a positive unique identifier for the batch job.  On failure, returns a negative number.
Zero is not a valid batch job id and indicates an internal failure.
*/
batch_queue_id_t batch_queue_submit(struct batch_queue *q, struct batch_task *task );

/** Wait for any batch job to complete.
Blocks until a batch job completes.
 * Note Submit may return 0 as a valid jobid. As of 04/18 wait will not return 0 as a valid jobid. 
 *  Wait returning 0 indicates there are no waiting jobs in this queue.
@param q The queue to wait on.
@param info Pointer to a @ref batch_job_info structure that will be filled in with the details of the completed job.
@return If greater than zero, indicates the jobid of the completed job.
If equal to zero, there were no more jobs to wait for.
If less than zero, the operation was interrupted by a system event, but may be tried again.
*/
batch_queue_id_t batch_queue_wait(struct batch_queue *q, struct batch_job_info *info);

/** Wait for any batch job to complete, with a timeout.
Blocks until a batch job completes or the current time exceeds stoptime.
 * Note Submit may return 0 as a valid jobid. As of 04/18 wait will not return 0 as a valid jobid. 
 *  Wait returning 0 indicates there are no waiting jobs in this queue.
@param q The queue to wait on.
@param info Pointer to a @ref batch_job_info structure that will be filled in with the details of the completed job.
@param stoptime An absolute time at which to stop waiting.  If less than or equal to the current time,
then this function will check for a complete job but will not block.
@return If greater than zero, indicates the jobid of the completed job.
If equal to zero, there were no more jobs to wait for.
If less than zero, the operation timed out or was interrupted by a system event, but may be tried again.
*/
batch_queue_id_t batch_queue_wait_timeout(struct batch_queue *q, struct batch_job_info *info, time_t stoptime);

/** Remove a batch job.
This call will start the removal process.
You must still call @ref batch_queue_wait to wait for the removal to complete.
@param q The queue to remove from.
@param jobid The job to be removed.
@return Greater than zero if the job exists and was removed, zero otherwise.
*/
int batch_queue_remove(struct batch_queue *q, batch_queue_id_t jobid);

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

/** Expresses support for feature in the underlying batch system.
This call specifies features that are supported by this batch system for
use in exterior systems. Used within batch_queue_* for the specific batch
system.
@param q The batch queue to adjust.
@param what The key for feature.
@param value The value of the feature.
*/
void batch_queue_set_feature(struct batch_queue *q, const char *what, const char *value);

/** As @ref batch_queue_set_option, but allowing an integer argument.
@param q The batch queue to adjust.
@param what The key for option.
@param value The value of the option.
*/
void batch_queue_set_int_option(struct batch_queue *q, const char *what, int value);

/** Get batch queue options.
This call returns the additional options to be passed to the batch system each
time a job is submitted.
@param q The batch queue.
@param what The option key.
@return The option value.
*/
const char *batch_queue_get_option(struct batch_queue *q, const char *what);

/** Check if option is set to yes
@param q The batch queue.
@param what The option key.
@return 1 if option is yes, 0 if unset or not set to yes.
*/
int batch_queue_option_is_yes (struct batch_queue *q, const char *what);

/** Get batch queue feature.
This call returns a valid const char if the feaute specified is
supported by the given queue type.
@param q The batch queue.
@param what The option key.
@return The option value.
*/
const char *batch_queue_supports_feature (struct batch_queue *q, const char *what);


/** Get batch queue type.
This call returns the type of the batch queue.
@param q The batch queue.
@return The type of the batch queue, defined when it was created.
*/
batch_queue_type_t batch_queue_get_type(struct batch_queue *q);

/** Delete a batch queue.
Note that this function just destroys the internal data structures,
it does not abort running jobs.  To properly clean up running jobs,
you must call @ref batch_queue_wait until it returns zero, or
call @ref batch_queue_remove on all runnings jobs.
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

/* Hack: provide a backdoor to allow the MPI module to perform
   some initial setup before the MPI batch queue is created.
*/
void batch_queue_mpi_setup( const char *debug_filename, int mpi_cores, int mpi_memory );

#endif
