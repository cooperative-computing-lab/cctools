/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef BATCH_JOB_H
#define BATCH_JOB_H

#include <time.h>

/** @file batch_job.h Batch job submission.
This module implements batch job submission to multiple systems,
currently Condor and plain Unix processes.  The simplifies the construction
of parallel abstractions that need a simple form of parallel process execution.
*/

/** An integer type indicating a unique batch job number.*/
typedef int batch_job_id_t;

/** Indicates which type of batch submission to use. */
typedef enum {
	BATCH_QUEUE_TYPE_UNIX,	/**< Batch jobs will run as local Unix processes. */
	BATCH_QUEUE_TYPE_CONDOR,	/**< Batch jobs will be sent to Condor pool. */
	BATCH_QUEUE_TYPE_SGE,	/**< Batch jobs will be sent to Sun Grid Engine. */
	BATCH_QUEUE_TYPE_WORK_QUEUE  /**< Batch jobs will be send to the Work Queue. */
} batch_queue_type_t;

/** Create a new batch queue.
@param type The type of the queue.
@return A new batch queue object.
*/
struct batch_queue * batch_queue_create( batch_queue_type_t type );

/** Delete a batch queue.
@param q The batch queue to delete.
*/
void batch_queue_delete( struct batch_queue *q );

/** Describes a batch job when it has completed. */
struct batch_job_info {
	time_t submitted;	/**< Time the job was submitted to the system. */
	time_t started;		/**< Time the job actually began executing. */
	time_t finished;	/**< Time at which the job actually completed. */
	int exited_normally;	/**< Non-zero if the job ran to completion, zero otherwise. */
	int exit_code;		/**< The result code of the job, if it exited normally. */
	int exit_signal;	/**< The signal by which the job was killed, if it exited abnormally. */
};

/** Submit a batch job..
@param q The queue to submit to.
@param cmd The command to execute.
@param args The command line arguments.
@param infile The standard input file.
@param outfile The standard output file.
@param errfile The standard error file.
@param extra_input_files A comma separated list of extra input files that will be required by the job.  Null pointer is equivalent to empty string.
@param extra_output_files A comma separated list of extra output files to retrieve from the job.  Null pointer is equivalent to empty string.
@return A unique identifier for the batch job.
*/

batch_job_id_t batch_job_submit(
	struct batch_queue *q,
	const char *cmd,
	const char *args,
	const char *infile,
	const char *outfile,
	const char *errfile,
	const char *extra_input_files,
	const char *extra_output_files );

/** Wait for any batch job to complete.
Blocks until a batch job completes.
@param q The queue to wait on.
@param info Pointer to a @ref batch_job_info structure that will be filled in with the details of the completed job.
@return If greater than zero, indicates the job id number of the completed job.  If equal to zero, there were no more jobs to wait for.  If less than zero, an error occurred while waiting, but the operation may be tried again.
*/

batch_job_id_t batch_job_wait( struct batch_queue *q, struct batch_job_info *info );

/** Remove a batch job.
This call will start the removal process.
You must still call @ref batch_job_wait to wait for the removal to complete.
@param q The queue to remove from.
@param jobid The job to be removed.
@return Greater than zero if the job exists and was removed, zero otherwise.
*/

int batch_job_remove( struct batch_queue *q, batch_job_id_t jobid );

#endif
