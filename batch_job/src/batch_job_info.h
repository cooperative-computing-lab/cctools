#ifndef BATCH_JOB_INFO_H
#define BATCH_JOB_INFO_H

#include <time.h>

/** Describes a batch job when it has completed. */
struct batch_job_info {
	time_t submitted;    /**< Time the job was submitted to the system. */
	time_t started;      /**< Time the job actually began executing. */
	time_t finished;     /**< Time at which the job actually completed. */
	time_t heartbeat;    /**< Time the job last wrote heartbeat. (only for batch_queue_cluster) */
	int exited_normally; /**< Non-zero if the job ran to completion, zero otherwise. */
	int exit_code;       /**< The result code of the job, if it exited normally. */
	int exit_signal;     /**< The signal by which the job was killed, if it exited abnormally. */
	int disk_allocation_exhausted; /**< Non-zero if the job filled its loop device allocation to capacity, zero otherwise */
	long log_pos;        /**< Last read position in the log file, for ftell and fseek. (only for batch_queue_cluster) */
};

/** Create a new batch_job_info struct.
@return A new empty batch_job_info struct.
*/
struct batch_job_info *batch_job_info_create();

/** Delete a batch_job_info struct.
@param info The batch_job_info struct to be deleted.
*/
void batch_job_info_delete(struct batch_job_info *info);

#endif
