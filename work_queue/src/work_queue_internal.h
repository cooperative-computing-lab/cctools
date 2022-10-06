/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "work_queue_resources.h"

#include "list.h"
#include "hash_table.h"

struct work_queue_file {
	work_queue_file_t type;
	int flags;		// WORK_QUEUE_CACHE or others in the future.
	int length;		// length of payload, only used for non-file objects like buffers and urls
	off_t offset;		// file offset for WORK_QUEUE_FILE_PIECE
	off_t piece_length;	// file piece length for WORK_QUEUE_FILE_PIECE
	char *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
	char *cached_name;	// name on remote machine in cached directory.
};

struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct link *foreman_uplink, int *foreman_uplink_active, const char *tag);

/* Adds (arithmetically) all the workers resources (cores, memory, disk) */
void aggregate_workers_resources( struct work_queue *q, struct work_queue_resources *r, struct hash_table *categories );

/** Enable use of the process module.
This allows @ref work_queue_wait to call @ref process_pending from @ref process.h, exiting if a process has completed.
Warning: this will reap any child processes, and their information can only be retrieved via @ref process_wait.
@param q A work queue object.
*/
void work_queue_enable_process_module(struct work_queue *q);

/** Does all the heavy lifting for submitting a task. work_queue_submit is
simply a wrapper of this function that also generates a taskid.
work_queue_submit_internal is the submit function used in foreman, where the
taskid should not be modified.*/
int work_queue_submit_internal(struct work_queue *q, struct work_queue_task *t);

/** Same as @ref work_queue_invalidate_cached_file, but takes filename as face value, rather than computing cached_name. */
void work_queue_invalidate_cached_file_internal(struct work_queue *q, const char *filename);

void release_all_workers(struct work_queue *q);

void update_catalog(struct work_queue *q, struct link *foreman_uplink, int force_update );

/** Send msg to all the workers in the queue. **/
void work_queue_broadcast_message(struct work_queue *q, const char *msg);

/* shortcut to set cores, memory, disk, etc. from a single function. */
void work_queue_task_specify_resources(struct work_queue_task *t, const struct rmsummary *rm);

