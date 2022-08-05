/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_manager.h"
#include "ds_resources.h"

#include "list.h"
#include "hash_table.h"

struct ds_file {
	ds_file_t type;
	int flags;		// DS_CACHE or others in the future.
	int length;		// length of payload, only used for non-file objects like buffers and urls
	off_t offset;		// file offset for DS_FILE_PIECE
	off_t piece_length;	// file piece length for DS_FILE_PIECE
	char *payload;		// name on master machine or buffer of data.
	char *remote_name;	// name on remote machine.
	char *cached_name;	// name on remote machine in cached directory.
};

struct ds_task *ds_wait_internal(struct ds_manager *q, int timeout, struct link *foreman_uplink, int *foreman_uplink_active, const char *tag);

/* Adds (arithmetically) all the workers resources (cores, memory, disk) */
void aggregate_workers_resources( struct ds_manager *q, struct ds_resources *r, struct hash_table *categories );

/** Enable use of the process module.
This allows @ref ds_wait to call @ref process_pending from @ref process.h, exiting if a process has completed.
Warning: this will reap any child processes, and their information can only be retrieved via @ref process_wait.
@param q A work queue object.
*/
void ds_enable_process_module(struct ds_manager *q);

/** Does all the heavy lifting for submitting a task. ds_submit is
simply a wrapper of this function that also generates a taskid.
ds_submit_internal is the submit function used in foreman, where the
taskid should not be modified.*/
int ds_submit_internal(struct ds_manager *q, struct ds_task *t);

/** Same as @ref ds_invalidate_cached_file, but takes filename as face value, rather than computing cached_name. */
void ds_invalidate_cached_file_internal(struct ds_manager *q, const char *filename);

void release_all_workers(struct ds_manager *q);

void update_catalog(struct ds_manager *q, struct link *foreman_uplink, int force_update );

/** Send msg to all the workers in the queue. **/
void ds_broadcast_message(struct ds_manager *q, const char *msg);

/* shortcut to set cores, memory, disk, etc. from a single function. */
void ds_task_specify_resources(struct ds_task *t, const struct rmsummary *rm);

