/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_manager.h"
#include "ds_resources.h"
#include "ds_file.h"

#include "list.h"
#include "hash_table.h"

#define RESOURCE_MONITOR_TASK_LOCAL_NAME "ds-%d-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"
#define RESOURCE_MONITOR_REMOTE_NAME_EVENTS RESOURCE_MONITOR_REMOTE_NAME "events.json"

struct ds_task *ds_wait_internal(struct ds_manager *q, int timeout, const char *tag );

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

void update_catalog(struct ds_manager *q, int force_update );

/** Send msg to all the workers in the queue. **/
void ds_broadcast_message(struct ds_manager *q, const char *msg);

/* shortcut to set cores, memory, disk, etc. from a single function. */
void ds_task_specify_resources(struct ds_task *t, const struct rmsummary *rm);

