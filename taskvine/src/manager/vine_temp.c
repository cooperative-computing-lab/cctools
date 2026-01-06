#include "vine_temp.h"
#include "vine_file.h"
#include "vine_worker_info.h"
#include "vine_file_replica_table.h"
#include "vine_manager.h"
#include "vine_manager_put.h"
#include "vine_file_replica.h"
#include "vine_task.h"
#include "vine_mount.h"

#include "priority_queue.h"
#include "macros.h"
#include "stringtools.h"
#include "debug.h"
#include "random.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
Check whether a worker is eligible to participate in peer transfers.
*/
static int worker_can_peer_transfer(struct vine_worker_info *w)
{
	if (!w) {
		return 0;
	}
	if (w->type != VINE_WORKER_TYPE_WORKER) {
		return 0;
	}
	if (!w->transfer_port_active) {
		return 0;
	}
	if (w->draining) {
		return 0;
	}
	if (!w->resources) {
		return 0;
	}
	if (w->resources->tag < 0) {
		return 0;
	}

	return 1;
}

/**
Return available disk space (in bytes) on a worker.
Note that w->resources->disk.total is in megabytes.
Use int64_t in case the actual usage is larger than the total space.
Returns -1 if worker or resources is invalid, or if the available disk space is negative.
*/
static int64_t worker_get_available_disk(const struct vine_worker_info *w)
{
	if (!w || !w->resources) {
		return -1;
	}

	int64_t available_disk = (int64_t)MEGABYTES_TO_BYTES(w->resources->disk.total) - w->inuse_cache;
	if (available_disk < 0) {
		return -1;
	}

	return available_disk;
}

/**
Find the most suitable worker to serve as the source of a replica transfer.
Eligible workers already host the file, have a ready replica, and are not
overloaded with outgoing transfers. Preference is given to workers with fewer
outgoing transfers to balance load.
*/
static struct vine_worker_info *get_best_source_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct set *sources = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!sources) {
		return NULL;
	}

	struct vine_worker_info *best_source_worker = NULL;

	struct vine_worker_info *w = NULL;
	SET_ITERATE(sources, w)
	{
		/* skip if the worker cannot participate in peer transfers */
		if (!worker_can_peer_transfer(w)) {
			continue;
		}
		/* skip if outgoing transfer counter is too high */
		if (w->outgoing_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker does not have this file (which is faulty) */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (!replica) {
			continue;
		}
		/* skip if the file is not ready */
		if (replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}
		/* workers with fewer outgoing transfers are preferred */
		if (!best_source_worker || w->outgoing_xfer_counter < best_source_worker->outgoing_xfer_counter) {
			best_source_worker = w;
		}
	}

	return best_source_worker;
}

/**
Select a destination worker that can accept a new replica. Workers must be
active, not currently hosting the file, and have sufficient free cache space.
Those with more available disk space are prioritized to reduce pressure on
heavily utilized workers.
*/
static struct vine_worker_info *get_best_dest_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct vine_worker_info *best_dest_worker = NULL;

	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* skip if the worker cannot participate in peer transfers */
		if (!worker_can_peer_transfer(w)) {
			continue;
		}
		/* skip if the incoming transfer counter is too high */
		if (w->incoming_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker already has this file */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (replica) {
			continue;
		}
		/* skip if the worker does not have enough disk space */
		if (worker_get_available_disk(w) < (int64_t)f->size) {
			continue;
		}
		/* workers with more available disk space are preferred to hold the file */
		if (!best_dest_worker || worker_get_available_disk(w) > worker_get_available_disk(best_dest_worker)) {
			best_dest_worker = w;
		}
	}

	return best_dest_worker;
}

/**
Initiate a peer-to-peer transfer between two workers for the specified file.
The source worker provides a direct URL so the destination worker can pull the
replica immediately via `vine_manager_put_url_now`.
*/
static void start_peer_transfer(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker)
{
	if (!q || !f || f->type != VINE_TEMP || !dest_worker || !source_worker) {
		return;
	}

	char *source_addr = string_format("%s/%s", source_worker->transfer_url, f->cached_name);
	vine_manager_put_url_now(q, dest_worker, source_worker, source_addr, f);
	free(source_addr);
}

/**
Attempt to replicate a temporary file immediately by selecting compatible
source and destination workers. Returns 1 when a transfer is launched, or 0 if
no suitable pair of workers is currently available.
*/
static int attempt_replication(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return 0;
	}

	struct vine_worker_info *source_worker = get_best_source_worker(q, f);
	if (!source_worker) {
		return 0;
	}

	struct vine_worker_info *dest_worker = get_best_dest_worker(q, f);
	if (!dest_worker) {
		return 0;
	}

	start_peer_transfer(q, f, dest_worker, source_worker);

	return 1;
}

/*************************************************************/
/* Public Functions */
/*************************************************************/

/**
Check if a temporary file exists somewhere in all workers.
Returns 1 if at least one CREATING or READY replica exists, 0 otherwise.

We accept both CREATING and READY replicas as available sources, since a CREATING
replica may already exist physically but hasn't yet received the cache-update
message from the manager. However, we do not accept DELETING replicas, as they
indicate the source worker has already been sent an unlink request—any subsequent
cache-update or cache-invalid events will lead to deletion.

If the file's state is not CREATED, we need to wait for the producer task to
complete before checking for replicas.
*/
int vine_temp_exists_somewhere(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	struct set *workers = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!workers || set_size(workers) < 1) {
		return 0;
	}

	struct vine_worker_info *w;
	SET_ITERATE(workers, w)
	{
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (replica && (replica->state == VINE_FILE_REPLICA_STATE_CREATING || replica->state == VINE_FILE_REPLICA_STATE_READY)) {
			return 1;
		}
	}

	return 0;
}

/**
Queue a temporary file for replication when it still lacks the target number of
replicas. Files without any replica and those already satisfying the quota are
ignored. A lower priority value gives preference to scarcer replicas.
*/
int vine_temp_queue_for_replication(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	if (q->temp_replica_count <= 1) {
		return 0;
	}

	int current_replica_count = vine_file_replica_count(q, f);
	if (current_replica_count == 0 || current_replica_count >= q->temp_replica_count) {
		return 0;
	}

	priority_queue_push(q->temp_files_to_replicate, f, -current_replica_count);

	return 1;
}

/**
Iterate through temporary files that still need additional replicas and
trigger peer-to-peer transfers when both a source and destination worker
are available. The function honors the manager's scheduling depth so that we
do not spend too much time evaluating the queue in a single invocation.
Files that cannot be replicated immediately are deferred by lowering their
priority and will be reconsidered in future calls.
*/
int vine_temp_start_replication(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	/* Count the number of files that have been processed. */
	int processed = 0;

	/* Only examine up to attempt_schedule_depth files to keep the event loop responsive. */
	int iter_depth = MIN(q->attempt_schedule_depth, priority_queue_size(q->temp_files_to_replicate));

	/* Files that cannot be replicated now are temporarily stored and re-queued at the end. */
	struct list *skipped = list_create();

	struct vine_file *f;
	for (int i = 0; i < iter_depth; i++) {
		f = priority_queue_pop(q->temp_files_to_replicate);
		if (!f) {
			break;
		}
		/* skip and discard the replication if the file is not valid */
		if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
			continue;
		}

		/* skip and discard the replication if the file has enough replicas or no sources at all */
		int current_replica_count = vine_file_replica_count(q, f);
		if (current_replica_count >= q->temp_replica_count || current_replica_count == 0) {
			continue;
		}
		/* skip and discard the replication if the file has no ready replicas, will be re-enqueued upon its next cache-update */
		int current_ready_replica_count = vine_file_replica_table_count_replicas(q, f->cached_name, VINE_FILE_REPLICA_STATE_READY);
		if (current_ready_replica_count == 0) {
			continue;
		}

		/* If reaches here, the file needs more replicas and has at least one ready source, so we start finding a valid source and destination worker
		 * to trigger the replication. If fails to find a valid source or destination worker, we requeue the file and will consider it later. */
		if (!attempt_replication(q, f)) {
			list_push_tail(skipped, f);
			continue;
		}

		processed++;

		/* Requeue the file with lower priority (-1 because the current replica count is added up) so it can accumulate replicas gradually. */
		vine_temp_queue_for_replication(q, f);
	}

	while ((f = list_pop_head(skipped))) {
		vine_temp_queue_for_replication(q, f);
	}
	list_delete(skipped);

	return processed;
}

/**
Clean redundant replicas of a temporary file.
For example, a file may be transferred to another worker because a task that declares it
as input is scheduled there, resulting in an extra replica that consumes storage space.
This function evaluates whether the file has excessive replicas and removes those on
workers that do not execute their dependent tasks.
*/
void vine_temp_clean_redundant_replicas(struct vine_manager *q, struct vine_file *f)
{
	if (!f || f->type != VINE_TEMP) {
		return;
	}

	struct set *source_workers = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!source_workers) {
		return;
	}

	int excess_replicas = set_size(source_workers) - q->temp_replica_count;
	if (excess_replicas <= 0) {
		return;
	}
	/* Note that this replica may serve as a source for a peer transfer. If it is unlinked prematurely,
	 * the corresponding transfer could fail and leave a task without its required data.
	 * Therefore, we must wait until all replicas are confirmed ready before proceeding. */
	if (vine_file_replica_table_count_replicas(q, f->cached_name, VINE_FILE_REPLICA_STATE_READY) != set_size(source_workers)) {
		return;
	}

	struct priority_queue *clean_replicas_from_workers = priority_queue_create(0);

	struct vine_worker_info *source_worker = NULL;
	SET_ITERATE(source_workers, source_worker)
	{
		/* if the file is actively in use by a task (the input to that task), we don't remove the replica on this worker */
		int file_inuse = 0;

		uint64_t task_id;
		struct vine_task *task;
		ITABLE_ITERATE(source_worker->current_tasks, task_id, task)
		{
			struct vine_mount *input_mount;
			LIST_ITERATE(task->input_mounts, input_mount)
			{
				if (f == input_mount->file) {
					file_inuse = 1;
					break;
				}
			}
			if (file_inuse) {
				break;
			}
		}

		if (file_inuse) {
			continue;
		}

		priority_queue_push(clean_replicas_from_workers, source_worker, source_worker->inuse_cache);
	}

	while (excess_replicas > 0) {
		source_worker = priority_queue_pop(clean_replicas_from_workers);
		if (!source_worker) {
			break;
		}
		delete_worker_file(q, source_worker, f->cached_name, 0, 0);
		excess_replicas--;
	}

	priority_queue_delete(clean_replicas_from_workers);

	return;
}

/**
Shift a temp file replica away from the worker using the most cache space.
This function looks for an alternative worker that can accept the file immediately
so that the original replica can be cleaned up later by @vine_temp_clean_redundant_replicas().
*/
void vine_temp_shift_disk_load(struct vine_manager *q, struct vine_worker_info *source_worker, struct vine_file *f)
{
	if (!q || !source_worker || !f || f->type != VINE_TEMP) {
		return;
	}

	struct vine_worker_info *target_worker = NULL;

	char *key;
	struct vine_worker_info *w = NULL;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* skip if the worker cannot participate in peer transfers */
		if (!worker_can_peer_transfer(w)) {
			continue;
		}
		/* skip if the worker already has this file */
		if (vine_file_replica_table_lookup(w, f->cached_name)) {
			continue;
		}
		/* skip if the worker does not have enough disk space */
		if (worker_get_available_disk(w) < (int64_t)f->size) {
			continue;
		}
		/* skip if the worker becomes heavier after the transfer */
		if (w->inuse_cache + f->size > source_worker->inuse_cache - f->size) {
			continue;
		}
		/* workers with more available disk space are preferred */
		if (!target_worker || worker_get_available_disk(w) > worker_get_available_disk(target_worker)) {
			target_worker = w;
		}
	}
	if (target_worker) {
		start_peer_transfer(q, f, target_worker, source_worker);
	}

	/* We can clean up the original one safely when the replica arrives at the destination worker. */
	vine_temp_clean_redundant_replicas(q, f);
}