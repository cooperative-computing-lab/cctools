#include "vine_temp.h"
#include "priority_queue.h"
#include "vine_file.h"
#include "vine_worker_info.h"
#include "vine_file_replica_table.h"
#include "macros.h"
#include "stringtools.h"
#include "vine_manager.h"
#include "debug.h"
#include "random.h"
#include "vine_manager_put.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

static struct vine_worker_info *get_best_source_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct set *sources = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!sources) {
		return NULL;
	}

	struct priority_queue *valid_sources_queue = priority_queue_create(0);
	struct vine_worker_info *w = NULL;
	SET_ITERATE(sources, w)
	{
		/* skip if transfer port is not active or in draining mode */
		if (!w->transfer_port_active || w->draining) {
			continue;
		}
		/* skip if incoming transfer counter is too high */
		if (w->outgoing_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker does not have this file */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (!replica) {
			continue;
		}
		/* skip if the file is not ready */
		if (replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}
		/* those with less outgoing_xfer_counter are preferred */
		priority_queue_push(valid_sources_queue, w, -w->outgoing_xfer_counter);
	}

	struct vine_worker_info *best_source = priority_queue_pop(valid_sources_queue);
	priority_queue_delete(valid_sources_queue);

	return best_source;
}

static struct vine_worker_info *get_best_dest_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct priority_queue *valid_destinations = priority_queue_create(0);

	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* skip if transfer port is not active or in draining mode */
		if (!w->transfer_port_active || w->draining) {
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
		int64_t available_disk_space = get_worker_available_disk_bytes(w);
		if ((int64_t)f->size > available_disk_space) {
			continue;
		}
		/* workers with more available disk space are preferred to hold the file */
		priority_queue_push(valid_destinations, w, available_disk_space);
	}

	struct vine_worker_info *best_destination = priority_queue_pop(valid_destinations);
	priority_queue_delete(valid_destinations);

	return best_destination;
}

static int vine_temp_replicate_file_now(struct vine_manager *q, struct vine_file *f)
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

	vine_temp_start_peer_transfer(q, f, source_worker, dest_worker);

	return 1;
}

/*************************************************************/
/* Public Functions */
/*************************************************************/

void vine_temp_start_peer_transfer(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source_worker, struct vine_worker_info *dest_worker)
{
	if (!q || !f || f->type != VINE_TEMP || !source_worker || !dest_worker) {
		return;
	}

	char *source_addr = string_format("%s/%s", source_worker->transfer_url, f->cached_name);
	vine_manager_put_url_now(q, dest_worker, source_worker, source_addr, f);
	free(source_addr);
}

int vine_temp_replicate_file_later(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	int current_replica_count = vine_file_replica_count(q, f);
	if (current_replica_count == 0 || current_replica_count >= q->temp_replica_count) {
		return 0;
	}

	priority_queue_push(q->temp_files_to_replicate, f, -current_replica_count);

	return 1;
}

int vine_temp_handle_file_lost(struct vine_manager *q, char *cachename)
{
	if (!q || !cachename) {
		return 0;
	}

	struct vine_file *f = hash_table_lookup(q->file_table, cachename);
	if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	vine_temp_replicate_file_later(q, f);

	return 1;
}

int vine_temp_start_replication(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	int processed = 0;
	int iter_count = 0;
	int iter_depth = MIN(q->attempt_schedule_depth, priority_queue_size(q->temp_files_to_replicate));
	struct list *skipped = list_create();

	struct vine_file *f;
	while ((f = priority_queue_pop(q->temp_files_to_replicate)) && (iter_count++ < iter_depth)) {
		if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
			continue;
		}

		/* skip if the file has enough replicas or no replicas */
		int current_replica_count = vine_file_replica_count(q, f);
		if (current_replica_count >= q->temp_replica_count || current_replica_count == 0) {
			continue;
		}
		/* skip if the file has no ready replicas */
		int current_ready_replica_count = vine_file_replica_table_count_replicas(q, f->cached_name, VINE_FILE_REPLICA_STATE_READY);
		if (current_ready_replica_count == 0) {
			continue;
		}

		/* if reach here, it means the file needs to be replicated and there is at least one ready replica. */
		if (!vine_temp_replicate_file_now(q, f)) {
			list_push_tail(skipped, f);
			continue;
		}

		processed++;

		/* push back and keep evaluating the same file with a lower priority, until no more source
		 * or destination workers are available, or the file has enough replicas. */
		vine_temp_replicate_file_later(q, f);
	}

	while ((f = list_pop_head(skipped))) {
		vine_temp_replicate_file_later(q, f);
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
		/* no surprise - a cache-update message may trigger a file deletion. */
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

	source_worker = NULL;
	while (excess_replicas > 0 && (source_worker = priority_queue_pop(clean_replicas_from_workers))) {
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
		if (w->type != VINE_WORKER_TYPE_WORKER) {
			continue;
		}
		if (!w->transfer_port_active) {
			continue;
		}
		if (w->draining) {
			continue;
		}
		if (w->resources->tag < 0) {
			continue;
		}
		if (vine_file_replica_table_lookup(w, f->cached_name)) {
			continue;
		}
		if (w->inuse_cache + f->size > (source_worker->inuse_cache - f->size) * 0.8) {
			continue;
		}
		if (!target_worker || w->inuse_cache < target_worker->inuse_cache) {
			target_worker = w;
		}
	}
	if (target_worker) {
		char *source_addr = string_format("%s/%s", source_worker->transfer_url, f->cached_name);
		vine_manager_put_url_now(q, target_worker, source_worker, source_addr, f);
		free(source_addr);
	}

	/* We can clean up the original one safely when the replica arrives at the destination worker. */
	vine_temp_clean_redundant_replicas(q, f);
}