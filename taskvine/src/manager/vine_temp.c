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

int is_checkpoint_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!q || !w || !w->features) {
		return 0;
	}

	return hash_table_lookup(w->features, "checkpoint-worker") != NULL;
}

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
		/* skip if the worker is a checkpoint worker */
		if (is_checkpoint_worker(q, w)) {
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
		/* workers with more available disk space are preferred */
		priority_queue_push(valid_destinations, w, available_disk_space);
		switch (q->replica_placement_policy) {
		case VINE_REPLICA_PLACEMENT_POLICY_RANDOM:
			priority_queue_push(valid_destinations, w, random_double());
			break;
		case VINE_REPLICA_PLACEMENT_POLICY_DISK_LOAD:
			priority_queue_push(valid_destinations, w, available_disk_space);
			break;
		case VINE_REPLICA_PLACEMENT_POLICY_TRANSFER_LOAD:
			priority_queue_push(valid_destinations, w, -w->incoming_xfer_counter);
			break;
		}
	}

	struct vine_worker_info *best_destination = priority_queue_pop(valid_destinations);
	priority_queue_delete(valid_destinations);

	return best_destination;
}

void vine_temp_start_peer_transfer(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source_worker, struct vine_worker_info *dest_worker)
{
	if (!q || !f || f->type != VINE_TEMP || !source_worker || !dest_worker) {
		return;
	}

	char *source_addr = string_format("%s/%s", source_worker->transfer_url, f->cached_name);
	vine_manager_put_url_now(q, dest_worker, source_worker, source_addr, f);
	free(source_addr);
}

int vine_temp_replicate_file_now(struct vine_manager *q, struct vine_file *f)
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

/*************************************************************/
/* Public Functions */
/*************************************************************/

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

void vine_set_replica_placement_policy(struct vine_manager *q, vine_replica_placement_policy_t policy)
{
	if (!q) {
		return;
	}

	switch (policy) {
	case VINE_REPLICA_PLACEMENT_POLICY_RANDOM:
		debug(D_VINE, "Setting replica placement policy to RANDOM");
		q->replica_placement_policy = policy;
		break;
	case VINE_REPLICA_PLACEMENT_POLICY_DISK_LOAD:
		debug(D_VINE, "Setting replica placement policy to DISK_LOAD");
		q->replica_placement_policy = policy;
		break;
	case VINE_REPLICA_PLACEMENT_POLICY_TRANSFER_LOAD:
		q->replica_placement_policy = policy;
		break;
	default:
		debug(D_ERROR, "Invalid replica placement policy: %d", policy);
	}
}
