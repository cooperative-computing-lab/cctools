#include "vine_checkpoint.h"

static struct list *get_reachable_files_by_topo_order(struct vine_manager *q, struct vine_file *start_file);
static void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f);
static int vine_checkpoint_ensure_pbb_space(struct vine_manager *q, struct vine_file *f);
static struct vine_worker_info *vine_checkpoint_choose_source(struct vine_manager *q, struct vine_file *f);
int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f);
static int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f);
int vine_checkpoint_persist(struct vine_manager *q, struct vine_worker_info *source, struct vine_file *f);


struct vine_worker_info *choose_checkpoint_worker(struct vine_manager *q)
{
    if (!q || !q->worker_table) {
        return NULL;
    }

    struct priority_queue *available_workers = priority_queue_create(0);

    char *key;
    struct vine_worker_info *w;

    HASH_TABLE_ITERATE(q->worker_table, key, w) {
        if (!w->is_checkpoint_worker) {
            continue;
        }
        
    }


}

int process_checkpoint_queue(struct vine_manager *q)
{
	if (q->checkpoint_threshold < 0 || !q->pbb_worker) {
		return 0;
	}

	timestamp_t start_time = timestamp_get();

	int num_checkpointed = 0;

	/* collect files to be removed */
	struct list *to_remove = list_create();

	char *key;
	struct vine_file *f;

	HASH_TABLE_ITERATE(q->checkpoint_queue, key, f)
	{	
		/* no need to checkpoint */
		if (f->penalty < q->checkpoint_threshold) {
			list_push_tail(to_remove, f);
			continue;
		}
		/* already checkpointed */
		if (vine_checkpoint_checkpointed(q, f)) {
			list_push_tail(to_remove, f);
			continue;
		}
		/* no valid source at the moment */
		struct vine_worker_info *source = vine_checkpoint_choose_source(q, f);
		if (!source) {
			continue;
		}
		/* no available space on PBB */
		if (!vine_checkpoint_ensure_pbb_space(q, f)) {
			list_push_tail(to_remove, f);
			continue;
		}
		/* checkpoint the file */
		vine_checkpoint_persist(q, source, f);
		num_checkpointed++;
		list_push_tail(to_remove, f);
	}

	LIST_ITERATE(to_remove, f)
	{
		char *cached_name = strdup(f->cached_name);
		hash_table_remove(q->checkpoint_queue, cached_name);
		free(cached_name);
	}

	list_delete(to_remove);

	timestamp_t end_time = timestamp_get();
	debug(D_VINE, "Checkpoint queue processing time: %ld", end_time - start_time);

	return num_checkpointed;
}

int vine_checkpoint_persist(struct vine_manager *q, struct vine_worker_info *source, struct vine_file *f)
{
    if (!q || !f || !source || !q->pbb_worker || f->type != VINE_TEMP) {
        return 0;
    }

	if (source->is_pbb_worker) {
		priority_queue_push_or_update(q->checkpointed_files, f, -f->penalty / f->size);
		assert(vine_file_replica_table_lookup(q->pbb_worker, f->cached_name));
	}

    if (!vine_file_replica_table_lookup(q->pbb_worker, f->cached_name)) {
        char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
        vine_manager_put_url_now(q, q->pbb_worker, source_addr, f);
        free(source_addr);
		priority_queue_push_or_update(q->checkpointed_files, f, -f->penalty / f->size);
    }

    f->recovery_critical_time = 0;
    f->recovery_total_time = 0;

    return 1;
}

int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f)
{
    if (q->checkpoint_threshold < 0 || !q || !f || f->type != VINE_TEMP) {
        return 0;
    }

    struct vine_file_replica *replica = vine_file_replica_table_lookup(q->pbb_worker, f->cached_name);
    if (!replica || replica->state != VINE_FILE_REPLICA_STATE_READY) {
        return 0;
    }

    /* update this file's recovery metrics after eviction */
	vine_checkpoint_update_file_penalty(q, f);

    /* update all downstream files' recovery metrics */
    struct list *files_in_topo_order = get_reachable_files_by_topo_order(q, f);
    struct vine_file *current_file;
    LIST_ITERATE(files_in_topo_order, current_file)
    {
        if (current_file && !vine_checkpoint_checkpointed(q, current_file)) {
            /* calculate new recovery metrics from input files */
            vine_checkpoint_update_file_penalty(q, current_file);
        }
    }
    list_delete(files_in_topo_order);

	priority_queue_remove_by_key(q->checkpointed_files, f->cached_name);
    delete_worker_file(q, q->pbb_worker, f->cached_name, 0, 0);

    return 1;
}

struct vine_worker_info *vine_checkpoint_choose_source(struct vine_manager *q, struct vine_file *f)
{
	struct priority_queue *valid_sources = ensure_temp_file_transfer_sources(q, f, 1e6);
	if (!valid_sources) {
		return NULL;
	}

	struct vine_worker_info *source = priority_queue_pop(valid_sources);
	priority_queue_delete(valid_sources);

	return source;
}

int vine_checkpoint_ensure_pbb_space(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f || f->type != VINE_TEMP) {
        return 0;
    }

    int64_t total_bytes = (int64_t)(q->pbb_worker->resources->disk.total * 1024 * 1024 * 0.95);
    int64_t available_bytes = total_bytes - q->pbb_worker->inuse_cache;
    int64_t this_file_size = (int64_t)(f->size);

    if (this_file_size <= available_bytes) {
        return 1;
    }

    double this_efficiency = f->penalty / f->size;

    double candidates_penalty = 0;
    int64_t candidates_size = 0;

    struct list *candidates = list_create();
    struct list *skipped_files = list_create();
    struct vine_file *popped_file;
    struct vine_file_replica *replica;
    
    while ((popped_file = priority_queue_pop(q->checkpointed_files))) {
        replica = vine_file_replica_table_lookup(q->pbb_worker, popped_file->cached_name);
        if (!replica) {
            continue;
        }
        
        if (replica->state != VINE_FILE_REPLICA_STATE_READY) {
            list_push_tail(skipped_files, popped_file);
            continue;
        }

        list_push_tail(candidates, popped_file);
        candidates_penalty += popped_file->penalty;
        candidates_size += popped_file->size;

        /* do we have enough space after evicting this file? */
        if (available_bytes + candidates_size >= (int64_t)f->size) {
            break;
        }
    }

    double candidates_efficiency = 0.0;
    if (candidates_size > 0) {
        candidates_efficiency = candidates_penalty / (double)candidates_size;
    } else {
        candidates_efficiency = DBL_MAX;
    }

    // if the size and efficiency of the candidates are not good enough, we don't want to evict for this file
	int ok = 0;
    if (available_bytes + candidates_size < (int64_t)f->size || candidates_efficiency > this_efficiency) {
		LIST_ITERATE(candidates, popped_file) {
			vine_checkpoint_persist(q, q->pbb_worker, popped_file);
		}
        ok = 0;
    } else {
		// otherwise, we can evict the selected files for this file
		LIST_ITERATE(candidates, popped_file) {
			vine_checkpoint_evict(q, popped_file);
		}
		debug(D_VINE | D_NOTICE, "evicted %d files, candidates penalty: %f, candidates size: %ld, candidates efficiency: %f, this efficiency: %f", 
          list_size(candidates), candidates_penalty, candidates_size, candidates_efficiency, this_efficiency);
		ok = 1;
	}

	LIST_ITERATE(skipped_files, popped_file) {
		vine_checkpoint_persist(q, q->pbb_worker, popped_file);
	}

	if (skipped_files) {
		list_delete(skipped_files);
	}
	if (candidates) {
		list_delete(candidates);
	}

	return ok;
}

/* Get all reachable files in topological order from the given starting file */
static struct list *get_reachable_files_by_topo_order(struct vine_manager *q, struct vine_file *start_file)
{
	if (!start_file || start_file->type != VINE_TEMP || vine_checkpoint_checkpointed(q, start_file)) {
		return list_create();
	}

	/* Define file visit states for topological sort */
	typedef enum {
		VISIT_STATE_UNVISITED = 0,   /* Node hasn't been seen yet */
		VISIT_STATE_IN_PROGRESS = 1, /* Node is being processed (temp mark) */
		VISIT_STATE_COMPLETED = 2    /* Node processing complete (permanent mark) */
	} visit_state_t;

	struct list *result = list_create();
	struct hash_table *visited = hash_table_create(0, 0);

	/* Helper structure for DFS */
	struct dfs_state {
		struct vine_file *file;
		struct list *queue; /* List of unprocessed children */
	};

	/* Create a stack for DFS (non-recursive) */
	struct list *stack = list_create();

	/* Add starting node to the stack */
	struct dfs_state *initial = malloc(sizeof(struct dfs_state));
	initial->file = start_file;
	initial->queue = list_create();

	/* Pre-populate with all VINE_TEMP type children */
	char *child_name;
	struct vine_file *child;
	HASH_TABLE_ITERATE(start_file->child_temp_files, child_name, child)
	{
		if (child && child->type == VINE_TEMP) {
			list_push_tail(initial->queue, child);
		}
	}

	list_push_head(stack, initial);
	hash_table_insert(visited, start_file->cached_name, (void *)VISIT_STATE_IN_PROGRESS);

	while (list_size(stack) > 0) {
		struct dfs_state *current = list_peek_head(stack);

		if (list_size(current->queue) == 0) {
			/* All children processed, add to result and mark as completed */
			list_pop_head(stack);
			list_push_tail(result, current->file);
			char *cached_name = strdup(current->file->cached_name);
			hash_table_remove(visited, cached_name);
			hash_table_insert(visited, current->file->cached_name, (void *)VISIT_STATE_COMPLETED);
			list_delete(current->queue);
			free(cached_name);
			free(current);
			continue;
		}

		/* Take next child from queue */
		struct vine_file *next_child = list_pop_head(current->queue);

		/* Check if this child has been visited */
		void *visit_state = hash_table_lookup(visited, next_child->cached_name);

		if (!visit_state) {
			/* Unvisited node - add to stack and mark as in progress */
			struct dfs_state *child_state = malloc(sizeof(struct dfs_state));
			child_state->file = next_child;
			child_state->queue = list_create();

			/* Pre-populate child queue with VINE_TEMP type files only */
			HASH_TABLE_ITERATE(next_child->child_temp_files, child_name, child)
			{
				if (child && child->type == VINE_TEMP && !vine_checkpoint_checkpointed(q, child)) {
					list_push_tail(child_state->queue, child);
				}
			}

			list_push_head(stack, child_state);
			hash_table_insert(visited, next_child->cached_name, (void *)VISIT_STATE_IN_PROGRESS);
		} else if ((visit_state_t)visit_state == VISIT_STATE_IN_PROGRESS) {
			/* Cycle detected, skip this child (could log cycle warning here) */
			continue;
		}
		/* If completed, we just skip it */
	}

	hash_table_delete(visited);

	return result;
}

void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f)
{
	if (!f || f->type != VINE_TEMP) {
		return;
	}

	f->recovery_critical_time = 0;
	f->recovery_total_time = 0;
	f->penalty = 0;

	struct vine_file *parent_file;
	char *parent_file_name;
	HASH_TABLE_ITERATE(f->parent_temp_files, parent_file_name, parent_file)
	{
		if (!parent_file) {
			continue;
		}
		f->recovery_critical_time = MAX(f->recovery_critical_time, parent_file->recovery_critical_time);
		f->recovery_total_time += parent_file->recovery_total_time;
	}

	f->recovery_critical_time += f->producer_task_execution_time;
	f->recovery_total_time += f->producer_task_execution_time;

	f->penalty = (double)(0.3 * f->recovery_total_time) + (double)(0.7 * f->recovery_critical_time);
}

int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || !q->pbb_worker || f->type != VINE_TEMP) {
		return 0;
	}

	return vine_file_replica_table_lookup(q->pbb_worker, f->cached_name) != NULL;
}