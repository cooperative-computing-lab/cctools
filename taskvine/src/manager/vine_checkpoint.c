#include "vine_checkpoint.h"

static struct list *get_reachable_files_by_topo_order(struct vine_manager *q, struct vine_file *start_file);
static void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f);
static int checkpoint_worker_ensure_space(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *checkpoint_worker);
static struct vine_worker_info *choose_source_worker(struct vine_manager *q, struct vine_file *f);
static struct vine_worker_info *choose_checkpoint_worker(struct vine_manager *q, struct vine_file *f);
static int checkpoint_demand(struct vine_manager *q, struct vine_file *f);
static int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f);
static int count_checkpoint_workers(struct vine_manager *q);
static int checkpoint_file(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *checkpoint_worker, struct vine_worker_info *source_worker);
static double vine_file_checkpoint_efficiency(struct vine_file *f);


static struct list *get_valid_sources(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct set *sources = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!sources) {
		return NULL;
	}

	struct list *valid_sources = list_create();
	struct vine_worker_info *w = NULL;
	SET_ITERATE(sources, w)
	{
        /* skip if transfer port is not active */
		if (!w->transfer_port_active) {
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
        /* workers with less outgoing transfer counter are preferred */
		list_push_tail(valid_sources, w);
	}

    if (list_size(valid_sources) == 0) {
        list_delete(valid_sources);
        return NULL;
    }

    return valid_sources;
}


static priority_queue *get_valid_destinations(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f || f->type != VINE_TEMP) {
        return NULL;
    }

    struct priority_queue *valid_destinations = priority_queue_create(0);

    char *key;
    struct vine_worker_info *w;
    HASH_TABLE_ITERATE(q->worker_table, key, w)
    {
        /* skip if transfer port is not active */
		if (!w->transfer_port_active) {
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
        /* skip if no space to checkpoint this file */
        /* if this is not a checkpoint worker, simply check the available disk space against the file size */
        int64_t available_disk_space = (int64_t)MEGABYTES_TO_BYTES(w->resources->disk.total) - w->inuse_cache;
        if (!w->is_checkpoint_worker) {
            if ((int64_t)f->size > available_disk_space) {
                continue;
            }
        }
        /* if this is a checkpoint worker, and checkpointing is enabled, ensure the space by evicting files */
        if (w->is_checkpoint_worker && q->checkpoint_threshold >= 0) {
            if (!checkpoint_worker_ensure_space(q, f, w)) {
                continue;
            }
        }
        /* workers with more available disk space are preferred */
        priority_queue_push(valid_destinations, w, available_disk_space);
    }

    if (priority_queue_size(valid_destinations) == 0) {
        priority_queue_delete(valid_destinations);
        return NULL;
    }

    return valid_destinations;
}


static int replicate_file(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source, struct vine_worker_info *destination)
{
    if (!q || !f || f->type != VINE_TEMP || !source  || !destination) {
        return 0;
    }

    char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
    vine_manager_put_url_now(q, destination, source_addr, f);
    free(source_addr);

    return 1;
}

static int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f || f->type != VINE_TEMP) {
        return 0;
    }

    struct vine_file_replica *replica = vine_file_replica_table_lookup(q->pbb_worker, f->cached_name);
    assert(replica != NULL);
    assert(replica->state == VINE_FILE_REPLICA_STATE_READY);

    /* remove the file from the checkpointed files */
    priority_map_remove(q->checkpointed_files, f);
    delete_worker_file(q, q->pbb_worker, f->cached_name, 0, 0);

    /* update this file's recovery metrics after eviction */
	vine_checkpoint_update_file_penalty(q, f);

    /* update all downstream files' recovery metrics */
    struct list *files_in_topo_order = get_reachable_files_by_topo_order(q, f);
    struct vine_file *current_file;
    LIST_ITERATE(files_in_topo_order, current_file)
    {
        assert(current_file != NULL);
        assert(current_file->type == VINE_TEMP);
        vine_checkpoint_update_file_penalty(q, current_file);
    }
    list_delete(files_in_topo_order);

    return 1;
}

static double vine_file_checkpoint_efficiency(struct vine_file *f)
{
    if (!f || f->type != VINE_TEMP) {
        return 0;
    }

    return (double)f->penalty / f->size;
}


static int checkpoint_worker_ensure_space(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *checkpoint_worker)
{
    if (!q || !f || f->type != VINE_TEMP || !checkpoint_worker) {
        return 0;
    }

    int64_t disk_available = MEGABYTES_TO_BYTES(checkpoint_worker->resources->disk.total) - checkpoint_worker->inuse_cache;

    /* return immediately if the worker has enough space */
    if (f->size <= disk_available) {
        return 1;
    }

    double this_efficiency = f->penalty / f->size;

    struct list *to_evict = list_create();
    double eviction_penalty = 0;
    int64_t eviction_size = 0;

    struct list *to_keep = list_create();
    struct vine_file *popped_file;
    
    while ((popped_file = priority_map_pop(q->checkpointed_files))) {
        if (!popped_file) {
            continue;
        }
        /* this file should be found */
        struct vine_file_replica *replica = vine_file_replica_table_lookup(checkpoint_worker, popped_file->cached_name);
        assert(replica != NULL);
        /* skip if the file is not ready */
        if (replica->state != VINE_FILE_REPLICA_STATE_READY) {
            list_push_tail(to_keep, popped_file);
            continue;
        }
        /* try to evict this file */
        list_push_tail(to_evict, popped_file);
        eviction_penalty += popped_file->penalty;
        eviction_size += popped_file->size;

        /* do we have enough space after evicting this file? */
        if (disk_available + eviction_size >= (int64_t)(f->size)) {
            break;
        }
    }

    /* first pop back the skipped files */
    while ((popped_file = list_pop_head(to_keep))) {
        priority_map_push(q->checkpointed_files, popped_file, -popped_file->penalty / popped_file->size);
    }
    list_delete(to_keep);

    int ok = 1;

    /* return if we don't have enough space, or the efficiency is not better */
    if (ok && disk_available + eviction_size < (int64_t)f->size) {
        ok = 0;
    }
    if (ok && ((eviction_penalty / eviction_size) > vine_file_checkpoint_efficiency(f))) {
        ok = 0;
    }

    if (!ok) {
        /* no need to evict, pop back the files */
        while ((popped_file = list_pop_head(to_evict))) {
            priority_map_push(q->checkpointed_files, popped_file, -vine_file_checkpoint_efficiency(popped_file));
        }
        list_delete(to_evict);
        return 0;
    }

    /* evict the files to free up space */
    while ((popped_file = list_pop_head(to_evict))) {
        vine_checkpoint_evict(q, popped_file);
    }
    list_delete(to_evict);

    return 1;
}

/* Get all reachable files in topological order from the given starting file */
static struct list *get_reachable_files_by_topo_order(struct vine_manager *q, struct vine_file *start_file)
{
	if (!start_file || start_file->type != VINE_TEMP || checkpoint_demand(q, start_file)) {
		return list_create();
	}

	/* for topological sort */
	typedef enum {
		VISIT_STATE_UNVISITED = 0,   /* node hasn't been seen yet */
		VISIT_STATE_IN_PROGRESS = 1, /* node is being processed (temp mark) */
		VISIT_STATE_COMPLETED = 2    /* node processing complete (permanent mark) */
	} visit_state_t;

	struct list *result = list_create();
	struct hash_table *visited = hash_table_create(0, 0);

	/* for DFS */
	struct dfs_state {
		struct vine_file *file;
		struct list *queue; /* unprocessed children */
	};

	/* for DFS */
	struct list *stack = list_create();

	/* add starting node to the stack */
	struct dfs_state *initial = malloc(sizeof(struct dfs_state));
	initial->file = start_file;
	initial->queue = list_create();

	/* pre-populate with all VINE_TEMP type children */
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
			/* all children processed, add to result and mark as completed */
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

		/* take next child from queue */
		struct vine_file *next_child = list_pop_head(current->queue);

		/* check if this child has been visited */
		void *visit_state = hash_table_lookup(visited, next_child->cached_name);

		if (!visit_state) {
			/* unvisited node - add to stack and mark as in progress */
			struct dfs_state *child_state = malloc(sizeof(struct dfs_state));
			child_state->file = next_child;
			child_state->queue = list_create();

			/* pre-populate child queue with VINE_TEMP type files only */
			HASH_TABLE_ITERATE(next_child->child_temp_files, child_name, child)
			{
				if (child && child->type == VINE_TEMP && !checkpoint_demand(q, child)) {
					list_push_tail(child_state->queue, child);
				}
			}

			list_push_head(stack, child_state);
			hash_table_insert(visited, next_child->cached_name, (void *)VISIT_STATE_IN_PROGRESS);
		} else if ((visit_state_t)visit_state == VISIT_STATE_IN_PROGRESS) {
			/* cycle detected, skip this child (should not happen) */
			continue;
		}
		/* if completed, we just skip it */
	}

	hash_table_delete(visited);

	return result;
}

void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f)
{
	if (!f || f->type != VINE_TEMP || q->checkpoint_threshold < 0) {
		return;
	}

    /* return if the file has been checkpointed */
    if (!checkpoint_demand(q, f)) {
        f->recovery_critical_time = 0;
        f->recovery_total_time = 0;
        f->penalty = 0;
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

	f->penalty = (double)(0.5 * f->recovery_total_time) + (double)(0.5 * f->recovery_critical_time);
}

static int checkpoint_demand(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || q->checkpoint_threshold < 0) {
		return 0;
	}

	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
    {
		if (w->is_checkpoint_worker) {
			struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
			if (replica) {
				/* already checkpointed, no need to checkpoint again */
				return 0;
			}
		}
	}

	/* no checkpoint replica found, need to checkpoint */
	return 1;
}


static int replica_demand(struct vine_manager *q, struct vine_file *f)
{
	if (!f || f->type != VINE_TEMP || q->temp_replica_count <= 1) {
		return 0;
	}

    int demand = q->temp_replica_count - vine_file_replica_count(q, f);
    if (demand > 0) {
        return demand;
    }
    return 0;
}

int vine_redundancy_process_temp_files(struct vine_manager *q)
{
    /* return if both replication and checkpointing are disabled */
    if (q->temp_replica_count <= 1 && q->checkpoint_threshold < 0) {
        return 0;
    }

    int processed = 0;

    int iter_count = 0;
    int iter_depth = MIN(q->attempt_schedule_depth, priority_map_size(q->temp_files_to_process));

    struct list *no_source_files = list_create();

	struct vine_file *f;
    while ((f = priority_map_pop(q->temp_files_to_process)) && (iter_count < iter_depth)) {
        assert(f != NULL);
        assert(f->type == VINE_TEMP);

        /* skip if the redundancy requirement is already satisfied */
        if ((replica_demand(q, f) <= 0) && (checkpoint_demand(q, f) <= 0)) {
            continue;
        }

        /* first get valid sources and destinations */
        struct list *valid_sources = get_valid_sources(q, f);
        if (!valid_sources) {
            list_push_tail(no_source_files, f);
            continue;
        }
        struct priority_queue *valid_destinations = get_valid_destinations(q, f);
        if (!valid_destinations) {
            list_delete(valid_sources);
            continue;
        }

        /* for each destination, choose a valid source and replicate the file */
        int replicated = 0;
        int checkpointed = 0;
		struct vine_worker_info *destination = NULL;
		struct vine_worker_info *source = NULL;
        while ((destination = priority_queue_pop(valid_destinations))) {
            int success = 0;
            LIST_ITERATE(valid_sources, source)
            {
                /* skip this source if they are on the same node */
                if (strcmp(source->hostname, destination->hostname) == 0) {
                    continue;
                }
                /* perform checkpointing */
                if (destination->is_checkpoint_worker && checkpoint_demand(q, f) > 0) {
                    replicate_file(q, f, source, destination);
                    f->recovery_critical_time = 0;
                    f->recovery_total_time = 0;
                    vine_checkpoint_update_file_penalty(q, f);
                    success = 1;
                    break;
                }
                /* perform replication */
                if (!destination->is_checkpoint_worker && replica_demand(q, f) > 0) {
                    replicate_file(q, f, source, destination);
                    success = 1;
                    break;
                }
            }
            /* break if we have checkpointed or replicated */
            if (success) {
                processed++;
                break;
            }
        }

        /* push back the file if we need more redundancy, files with less replicas are prioritized to consider */
        if (checkpoint_demand(q, f) > 0 || replica_demand(q, f) > 0) {
            priority_map_push_or_update(q->temp_files_to_process, f, replica_demand(q, f));
        }

        if (++iter_count >= iter_depth) {
            break;
        }
    }

    /* check those files that have no valid sources */
    struct vine_file *no_source_file;
    while ((no_source_file = list_pop_head(no_source_files))) {
        int has_valid_source = 0;
        struct set *sources = hash_table_lookup(q->file_worker_table, no_source_file->cached_name);

        if (sources) {
            struct vine_worker_info *source;
            SET_ITERATE(sources, source)
            {
                struct vine_file_replica *replica = vine_file_replica_table_lookup(source, no_source_file->cached_name);
                if (replica && replica->state == VINE_FILE_REPLICA_STATE_READY) {
                    has_valid_source = 1;
                    break;
                }
            }
        }

        if (!has_valid_source) {
            vine_prune_file(q, no_source_file);
            priority_map_remove(q->temp_files_to_process, no_source_file);
            if (q->transfer_temps_recovery) {
                vine_manager_consider_recovery_task(q, no_source_file, no_source_file->recovery_task);
            }
        } else {
            priority_map_push_or_update(q->temp_files_to_process, no_source_file, replica_demand(q, no_source_file));
        }
    }
    list_delete(no_source_files);

    return processed;
}