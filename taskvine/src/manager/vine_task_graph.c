#include "vine_task_graph.h"
#include "taskvine.h"
#include "vine_manager.h"
#include "vine_worker_info.h"
#include "priority_queue.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "priority_queue.h"
#include <math.h>
#include "hash_table.h"
#include <sys/stat.h>
#include "itable.h"
#include "list.h"
#include "vine_task.h"
#include "timestamp.h"
#include "vine_file.h"
#include "set.h"
#include "vine_mount.h"
#include "progress_bar.h"
#include "assert.h"
#include "macros.h"
#include <signal.h>
#include <stdio.h>

static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

static void handle_sigint(int signal)
{
	interrupted = 1;
}

static void submit_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	int task_id = vine_task_node_submit(node);
	itable_insert(tg->task_id_to_node, task_id, node);

	node->retry_attempts_left--;
	if (node->retry_attempts_left < 0) {
		debug(D_ERROR, "Aborting, node %s has exhausted all retry attempts", node->node_key);
		vine_task_graph_delete(tg);
		exit(1);
	}

	return;
}

static void submit_node_ready_children(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	struct vine_task_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		/* Remove this parent from the child's pending set if it exists */
		if (child_node->pending_parents) {
			/* Assert that this parent is indeed pending for the child */
			assert(set_lookup(child_node->pending_parents, node));
			set_remove(child_node->pending_parents, node);
		}

		/* If no more parents are pending, submit the child */
		if (!child_node->pending_parents || set_size(child_node->pending_parents) == 0) {
			submit_node_task(tg, child_node);
		}
	}

	return;
}

static struct list *get_topological_order(struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}

	int total_nodes = hash_table_size(tg->nodes);
	struct list *topo_order = list_create();
	struct hash_table *in_degree_map = hash_table_create(0, 0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	char *key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, key, node)
	{
		int deg = list_size(node->parents);
		hash_table_insert(in_degree_map, key, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, compute_lex_priority(node->node_key));
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct vine_task_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct vine_task_node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)hash_table_lookup(in_degree_map, child->node_key);
			int deg = (int)raw_deg - 1;

			hash_table_remove(in_degree_map, child->node_key);
			hash_table_insert(in_degree_map, child->node_key, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, compute_lex_priority(child->node_key));
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_ERROR, "Error: task graph contains cycles or is malformed.\n");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.\n", total_nodes, list_size(topo_order));

		HASH_TABLE_ITERATE(tg->nodes, key, node)
		{
			intptr_t raw_deg = (intptr_t)hash_table_lookup(in_degree_map, key);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %s has in-degree %d. Parents:\n", key, deg);
				struct vine_task_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %s\n", p->node_key);
				}
			}
		}

		list_delete(topo_order);
		exit(1);
	}

	hash_table_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

static struct list *extract_weakly_connected_components(struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (set_lookup(visited, node)) {
			continue;
		}

		struct list *component = list_create();
		struct list *queue = list_create();

		list_push_tail(queue, node);
		set_insert(visited, node);
		list_push_tail(component, node);

		while (list_size(queue) > 0) {
			struct vine_task_node *curr = list_pop_head(queue);

			struct vine_task_node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct vine_task_node *c;
			LIST_ITERATE(curr->children, c)
			{
				if (!set_lookup(visited, c)) {
					list_push_tail(queue, c);
					set_insert(visited, c);
					list_push_tail(component, c);
				}
			}
		}

		list_push_tail(components, component);
		list_delete(queue);
	}

	set_delete(visited);
	return components;
}

static double compute_node_heavy_score(struct vine_task_node *node)
{
	if (!node) {
		return 0;
	}

	double up_score = node->depth * node->upstream_subgraph_size * node->fan_in;
	double down_score = node->height * node->downstream_subgraph_size * node->fan_out;

	return up_score / (down_score + 1);
}

static struct vine_task_node *get_node_by_task(struct vine_task_graph *tg, struct vine_task *task)
{
	if (!tg || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		return itable_lookup(tg->task_id_to_node, task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/* note that recovery tasks are not mapped to any node but we still need the original node for pruning,
		 * so we look up the outfile of the task, then map it back to get the original node */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			if (mount->file->original_producer_task_id > 0) {
				return itable_lookup(tg->task_id_to_node, mount->file->original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);

	return NULL;
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

void vine_task_graph_execute(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;

	/* create mapping from task_id and outfile cached_name to node */
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (node->outfile) {
			hash_table_insert(tg->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
	}

	/* add the parents' outfiles as inputs to the task */
	struct list *topo_order = get_topological_order(tg);
	LIST_ITERATE(topo_order, node)
	{
		struct vine_task_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->outfile) {
				vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			}
		}
	}

	/* initialize pending_parents for all nodes */
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		struct vine_task_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->pending_parents) {
				/* Use parent_node->node_key to ensure pointer consistency */
				set_insert(node->pending_parents, parent_node);
			}
		}
	}

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(tg->manager);

	/* enqueue those without dependencies */
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (!node->pending_parents || set_size(node->pending_parents) == 0) {
			submit_node_task(tg, node);
		}
	}

	/* calculate steps to inject failure */
	double next_failure_threshold = -1.0;
	if (tg->failure_injection_step_percent > 0) {
		next_failure_threshold = tg->failure_injection_step_percent / 100.0;
	}

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	struct ProgressBarPart *regular_tasks_part = progress_bar_create_part("Regular", hash_table_size(tg->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, regular_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	int wait_timeout = 2;

	while (regular_tasks_part->current < regular_tasks_part->total) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(tg->manager, wait_timeout);
		progress_bar_set_part_total(pbar, recovery_tasks_part, tg->manager->num_submitted_recovery_tasks);
		if (task) {
			/* retrieve all possible tasks */
			wait_timeout = 0;

			/* get the original node by task id */
			struct vine_task_node *node = get_node_by_task(tg, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			/* in case of failure, resubmit this task */
			if (node->task->result != VINE_RESULT_SUCCESS || node->task->exit_code != 0) {
				debug(D_VINE | D_NOTICE, "Task %d failed with result %d and exit code %d, resubmitting...", task->task_id, node->task->result, node->task->exit_code);
				vine_task_reset(node->task);
				submit_node_task(tg, node);
				continue;
			}

			/* if the outfile is set to save on the sharedfs, stat to get the size of the file */
			switch (node->outfile_type) {
			case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
				struct stat info;
				int result = stat(node->outfile_remote_name, &info);
				if (result < 0) {
					debug(D_VINE | D_NOTICE, "Task %d succeeded but output file %s does not exist on the shared file system", task->task_id, node->outfile_remote_name);
					vine_task_reset(node->task);
					submit_node_task(tg, node);
					continue;
				}
				node->outfile_size_bytes = info.st_size;
				break;
			}
			case VINE_NODE_OUTFILE_TYPE_LOCAL:
			case VINE_NODE_OUTFILE_TYPE_TEMP:
				node->outfile_size_bytes = node->outfile->size;
				break;
			}
			debug(D_VINE, "Node %s completed with outfile %s size: %ld bytes", node->node_key, node->outfile_remote_name, node->outfile_size_bytes);

			/* mark the node as completed */
			node->completed = 1;

			/* prune nodes on task completion */
			vine_task_node_prune_ancestors(node);

			/* skip recovery tasks */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				progress_bar_update_part(pbar, recovery_tasks_part, 1);
				continue;
			}

			/* set the start time to the submit time of the first regular task */
			if (regular_tasks_part->current == 0) {
				progress_bar_set_start_time(pbar, task->time_when_commit_start);
			}

			/* update critical time */
			vine_task_node_update_critical_time(node, task->time_workers_execute_last);

			/* mark this regular task as completed */
			progress_bar_update_part(pbar, regular_tasks_part, 1);

			/* inject failure */
			if (tg->failure_injection_step_percent > 0) {
				double progress = (double)regular_tasks_part->current / (double)regular_tasks_part->total;
				if (progress >= next_failure_threshold && evict_random_worker(tg->manager)) {
					debug(D_VINE, "evicted a worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += tg->failure_injection_step_percent / 100.0;
				}
			}

			/* enqueue the output file for replication */
			switch (node->outfile_type) {
			case VINE_NODE_OUTFILE_TYPE_TEMP:
				vine_task_node_replicate_outfile(node);
				break;
			case VINE_NODE_OUTFILE_TYPE_LOCAL:
			case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_node_ready_children(tg, node);
		} else {
			wait_timeout = 2;
			progress_bar_update_part(pbar, recovery_tasks_part, 0);
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	double total_time_spent_on_unlink_local_files = 0;
	double total_time_spent_on_prune_ancestors_of_temp_node = 0;
	double total_time_spent_on_prune_ancestors_of_persisted_node = 0;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		total_time_spent_on_unlink_local_files += node->time_spent_on_unlink_local_files;
		total_time_spent_on_prune_ancestors_of_temp_node += node->time_spent_on_prune_ancestors_of_temp_node;
		total_time_spent_on_prune_ancestors_of_persisted_node += node->time_spent_on_prune_ancestors_of_persisted_node;
	}
	total_time_spent_on_unlink_local_files /= 1e6;
	total_time_spent_on_prune_ancestors_of_temp_node /= 1e6;
	total_time_spent_on_prune_ancestors_of_persisted_node /= 1e6;

	debug(D_VINE, "total time spent on prune ancestors of temp node: %.6f seconds\n", total_time_spent_on_prune_ancestors_of_temp_node);
	debug(D_VINE, "total time spent on prune ancestors of persisted node: %.6f seconds\n", total_time_spent_on_prune_ancestors_of_persisted_node);
	debug(D_VINE, "total time spent on unlink local files: %.6f seconds\n", total_time_spent_on_unlink_local_files);

	return;
}

void vine_task_graph_finalize_metrics(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(tg);
	if (!topo_order) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;
	struct vine_task_node *parent_node;
	struct vine_task_node *child_node;

	/* compute the depth of the node */
	LIST_ITERATE(topo_order, node)
	{
		node->depth = 0;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->depth < parent_node->depth + 1) {
				node->depth = parent_node->depth + 1;
			}
		}
	}

	/* compute the height of the node */
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		node->height = 0;
		LIST_ITERATE(node->children, child_node)
		{
			if (node->height < child_node->height + 1) {
				node->height = child_node->height + 1;
			}
		}
	}

	/* compute the upstream and downstream counts for each node */
	struct hash_table *upstream_map = hash_table_create(0, 0);
	struct hash_table *downstream_map = hash_table_create(0, 0);
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		struct set *upstream = set_create(0);
		struct set *downstream = set_create(0);
		hash_table_insert(upstream_map, node_key, upstream);
		hash_table_insert(downstream_map, node_key, downstream);
	}
	LIST_ITERATE(topo_order, node)
	{
		struct set *upstream = hash_table_lookup(upstream_map, node->node_key);
		LIST_ITERATE(node->parents, parent_node)
		{
			struct set *parent_upstream = hash_table_lookup(upstream_map, parent_node->node_key);
			set_union(upstream, parent_upstream);
			set_insert(upstream, parent_node);
		}
	}
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		struct set *downstream = hash_table_lookup(downstream_map, node->node_key);
		LIST_ITERATE(node->children, child_node)
		{
			struct set *child_downstream = hash_table_lookup(downstream_map, child_node->node_key);
			set_union(downstream, child_downstream);
			set_insert(downstream, child_node);
		}
	}
	LIST_ITERATE(topo_order, node)
	{
		node->upstream_subgraph_size = set_size(hash_table_lookup(upstream_map, node->node_key));
		node->downstream_subgraph_size = set_size(hash_table_lookup(downstream_map, node->node_key));
		node->fan_in = list_size(node->parents);
		node->fan_out = list_size(node->children);
		set_delete(hash_table_lookup(upstream_map, node->node_key));
		set_delete(hash_table_lookup(downstream_map, node->node_key));
	}
	hash_table_delete(upstream_map);
	hash_table_delete(downstream_map);

	/* compute the heavy score  */
	LIST_ITERATE(topo_order, node)
	{
		node->heavy_score = compute_node_heavy_score(node);
	}

	/* extract weakly connected components */
	struct list *weakly_connected_components = extract_weakly_connected_components(tg);
	struct list *component;
	int component_index = 0;
	debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
	LIST_ITERATE(weakly_connected_components, component)
	{
		debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
		component_index++;
	}
	list_delete(weakly_connected_components);

	list_delete(topo_order);

	return;
}

struct vine_task_graph *vine_task_graph_create(struct vine_manager *q)
{
	struct vine_task_graph *tg = xxmalloc(sizeof(struct vine_task_graph));
	tg->nodes = hash_table_create(0, 0);
	tg->task_id_to_node = itable_create(0);
	tg->outfile_cachename_to_node = hash_table_create(0, 0);

	tg->library_name = xxstrdup("vine_task_graph_library");
	tg->function_name = xxstrdup("compute_single_key");
	tg->manager = q;

	tg->failure_injection_step_percent = -1.0;

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", tg->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	signal(SIGINT, handle_sigint);

	return tg;
}

void vine_task_graph_set_failure_injection_step_percent(struct vine_task_graph *tg, double percent)
{
	if (!tg) {
		return;
	}

	if (percent <= 0 || percent > 100) {
		return;
	}

	debug(D_VINE, "setting failure injection step percent to %lf", percent);
	tg->failure_injection_step_percent = percent;
}

struct vine_task_node *vine_task_graph_create_node(
		struct vine_task_graph *tg,
		const char *node_key,
		const char *staging_dir,
		int prune_depth,
		vine_task_node_priority_mode_t priority_mode)
{
	if (!tg || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		node = vine_task_node_create(tg->manager,
				node_key,
				tg->library_name,
				tg->function_name,
				staging_dir,
				prune_depth,
				priority_mode);
		hash_table_insert(tg->nodes, node_key, node);
	}

	return node;
}

void vine_task_graph_add_dependency(struct vine_task_graph *tg, const char *parent_key, const char *child_key)
{
	if (!tg || !parent_key || !child_key) {
		return;
	}

	struct vine_task_node *parent_node = hash_table_lookup(tg->nodes, parent_key);
	struct vine_task_node *child_node = hash_table_lookup(tg->nodes, child_key);
	if (!parent_node) {
		debug(D_ERROR, "parent node %s not found", parent_key);
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %s not found", child_key);
		exit(1);
	}

	list_push_tail(child_node->parents, parent_node);
	list_push_tail(parent_node->children, child_node);
	debug(D_VINE, "added dependency: %s -> %s", parent_key, child_key);
}

const char *vine_task_graph_get_library_name(const struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}
	return tg->library_name;
}

const char *vine_task_graph_get_function_name(const struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}
	return tg->function_name;
}

double vine_task_graph_get_node_heavy_score(const struct vine_task_graph *tg, const char *node_key)
{
	if (!tg) {
		return -1;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		return -1;
	}

	return node->heavy_score;
}

const char *vine_task_graph_get_node_local_outfile_source(const struct vine_task_graph *tg, const char *node_key)
{
	if (!tg || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		debug(D_ERROR, "node %s not found", node_key);
		exit(1);
	}

	if (node->outfile_type != VINE_NODE_OUTFILE_TYPE_LOCAL) {
		debug(D_ERROR, "node %s is not a local output file", node_key);
		exit(1);
	}

	return node->outfile->source;
}

void vine_task_graph_set_node_outfile(struct vine_task_graph *tg, const char *node_key, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name)
{
	if (!tg || !node_key || !outfile_remote_name) {
		return;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		return;
	}

	vine_task_node_set_outfile(node, outfile_type, outfile_remote_name);

	return;
}

void vine_task_graph_delete(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (node->infile) {
			vine_prune_file(tg->manager, node->infile);
			hash_table_remove(tg->manager->file_table, node->infile->cached_name);
		}
		if (node->outfile) {
			vine_prune_file(tg->manager, node->outfile);
			hash_table_remove(tg->outfile_cachename_to_node, node->outfile->cached_name);
			hash_table_remove(tg->manager->file_table, node->outfile->cached_name);
		}
		vine_task_node_delete(node);
	}

	vine_delete(tg->manager);

	free(tg->library_name);
	free(tg->function_name);

	hash_table_delete(tg->nodes);
	itable_delete(tg->task_id_to_node);
	hash_table_delete(tg->outfile_cachename_to_node);
	free(tg);
}
