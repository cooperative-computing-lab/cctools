#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#include "priority_queue.h"
#include "list.h"
#include "debug.h"
#include "itable.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "random.h"
#include "hash_table.h"
#include "set.h"
#include "timestamp.h"
#include "progress_bar.h"
#include "macros.h"
#include "uuid.h"

#include "strategic_orchestration_node.h"
#include "strategic_orchestration_graph.h"
#include "vine_manager.h"
#include "vine_worker_info.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "taskvine.h"
#include "vine_temp.h"

static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
 * Handle the SIGINT signal.
 * @param signal Reference to the signal.
 */
static void handle_sigint(int signal)
{
	interrupted = 1;
}

/**
 * Compute a lexicographic priority score from the node key.
 * Used during topological sorting to break ties deterministically.
 * @param key Reference to the node key.
 * @return The lexical priority.
 */
static double compute_lex_priority(const char *key)
{
	double score = 0.0;
	double factor = 1.0;
	for (int i = 0; i < 8 && key[i] != '\0'; i++) {
		score += (unsigned char)key[i] * factor;
		factor *= 0.01;
	}
	return -score;
}

/**
 * Calculate the priority of a node given the priority mode.
 * @param node Reference to the node object.
 * @param priority_mode Reference to the priority mode.
 * @return The priority.
 */
static double calculate_task_priority(struct strategic_orchestration_node *node, task_priority_mode_t priority_mode)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();

	struct strategic_orchestration_node *parent_node;

	switch (priority_mode) {
	case TASK_PRIORITY_MODE_RANDOM:
		priority = random_double();
		break;
	case TASK_PRIORITY_MODE_DEPTH_FIRST:
		priority = (double)node->depth;
		break;
	case TASK_PRIORITY_MODE_BREADTH_FIRST:
		priority = -(double)node->depth;
		break;
	case TASK_PRIORITY_MODE_FIFO:
		priority = -(double)current_time;
		break;
	case TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->outfile) {
				continue;
			}
			priority += (double)vine_file_size(parent_node->outfile);
		}
		break;
	case TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->outfile) {
				continue;
			}
			timestamp_t parent_task_completion_time = parent_node->task->time_workers_execute_last;
			priority += (double)vine_file_size(parent_node->outfile) * (double)parent_task_completion_time;
		}
		break;
	}

	return priority;
}

/**
 * Submit a node to the taskvine manager.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node Reference to the node object.
 */
static void submit_node_task(struct strategic_orchestration_graph *sog, struct strategic_orchestration_node *node)
{
	if (!sog || !node) {
		return;
	}

	/* calculate the priority of the node */
	double priority = calculate_task_priority(node, sog->task_priority_mode);
	vine_task_set_priority(node->task, priority);

	/* submit the task to the manager */
	timestamp_t time_start = timestamp_get();
	int task_id = vine_submit(sog->manager, node->task);
	double time_taken = (double)(timestamp_get() - time_start) / 1e6;
	FILE *fp = fopen("vinedag_submission_time.txt", "a");
	fprintf(fp, "%.6f\n", time_taken);
	fclose(fp);

	/* insert the task id to the task id to node map */
	itable_insert(sog->task_id_to_node, task_id, node);

	debug(D_VINE, "submitted node %s with task id %d", node->node_key, task_id);

	return;
}

/**
 * Submit the children of a node if all its dependencies are resolved.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node Reference to the node object.
 */
static void submit_unblocked_children(struct strategic_orchestration_graph *sog, struct strategic_orchestration_node *node)
{
	if (!sog || !node) {
		return;
	}

	struct strategic_orchestration_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		/* Remove this parent from the child's pending set if it exists */
		if (child_node->pending_parents) {
			/* Assert that this parent is indeed pending for the child */
			if (child_node->pending_parents && set_lookup(child_node->pending_parents, node)) {
				set_remove(child_node->pending_parents, node);
			} else {
				debug(D_ERROR, "inconsistent pending set: child=%s missing parent=%s", child_node->node_key, node->node_key);
			}
		}

		/* If no more parents are pending, submit the child */
		if (!child_node->pending_parents || set_size(child_node->pending_parents) == 0) {
			submit_node_task(sog, child_node);
		}
	}

	return;
}

/**
 * Get the topological order of the strategic orchestration graph.
 * Must be called after all nodes and dependencies are added and the topology metrics are computed.
 * @param sog Reference to the strategic orchestration graph object.
 * @return The list of nodes in topological order.
 */
static struct list *get_topological_order(struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return NULL;
	}

	int total_nodes = hash_table_size(sog->nodes);
	struct list *topo_order = list_create();
	struct hash_table *in_degree_map = hash_table_create(0, 0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	char *key;
	struct strategic_orchestration_node *node;
	HASH_TABLE_ITERATE(sog->nodes, key, node)
	{
		int deg = list_size(node->parents);
		hash_table_insert(in_degree_map, key, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, compute_lex_priority(node->node_key));
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct strategic_orchestration_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct strategic_orchestration_node *child;
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
		debug(D_ERROR, "Error: strategic orchestration graph contains cycles or is malformed.\n");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.\n", total_nodes, list_size(topo_order));

		HASH_TABLE_ITERATE(sog->nodes, key, node)
		{
			intptr_t raw_deg = (intptr_t)hash_table_lookup(in_degree_map, key);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %s has in-degree %d. Parents:\n", key, deg);
				struct strategic_orchestration_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %s\n", p->node_key);
				}
			}
		}

		list_delete(topo_order);
		hash_table_delete(in_degree_map);
		priority_queue_delete(pq);
		exit(1);
	}

	hash_table_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

/**
 * Extract the weakly connected components of the strategic orchestration graph.
 * This function is used only for debugging purposes at the moment.
 * @param sog Reference to the strategic orchestration graph object.
 * @return The list of weakly connected components.
 */
static struct list *extract_weakly_connected_components(struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	char *node_key;
	struct strategic_orchestration_node *node;
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
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
			struct strategic_orchestration_node *curr = list_pop_head(queue);

			struct strategic_orchestration_node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct strategic_orchestration_node *c;
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

/**
 * Compute the heavy score of a node in the strategic orchestration graph.
 * @param node Reference to the node object.
 * @return The heavy score.
 */
static double compute_node_heavy_score(struct strategic_orchestration_node *node)
{
	if (!node) {
		return 0;
	}

	double up_score = node->depth * node->upstream_subgraph_size * node->fan_in;
	double down_score = node->height * node->downstream_subgraph_size * node->fan_out;

	return up_score / (down_score + 1);
}

/**
 * Map a task to a node in the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @param task Reference to the task object.
 * @return The node object.
 */
static struct strategic_orchestration_node *get_node_by_task(struct strategic_orchestration_graph *sog, struct vine_task *task)
{
	if (!sog || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		/* standard tasks are mapped directly to a node */
		return itable_lookup(sog->task_id_to_node, task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/* note that recovery tasks are not mapped to any node but we still need the original node for pruning,
		 * so we look up the outfile of the task, then map it back to get the original node */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			if (mount->file->original_producer_task_id > 0) {
				return itable_lookup(sog->task_id_to_node, mount->file->original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);

	return NULL;
}

/**
 * Prune the ancestors of a persisted node. This is only used for persisted nodes that produce persisted files.
 * All ancestors we consider here include both temp nodes and persisted nodes, becasue data written to shared file system
 * is safe and can definitely trigger upstream data redundancy to be released.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_persisted_node(struct strategic_orchestration_graph *sog, struct strategic_orchestration_node *node)
{
	if (!sog || !node) {
		return -1;
	}

	/* find all safe ancestors */
	struct set *safe_ancestors = son_find_safe_ancestors(node);
	if (!safe_ancestors) {
		return 0;
	}

	int pruned_replica_count = 0;

	timestamp_t start_time = timestamp_get();

	/* prune all safe ancestors */
	struct strategic_orchestration_node *ancestor_node;
	SET_ITERATE(safe_ancestors, ancestor_node)
	{
		switch (ancestor_node->outfile_type) {
		case NODE_OUTFILE_TYPE_LOCAL:
			/* do not prune the local file */
			break;
		case NODE_OUTFILE_TYPE_TEMP:
			/* prune the temp file */
			vine_prune_file(sog->manager, ancestor_node->outfile);
			break;
		case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
			/* unlink directly from the shared file system */
			unlink(ancestor_node->outfile_remote_name);
			break;
		}
		ancestor_node->prune_status = PRUNE_STATUS_SAFE;
		pruned_replica_count++;
	}

	set_delete(safe_ancestors);

	node->time_spent_on_prune_ancestors_of_persisted_node += timestamp_get() - start_time;

	return pruned_replica_count;
}

/**
 * Prune the ancestors of a temp node.
 * This function opportunistically releases upstream temporary files
 * that are no longer needed once this temp-producing node has completed.
 *
 * Only ancestors producing temporary outputs are considered here.
 * Files stored in the shared filesystem are never pruned by this function,
 * because temp outputs are not considered sufficiently safe to trigger
 * deletion of persisted data upstream.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_temp_node(struct strategic_orchestration_graph *sog, struct strategic_orchestration_node *node)
{
	if (!sog || !node || !node->outfile || node->prune_depth <= 0) {
		return 0;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	struct list *parents = son_find_parents_by_depth(node, node->prune_depth);

	struct strategic_orchestration_node *parent_node;
	LIST_ITERATE(parents, parent_node)
	{
		/* skip if the parent does not produce a temp file */
		if (parent_node->outfile_type != NODE_OUTFILE_TYPE_TEMP) {
			continue;
		}

		/* a file is prunable if its outfile is no longer needed by any child node:
		 * 1. it has no pending dependents
		 * 2. all completed dependents have also completed their corresponding recovery tasks, if any */
		int all_children_completed = 1;
		struct strategic_orchestration_node *child_node;
		LIST_ITERATE(parent_node->children, child_node)
		{
			/* break early if the child node is not completed */
			if (!child_node->completed) {
				all_children_completed = 0;
				break;
			}
			/* if the task produces a temp file and the recovery task is running, the parent is not prunable */
			if (child_node->outfile && child_node->outfile->type == VINE_TEMP) {
				struct vine_task *child_node_recovery_task = child_node->outfile->recovery_task;
				if (child_node_recovery_task && (child_node_recovery_task->state != VINE_TASK_INITIAL && child_node_recovery_task->state != VINE_TASK_DONE)) {
					all_children_completed = 0;
					break;
				}
			}
		}
		if (!all_children_completed) {
			continue;
		}

		pruned_replica_count += vine_prune_file(sog->manager, parent_node->outfile);
		/* this parent is pruned because a successor that produces a temp file is completed, it is unsafe because the
		 * manager may submit a recovery task to bring it back in case of worker failures. */
		parent_node->prune_status = PRUNE_STATUS_UNSAFE;
	}

	list_delete(parents);

	node->time_spent_on_prune_ancestors_of_temp_node += timestamp_get() - start_time;

	return pruned_replica_count;
}

/**
 * Prune the ancestors of a node when it is completed.
 * @param node Reference to the node object.
 */
static void prune_ancestors_of_node(struct strategic_orchestration_graph *sog, struct strategic_orchestration_node *node)
{
	if (!sog || !node) {
		return;
	}

	/* do not prune if the node is not completed */
	if (!node->completed) {
		return;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	switch (node->outfile_type) {
	case NODE_OUTFILE_TYPE_LOCAL:
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		/* If the outfile was declared as a VINE_FILE or was written to the shared fs, then it is guaranteed to be persisted
		 * and there is no chance that it will be lost unexpectedly. So we can safely prune all ancestors of this node. */
		pruned_replica_count = prune_ancestors_of_persisted_node(sog, node);
		break;
	case NODE_OUTFILE_TYPE_TEMP:
		/* Otherwise, if the node outfile is a temp file, we need to be careful about pruning, because temp files are prone
		 * to failures, while means they can be lost due to node evictions or failures. */
		pruned_replica_count = prune_ancestors_of_temp_node(sog, node);
		break;
	}

	timestamp_t elapsed_time = timestamp_get() - start_time;

	debug(D_VINE, "pruned %d ancestors of node %s in %.6f seconds", pruned_replica_count, node->node_key, elapsed_time / 1000000.0);

	return;
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/** Tune the strategic orchestration graph.
 *@param sog Reference to the strategic orchestration graph object.
 *@param name Reference to the name of the parameter to tune.
 *@param value Reference to the value of the parameter to tune.
 *@return 0 on success, -1 on failure.
 */
int sog_tune(struct strategic_orchestration_graph *sog, const char *name, const char *value)
{
	if (!sog || !name || !value) {
		return -1;
	}

	if (strcmp(name, "failure-injection-step-percent") == 0) {
		sog->failure_injection_step_percent = atof(value);

	} else if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "random") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_RANDOM;
		} else if (strcmp(value, "depth-first") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_DEPTH_FIRST;
		} else if (strcmp(value, "breadth-first") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_BREADTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_FIFO;
		} else if (strcmp(value, "lifo") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_LIFO;
		} else if (strcmp(value, "largest-input-first") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "largest-storage-footprint-first") == 0) {
			sog->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST;
		} else {
			debug(D_ERROR, "invalid priority mode: %s", value);
			return -1;
		}

	} else if (strcmp(name, "output-dir") == 0) {
		if (sog->output_dir) {
			free(sog->output_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		sog->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		sog->prune_depth = atoi(value);

	} else if (strcmp(name, "checkpoint-fraction") == 0) {
		double fraction = atof(value);
		if (fraction < 0.0 || fraction > 1.0) {
			debug(D_ERROR, "invalid checkpoint fraction: %s (must be between 0.0 and 1.0)", value);
			return -1;
		}
		sog->checkpoint_fraction = fraction;

	} else if (strcmp(name, "checkpoint-dir") == 0) {
		if (sog->checkpoint_dir) {
			free(sog->checkpoint_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		sog->checkpoint_dir = xxstrdup(value);

	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double val = atof(value);
		sog->progress_bar_update_interval_sec = (val > 0.0) ? val : 0.1;

	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;
	}

	return 0;
}

/**
 * Get the outfile remote name of a node in the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node_key Reference to the node key.
 * @return The outfile remote name.
 */
const char *sog_get_node_outfile_remote_name(const struct strategic_orchestration_graph *sog, const char *node_key)
{
	if (!sog || !node_key) {
		return NULL;
	}

	struct strategic_orchestration_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		return NULL;
	}

	return node->outfile_remote_name;
}

/**
 * Get the proxy library name of the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @return The proxy library name.
 */
const char *sog_get_proxy_library_name(const struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return NULL;
	}

	return sog->proxy_library_name;
}

/**
 * Set the proxy function name of the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @param proxy_function_name Reference to the proxy function name.
 */
void sog_set_proxy_function_name(struct strategic_orchestration_graph *sog, const char *proxy_function_name)
{
	if (!sog || !proxy_function_name) {
		return;
	}

	if (sog->proxy_function_name) {
		free(sog->proxy_function_name);
	}

	sog->proxy_function_name = xxstrdup(proxy_function_name);
}

/**
 * Get the heavy score of a node in the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node_key Reference to the node key.
 * @return The heavy score.
 */
double sog_get_node_heavy_score(const struct strategic_orchestration_graph *sog, const char *node_key)
{
	if (!sog) {
		return -1;
	}

	struct strategic_orchestration_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		return -1;
	}

	return node->heavy_score;
}

/**
 * Get the local outfile source of a node in the strategic orchestration graph, only valid for local output files.
 * The source of a local output file is the path on the local filesystem.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node_key Reference to the node key.
 * @return The local outfile source.
 */
const char *sog_get_node_local_outfile_source(const struct strategic_orchestration_graph *sog, const char *node_key)
{
	if (!sog || !node_key) {
		return NULL;
	}

	struct strategic_orchestration_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		debug(D_ERROR, "node %s not found", node_key);
		exit(1);
	}

	if (node->outfile_type != NODE_OUTFILE_TYPE_LOCAL) {
		debug(D_ERROR, "node %s is not a local output file", node_key);
		exit(1);
	}

	return node->outfile->source;
}

/**
 * Compute the topology metrics of the strategic orchestration graph, including depth, height, upstream and downstream counts,
 * heavy scores, and weakly connected components. Must be called after all nodes and dependencies are added.
 * @param sog Reference to the strategic orchestration graph object.
 */
void sog_compute_topology_metrics(struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return;
	}

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(sog);
	if (!topo_order) {
		return;
	}

	char *node_key;
	struct strategic_orchestration_node *node;
	struct strategic_orchestration_node *parent_node;
	struct strategic_orchestration_node *child_node;

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
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
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

	/* compute the heavy score for each node */
	LIST_ITERATE(topo_order, node)
	{
		node->heavy_score = compute_node_heavy_score(node);
	}

	/* sort nodes using priority queue */
	int total_nodes = list_size(topo_order);
	int total_target_nodes = 0;
	struct priority_queue *sorted_nodes = priority_queue_create(total_nodes);
	LIST_ITERATE(topo_order, node)
	{
		if (node->is_target_key) {
			total_target_nodes++;
		}
		priority_queue_push(sorted_nodes, node, node->heavy_score);
	}
	/* calculate the number of nodes to be checkpointed */
	int checkpoint_count = (int)((total_nodes - total_target_nodes) * sog->checkpoint_fraction);
	if (checkpoint_count < 0) {
		checkpoint_count = 0;
	}

	/* assign outfile types to each node */
	int assigned_checkpoint_count = 0;
	while ((node = priority_queue_pop(sorted_nodes))) {
		if (node->is_target_key) {
			/* declare the output file as a vine_file so that it can be retrieved by the manager as usual */
			node->outfile_type = NODE_OUTFILE_TYPE_LOCAL;
			char *local_outfile_path = string_format("%s/%s", sog->output_dir, node->outfile_remote_name);
			node->outfile = vine_declare_file(sog->manager, local_outfile_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
			free(local_outfile_path);
			continue;
		}
		if (assigned_checkpoint_count < checkpoint_count) {
			/* checkpointed files will be written directly to the shared file system, no need to manage them in the manager */
			node->outfile_type = NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM;
			char *shared_file_system_outfile_path = string_format("%s/%s", sog->checkpoint_dir, node->outfile_remote_name);
			free(node->outfile_remote_name);
			node->outfile_remote_name = shared_file_system_outfile_path;
			node->outfile = NULL;
			assigned_checkpoint_count++;
		} else {
			/* other nodes will be declared as temp files to leverage node-local storage */
			node->outfile_type = NODE_OUTFILE_TYPE_TEMP;
			node->outfile = vine_declare_temp(sog->manager);
		}
	}
	/* track the output dependencies of regular and vine_temp nodes */
	LIST_ITERATE(topo_order, node)
	{
		if (node->outfile) {
			vine_task_add_output(node->task, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}
	priority_queue_delete(sorted_nodes);

	/* extract weakly connected components */
	struct list *weakly_connected_components = extract_weakly_connected_components(sog);
	struct list *component;
	int component_index = 0;
	debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
	LIST_ITERATE(weakly_connected_components, component)
	{
		debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
		list_delete(component);
		component_index++;
	}
	list_delete(weakly_connected_components);

	list_delete(topo_order);

	return;
}

/**
 * Create a new node and track it in the strategic orchestration graph.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node_key Reference to the node key.
 * @param is_target_key Reference to whether the node is a target key.
 * @return A new node object.
 */
void sog_add_node(struct strategic_orchestration_graph *sog, const char *node_key, int is_target_key)
{
	if (!sog || !node_key) {
		return;
	}

	/* if the node already exists, skip creating a new one */
	struct strategic_orchestration_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		node = son_create(node_key, is_target_key);

		if (!node) {
			debug(D_ERROR, "failed to create node %s", node_key);
			sog_delete(sog);
			exit(1);
		}

		if (!sog->proxy_function_name) {
			debug(D_ERROR, "proxy function name is not set");
			sog_delete(sog);
			exit(1);
		}

		if (!sog->proxy_library_name) {
			debug(D_ERROR, "proxy library name is not set");
			sog_delete(sog);
			exit(1);
		}

		/* create node task */
		node->task = vine_task_create(sog->proxy_function_name);
		vine_task_set_library_required(node->task, sog->proxy_library_name);
		vine_task_addref(node->task);

		/* construct the task arguments and declare the infile */
		char *task_arguments = son_construct_task_arguments(node);
		node->infile = vine_declare_buffer(sog->manager, task_arguments, strlen(task_arguments), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
		free(task_arguments);
		vine_task_add_input(node->task, node->infile, "infile", VINE_TRANSFER_ALWAYS);

		/* initialize the pruning depth of each node, currently statically set to the global prune depth */
		node->prune_depth = sog->prune_depth;

		hash_table_insert(sog->nodes, node_key, node);
	}
}

/**
 * Create a new strategic orchestration graph object and bind a manager to it.
 * @param q Reference to the manager object.
 * @return A new strategic orchestration graph object.
 */
struct strategic_orchestration_graph *sog_create(struct vine_manager *q)
{
	if (!q) {
		return NULL;
	}

	struct strategic_orchestration_graph *sog = xxmalloc(sizeof(struct strategic_orchestration_graph));

	sog->manager = q;

	sog->checkpoint_dir = xxstrdup(sog->manager->runtime_directory); // default to current working directory
	sog->output_dir = xxstrdup(sog->manager->runtime_directory);	 // default to current working directory

	sog->nodes = hash_table_create(0, 0);
	sog->task_id_to_node = itable_create(0);
	sog->outfile_cachename_to_node = hash_table_create(0, 0);

	cctools_uuid_t proxy_library_name_id;
	cctools_uuid_create(&proxy_library_name_id);
	sog->proxy_library_name = xxstrdup(proxy_library_name_id.str);

	sog->proxy_function_name = NULL;

	sog->prune_depth = 1;

	sog->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	sog->failure_injection_step_percent = -1.0;

	sog->progress_bar_update_interval_sec = 0.1;

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", sog->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	return sog;
}

/**
 * Add a dependency between two nodes in the strategic orchestration graph. Note that the input-output file relationship
 * is not handled here, because their file names may have not been determined yet.
 * @param sog Reference to the strategic orchestration graph object.
 * @param parent_key Reference to the parent node key.
 * @param child_key Reference to the child node key.
 */
void sog_add_dependency(struct strategic_orchestration_graph *sog, const char *parent_key, const char *child_key)
{
	if (!sog || !parent_key || !child_key) {
		return;
	}

	struct strategic_orchestration_node *parent_node = hash_table_lookup(sog->nodes, parent_key);
	struct strategic_orchestration_node *child_node = hash_table_lookup(sog->nodes, child_key);
	if (!parent_node) {
		debug(D_ERROR, "parent node %s not found", parent_key);
		char *node_key = NULL;
		struct strategic_orchestration_node *node;
		printf("parent_keys:\n");
		HASH_TABLE_ITERATE(sog->nodes, node_key, node)
		{
			printf("  %s\n", node->node_key);
		}
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %s not found", child_key);
		exit(1);
	}

	list_push_tail(child_node->parents, parent_node);
	list_push_tail(parent_node->children, child_node);

	return;
}

/**
 * Execute the strategic orchestration graph. This must be called after all nodes and dependencies are added and the topology metrics are computed.
 * @param sog Reference to the strategic orchestration graph object.
 */
void sog_execute(struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return;
	}

	signal(SIGINT, handle_sigint);

	debug(D_VINE, "start executing strategic orchestration graph");

	/* print the info of all nodes */
	char *node_key;
	struct strategic_orchestration_node *node;
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		son_debug_print(node);
	}

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(sog->manager);

	/* create mapping from task_id and outfile cached_name to node */
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		if (node->outfile) {
			hash_table_insert(sog->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
	}

	/* add the parents' outfiles as inputs to the task */
	struct list *topo_order = get_topological_order(sog);
	LIST_ITERATE(topo_order, node)
	{
		struct strategic_orchestration_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->outfile) {
				vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			}
		}
	}

	/* initialize pending_parents for all nodes */
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		struct strategic_orchestration_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->pending_parents) {
				/* Use parent_node->node_key to ensure pointer consistency */
				set_insert(node->pending_parents, parent_node);
			}
		}
	}

	/* enqueue those without dependencies */
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		if (!node->pending_parents || set_size(node->pending_parents) == 0) {
			submit_node_task(sog, node);
		}
	}

	/* calculate steps to inject failure */
	double next_failure_threshold = -1.0;
	if (sog->failure_injection_step_percent > 0) {
		next_failure_threshold = sog->failure_injection_step_percent / 100.0;
	}

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, sog->progress_bar_update_interval_sec);

	struct ProgressBarPart *regular_tasks_part = progress_bar_create_part("Regular", hash_table_size(sog->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, regular_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	int wait_timeout = 2;

	while (regular_tasks_part->current < regular_tasks_part->total) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(sog->manager, wait_timeout);
		progress_bar_set_part_total(pbar, recovery_tasks_part, sog->manager->num_submitted_recovery_tasks);
		if (task) {
			/* retrieve all possible tasks */
			wait_timeout = 0;

			/* get the original node by task id */
			struct strategic_orchestration_node *node = get_node_by_task(sog, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			/* in case of failure, resubmit this task */
			if (node->task->result != VINE_RESULT_SUCCESS || node->task->exit_code != 0) {
				if (node->retry_attempts_left <= 0) {
					debug(D_ERROR, "Task %d failed (result=%d, exit=%d). Node %s has no retries left. Aborting.", task->task_id, node->task->result, node->task->exit_code, node->node_key);
					sog_delete(sog);
					exit(1);
				}
				node->retry_attempts_left--;
				debug(D_VINE | D_NOTICE, "Task %d failed (result=%d, exit=%d). Retrying node %s (remaining=%d)...", task->task_id, node->task->result, node->task->exit_code, node->node_key, node->retry_attempts_left);
				vine_task_reset(node->task);
				submit_node_task(sog, node);
				continue;
			}

			/* if the outfile is set to save on the sharedfs, stat to get the size of the file */
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
				struct stat info;
				int result = stat(node->outfile_remote_name, &info);
				if (result < 0) {
					if (node->retry_attempts_left <= 0) {
						debug(D_ERROR, "Task %d succeeded but missing sharedfs output %s; no retries left for node %s. Aborting.", task->task_id, node->outfile_remote_name, node->node_key);
						sog_delete(sog);
						exit(1);
					}
					node->retry_attempts_left--;
					debug(D_VINE | D_NOTICE, "Task %d succeeded but missing sharedfs output %s; retrying node %s (remaining=%d)...", task->task_id, node->outfile_remote_name, node->node_key, node->retry_attempts_left);
					vine_task_reset(node->task);
					submit_node_task(sog, node);
					continue;
				}
				node->outfile_size_bytes = info.st_size;
				break;
			}
			case NODE_OUTFILE_TYPE_LOCAL:
			case NODE_OUTFILE_TYPE_TEMP:
				node->outfile_size_bytes = node->outfile->size;
				break;
			}
			debug(D_VINE, "Node %s completed with outfile %s size: %zu bytes", node->node_key, node->outfile_remote_name, node->outfile_size_bytes);

			/* mark the node as completed */
			node->completed = 1;

			/* prune nodes on task completion */
			prune_ancestors_of_node(sog, node);

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
			son_update_critical_path_time(node, task->time_workers_execute_last);

			/* mark this regular task as completed */
			progress_bar_update_part(pbar, regular_tasks_part, 1);

			/* inject failure */
			if (sog->failure_injection_step_percent > 0) {
				double progress = (double)regular_tasks_part->current / (double)regular_tasks_part->total;
				if (progress >= next_failure_threshold && evict_random_worker(sog->manager)) {
					debug(D_VINE, "evicted a worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += sog->failure_injection_step_percent / 100.0;
				}
			}

			/* enqueue the output file for replication */
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_TEMP:
				/* replicate the outfile of the temp node */
				vine_temp_replicate_file_later(sog->manager, node->outfile);
				break;
			case NODE_OUTFILE_TYPE_LOCAL:
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_unblocked_children(sog, node);
		} else {
			wait_timeout = 2;
			progress_bar_update_part(pbar, recovery_tasks_part, 0); // refresh the time and total for recovery tasks
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	double total_time_spent_on_unlink_local_files = 0;
	double total_time_spent_on_prune_ancestors_of_temp_node = 0;
	double total_time_spent_on_prune_ancestors_of_persisted_node = 0;
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
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

/**
 * Delete a strategic orchestration graph object.
 * @param sog Reference to the strategic orchestration graph object.
 */
void sog_delete(struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return;
	}

	char *node_key;
	struct strategic_orchestration_node *node;
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		if (node->infile) {
			vine_prune_file(sog->manager, node->infile);
			hash_table_remove(sog->manager->file_table, node->infile->cached_name);
		}
		if (node->outfile) {
			vine_prune_file(sog->manager, node->outfile);
			hash_table_remove(sog->outfile_cachename_to_node, node->outfile->cached_name);
			hash_table_remove(sog->manager->file_table, node->outfile->cached_name);
		}
		if (node->outfile_type == NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM) {
			unlink(node->outfile_remote_name);
		}
		son_delete(node);
	}

	free(sog->proxy_library_name);
	free(sog->proxy_function_name);

	hash_table_delete(sog->nodes);
	itable_delete(sog->task_id_to_node);
	hash_table_delete(sog->outfile_cachename_to_node);
	free(sog);
}
