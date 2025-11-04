#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
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

#include "vine_node.h"
#include "vine_graph.h"
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
 * Calculate the priority of a node given the priority mode.
 * @param node Reference to the node object.
 * @param priority_mode Reference to the priority mode.
 * @return The priority.
 */
static double calculate_task_priority(struct vine_node *node, task_priority_mode_t priority_mode)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();

	struct vine_node *parent_node;

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
 * Submit a node to the TaskVine manager via the vine graph.
 * @param vg Reference to the vine graph.
 * @param node Reference to the node.
 */
static void submit_node_task(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	/* calculate the priority of the node */
	double priority = calculate_task_priority(node, vg->task_priority_mode);
	vine_task_set_priority(node->task, priority);

	/* submit the task to the manager */
	timestamp_t time_start = timestamp_get();
	int task_id = vine_submit(vg->manager, node->task);
	node->submission_time = timestamp_get() - time_start;

	/* insert the task id to the task id to node map */
	itable_insert(vg->task_id_to_node, (uint64_t)task_id, node);

	debug(D_VINE, "submitted node %" PRIu64 " with task id %d", node->node_id, task_id);

	return;
}

/**
 * Submit the children of a node once every dependency has completed.
 * @param vg Reference to the vine graph.
 * @param node Reference to the node.
 */
static void submit_unblocked_children(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	struct vine_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		/* Remove this parent from the child's pending set if it exists */
		if (child_node->pending_parents) {
			/* Assert that this parent is indeed pending for the child */
			if (child_node->pending_parents && set_lookup(child_node->pending_parents, node)) {
				set_remove(child_node->pending_parents, node);
			} else {
				debug(D_ERROR, "inconsistent pending set: child=%" PRIu64 " missing parent=%" PRIu64, child_node->node_id, node->node_id);
			}
		}

		/* If no more parents are pending, submit the child */
		if (!child_node->pending_parents || set_size(child_node->pending_parents) == 0) {
			submit_node_task(vg, child_node);
		}
	}

	return;
}

/**
 * Compute a topological ordering of the vine graph.
 * Call only after all nodes, edges, and metrics have been populated.
 * @param vg Reference to the vine graph.
 * @return Nodes in topological order.
 */
static struct list *get_topological_order(struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	int total_nodes = itable_size(vg->nodes);
	struct list *topo_order = list_create();
	struct itable *in_degree_map = itable_create(0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		int deg = list_size(node->parents);
		itable_insert(in_degree_map, nid, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, -(double)node->node_id);
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct vine_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct vine_node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, child->node_id);
			int deg = (int)raw_deg - 1;
			itable_insert(in_degree_map, child->node_id, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, -(double)child->node_id);
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_ERROR, "Error: vine graph contains cycles or is malformed.");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.", total_nodes, list_size(topo_order));

		uint64_t id;
		ITABLE_ITERATE(vg->nodes, id, node)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, id);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %" PRIu64 " has in-degree %d. Parents:", id, deg);
				struct vine_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %" PRIu64, p->node_id);
				}
			}
		}

		list_delete(topo_order);
		itable_delete(in_degree_map);
		priority_queue_delete(pq);
		exit(1);
	}

	itable_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

/**
 * Extract weakly connected components of the vine graph.
 * Currently used for debugging and instrumentation only.
 * @param vg Reference to the vine graph.
 * @return List of weakly connected components.
 */
static struct list *extract_weakly_connected_components(struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
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
			struct vine_node *curr = list_pop_head(queue);

			struct vine_node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct vine_node *c;
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
 * Compute the heavy score of a node in the vine graph.
 * @param node Reference to the node.
 * @return Heavy score.
 */
static double compute_node_heavy_score(struct vine_node *node)
{
	if (!node) {
		return 0;
	}

	double up_score = node->depth * node->upstream_subgraph_size * node->fan_in;
	double down_score = node->height * node->downstream_subgraph_size * node->fan_out;

	return up_score / (down_score + 1);
}

/**
 * Map a TaskVine task back to its vine node.
 * @param vg Reference to the vine graph.
 * @param task Task reported by the manager.
 * @return Matching node.
 */
static struct vine_node *get_node_by_task(struct vine_graph *vg, struct vine_task *task)
{
	if (!vg || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		/* standard tasks are mapped directly to a node */
		return itable_lookup(vg->task_id_to_node, (uint64_t)task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/* note that recovery tasks are not mapped to any node but we still need the original node for pruning,
		 * so we look up the outfile of the task, then map it back to get the original node */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			uint64_t original_producer_task_id = mount->file->original_producer_task_id;
			if (original_producer_task_id > 0) {
				return itable_lookup(vg->task_id_to_node, original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);

	return NULL;
}

/**
 * Prune the ancestors of a persisted node. This is only used for persisted nodes that produce persisted files.
 * All ancestors we consider here include both temp nodes and persisted nodes, because data written to the shared file system
 * is safe and can definitely trigger upstream data redundancy to be released.
 * @param vg Reference to the vine graph.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_persisted_node(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return -1;
	}

	/* find all safe ancestors */
	struct set *safe_ancestors = vine_node_find_safe_ancestors(node);
	if (!safe_ancestors) {
		return 0;
	}

	int pruned_replica_count = 0;

	timestamp_t start_time = timestamp_get();

	/* prune all safe ancestors */
	struct vine_node *ancestor_node;
	SET_ITERATE(safe_ancestors, ancestor_node)
	{
		switch (ancestor_node->outfile_type) {
		case NODE_OUTFILE_TYPE_LOCAL:
			/* do not prune the local file */
			break;
		case NODE_OUTFILE_TYPE_TEMP:
			/* prune the temp file */
			vine_prune_file(vg->manager, ancestor_node->outfile);
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
 * @param vg Reference to the vine graph.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_temp_node(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node || !node->outfile || node->prune_depth <= 0) {
		return 0;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	struct list *parents = vine_node_find_parents_by_depth(node, node->prune_depth);

	struct vine_node *parent_node;
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
		struct vine_node *child_node;
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

		pruned_replica_count += vine_prune_file(vg->manager, parent_node->outfile);
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
static void prune_ancestors_of_node(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	/* do not prune if the node has not completed */
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
		pruned_replica_count = prune_ancestors_of_persisted_node(vg, node);
		break;
	case NODE_OUTFILE_TYPE_TEMP:
		/* Otherwise, if the node outfile is a temp file, we need to be careful about pruning, because temp files are prone
		 * to failures, while means they can be lost due to node evictions or failures. */
		pruned_replica_count = prune_ancestors_of_temp_node(vg, node);
		break;
	}

	timestamp_t elapsed_time = timestamp_get() - start_time;

	debug(D_VINE, "pruned %d ancestors of node %" PRIu64 " in %.6f seconds", pruned_replica_count, node->node_id, elapsed_time / 1000000.0);

	return;
}

/**
 * Print the time metrics of the vine graph to a csv file.
 * @param vg Reference to the vine graph.
 * @param filename Reference to the filename of the csv file.
 */
static void print_time_metrics(struct vine_graph *vg, const char *filename)
{
	if (!vg) {
		return;
	}

	/* first delete the file if it exists */
	if (access(filename, F_OK) != -1) {
		unlink(filename);
	}

	/* print the header as a csv file */
	FILE *fp = fopen(filename, "w");
	if (!fp) {
		debug(D_ERROR, "failed to open file %s", filename);
		return;
	}
	fprintf(fp, "node_id,submission_time_us,scheduling_time_us,commit_time_us,execution_time_us,retrieval_time_us,postprocessing_time_us\n");

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		fprintf(fp, "%" PRIu64 ",%lu,%lu,%lu,%lu,%lu,%lu\n", node->node_id, node->submission_time, node->scheduling_time, node->commit_time, node->execution_time, node->retrieval_time, node->postprocessing_time);
	}
	fclose(fp);

	return;
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/** Tune the vine graph.
 *@param vg Reference to the vine graph.
 *@param name Reference to the name of the parameter to tune.
 *@param value Reference to the value of the parameter to tune.
 *@return 0 on success, -1 on failure.
 */
int vine_graph_tune(struct vine_graph *vg, const char *name, const char *value)
{
	if (!vg || !name || !value) {
		return -1;
	}

	if (strcmp(name, "failure-injection-step-percent") == 0) {
		vg->failure_injection_step_percent = atof(value);

	} else if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "random") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_RANDOM;
		} else if (strcmp(value, "depth-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_DEPTH_FIRST;
		} else if (strcmp(value, "breadth-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_BREADTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_FIFO;
		} else if (strcmp(value, "lifo") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LIFO;
		} else if (strcmp(value, "largest-input-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "largest-storage-footprint-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST;
		} else {
			debug(D_ERROR, "invalid priority mode: %s", value);
			return -1;
		}

	} else if (strcmp(name, "output-dir") == 0) {
		if (vg->output_dir) {
			free(vg->output_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		vg->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		vg->prune_depth = atoi(value);

	} else if (strcmp(name, "checkpoint-fraction") == 0) {
		double fraction = atof(value);
		if (fraction < 0.0 || fraction > 1.0) {
			debug(D_ERROR, "invalid checkpoint fraction: %s (must be between 0.0 and 1.0)", value);
			return -1;
		}
		vg->checkpoint_fraction = fraction;

	} else if (strcmp(name, "checkpoint-dir") == 0) {
		if (vg->checkpoint_dir) {
			free(vg->checkpoint_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		vg->checkpoint_dir = xxstrdup(value);

	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double val = atof(value);
		vg->progress_bar_update_interval_sec = (val > 0.0) ? val : 0.1;

	} else if (strcmp(name, "time-metrics-filename") == 0) {
		if (strcmp(value, "0") == 0) {
			return 0;
		}

		if (vg->time_metrics_filename) {
			free(vg->time_metrics_filename);
		}

		vg->time_metrics_filename = xxstrdup(value);

		/** Extract parent directory inline **/
		const char *slash = strrchr(vg->time_metrics_filename, '/');
		if (slash) {
			size_t len = slash - vg->time_metrics_filename;
			char *parent = malloc(len + 1);
			memcpy(parent, vg->time_metrics_filename, len);
			parent[len] = '\0';

			/** Ensure the parent directory exists **/
			if (mkdir(parent, 0777) != 0 && errno != EEXIST) {
				debug(D_ERROR, "failed to mkdir %s (errno=%d)", parent, errno);
				free(parent);
				return -1;
			}
			free(parent);
		}

		/** Truncate or create the file **/
		FILE *fp = fopen(vg->time_metrics_filename, "w");
		if (!fp) {
			debug(D_ERROR, "failed to create file %s (errno=%d)", vg->time_metrics_filename, errno);
			return -1;
		}
		fclose(fp);

	} else if (strcmp(name, "enable-debug-log") == 0) {
		if (vg->enable_debug_log == 0) {
			return -1;
		}
		vg->enable_debug_log = (atoi(value) == 1) ? 1 : 0;
		if (vg->enable_debug_log == 0) {
			debug_flags_clear();
			debug_close();
		}

	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;
	}

	return 0;
}

/**
 * Get the outfile remote name of a node in the vine graph.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The outfile remote name.
 */
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return NULL;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		return NULL;
	}

	return node->outfile_remote_name;
}

/**
 * Get the proxy library name of the vine graph.
 * @param vg Reference to the vine graph.
 * @return The proxy library name.
 */
const char *vine_graph_get_proxy_library_name(const struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	return vg->proxy_library_name;
}

/**
 * Set the proxy function name of the vine graph.
 * @param vg Reference to the vine graph.
 * @param proxy_function_name Reference to the proxy function name.
 */
void vine_graph_set_proxy_function_name(struct vine_graph *vg, const char *proxy_function_name)
{
	if (!vg || !proxy_function_name) {
		return;
	}

	if (vg->proxy_function_name) {
		free(vg->proxy_function_name);
	}

	vg->proxy_function_name = xxstrdup(proxy_function_name);
}

/**
 * Get the heavy score of a node in the vine graph.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The heavy score.
 */
double vine_graph_get_node_heavy_score(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return -1;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		return -1;
	}

	return node->heavy_score;
}

/**
 * Get the local outfile source of a node in the vine graph, only valid for local output files.
 * The source of a local output file is the path on the local filesystem.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The local outfile source.
 */
const char *vine_graph_get_node_local_outfile_source(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return NULL;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}

	if (node->outfile_type != NODE_OUTFILE_TYPE_LOCAL) {
		debug(D_ERROR, "node %" PRIu64 " is not a local output file", node_id);
		exit(1);
	}

	return node->outfile->source;
}

/**
 * Compute the topology metrics of the vine graph, including depth, height, upstream and downstream counts,
 * heavy scores, and weakly connected components. Must be called after all nodes and dependencies are added.
 * @param vg Reference to the vine graph.
 */
void vine_graph_compute_topology_metrics(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(vg);
	if (!topo_order) {
		return;
	}

	struct vine_node *node;
	struct vine_node *parent_node;
	struct vine_node *child_node;

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
	struct itable *upstream_map = itable_create(0);
	struct itable *downstream_map = itable_create(0);
	uint64_t nid_tmp;
	ITABLE_ITERATE(vg->nodes, nid_tmp, node)
	{
		struct set *upstream = set_create(0);
		struct set *downstream = set_create(0);
		itable_insert(upstream_map, node->node_id, upstream);
		itable_insert(downstream_map, node->node_id, downstream);
	}
	LIST_ITERATE(topo_order, node)
	{
		struct set *upstream = itable_lookup(upstream_map, node->node_id);
		LIST_ITERATE(node->parents, parent_node)
		{
			struct set *parent_upstream = itable_lookup(upstream_map, parent_node->node_id);
			set_union(upstream, parent_upstream);
			set_insert(upstream, parent_node);
		}
	}
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		struct set *downstream = itable_lookup(downstream_map, node->node_id);
		LIST_ITERATE(node->children, child_node)
		{
			struct set *child_downstream = itable_lookup(downstream_map, child_node->node_id);
			set_union(downstream, child_downstream);
			set_insert(downstream, child_node);
		}
	}
	LIST_ITERATE(topo_order, node)
	{
		node->upstream_subgraph_size = set_size(itable_lookup(upstream_map, node->node_id));
		node->downstream_subgraph_size = set_size(itable_lookup(downstream_map, node->node_id));
		node->fan_in = list_size(node->parents);
		node->fan_out = list_size(node->children);
		set_delete(itable_lookup(upstream_map, node->node_id));
		set_delete(itable_lookup(downstream_map, node->node_id));
	}
	itable_delete(upstream_map);
	itable_delete(downstream_map);

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
		if (node->is_target) {
			total_target_nodes++;
		}
		priority_queue_push(sorted_nodes, node, node->heavy_score);
	}
	/* calculate the number of nodes to be checkpointed */
	int checkpoint_count = (int)((total_nodes - total_target_nodes) * vg->checkpoint_fraction);
	if (checkpoint_count < 0) {
		checkpoint_count = 0;
	}

	/* assign outfile types to each node */
	int assigned_checkpoint_count = 0;
	while ((node = priority_queue_pop(sorted_nodes))) {
		if (node->is_target) {
			/* declare the output file as a vine_file so that it can be retrieved by the manager as usual */
			node->outfile_type = NODE_OUTFILE_TYPE_LOCAL;
			char *local_outfile_path = string_format("%s/%s", vg->output_dir, node->outfile_remote_name);
			node->outfile = vine_declare_file(vg->manager, local_outfile_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
			free(local_outfile_path);
			continue;
		}
		if (assigned_checkpoint_count < checkpoint_count) {
			/* checkpointed files will be written directly to the shared file system, no need to manage them in the manager */
			node->outfile_type = NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM;
			char *shared_file_system_outfile_path = string_format("%s/%s", vg->checkpoint_dir, node->outfile_remote_name);
			free(node->outfile_remote_name);
			node->outfile_remote_name = shared_file_system_outfile_path;
			node->outfile = NULL;
			assigned_checkpoint_count++;
		} else {
			/* other nodes will be declared as temp files to leverage node-local storage */
			node->outfile_type = NODE_OUTFILE_TYPE_TEMP;
			node->outfile = vine_declare_temp(vg->manager);
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
	struct list *weakly_connected_components = extract_weakly_connected_components(vg);
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
 * Create a new node and track it in the vine graph.
 * @param vg Reference to the vine graph.
 * @return The auto-assigned node id.
 */
uint64_t vine_graph_add_node(struct vine_graph *vg)
{
	if (!vg) {
		return 0;
	}

	/* assign a new id based on current node count, ensure uniqueness */
	uint64_t candidate_id = itable_size(vg->nodes);
	candidate_id += 1;
	while (itable_lookup(vg->nodes, candidate_id)) {
		candidate_id++;
	}
	uint64_t node_id = candidate_id;

	/* create the backing node (defaults to non-target) */
	struct vine_node *node = vine_node_create(node_id);

	if (!node) {
		debug(D_ERROR, "failed to create node %" PRIu64, node_id);
		vine_graph_delete(vg);
		exit(1);
	}

	if (!vg->proxy_function_name) {
		debug(D_ERROR, "proxy function name is not set");
		vine_graph_delete(vg);
		exit(1);
	}

	if (!vg->proxy_library_name) {
		debug(D_ERROR, "proxy library name is not set");
		vine_graph_delete(vg);
		exit(1);
	}

	/* create node task */
	node->task = vine_task_create(vg->proxy_function_name);
	vine_task_set_library_required(node->task, vg->proxy_library_name);
	vine_task_addref(node->task);

	/* construct the task arguments and declare the infile */
	char *task_arguments = vine_node_construct_task_arguments(node);
	node->infile = vine_declare_buffer(vg->manager, task_arguments, strlen(task_arguments), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(task_arguments);
	vine_task_add_input(node->task, node->infile, "infile", VINE_TRANSFER_ALWAYS);

	/* initialize the pruning depth of each node, currently statically set to the global prune depth */
	node->prune_depth = vg->prune_depth;

	itable_insert(vg->nodes, node_id, node);

	return node_id;
}

/**
 * Mark a node as a retrieval target.
 */
void vine_graph_set_target(struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return;
	}
	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}
	node->is_target = 1;
}

/**
 * Create a new vine graph and bind a manager to it.
 * @param q Reference to the manager object.
 * @return A new vine graph instance.
 */
struct vine_graph *vine_graph_create(struct vine_manager *q)
{
	if (!q) {
		return NULL;
	}

	struct vine_graph *vg = xxmalloc(sizeof(struct vine_graph));

	vg->manager = q;

	vg->checkpoint_dir = xxstrdup(vg->manager->runtime_directory); // default to current working directory
	vg->output_dir = xxstrdup(vg->manager->runtime_directory);     // default to current working directory

	vg->nodes = itable_create(0);
	vg->task_id_to_node = itable_create(0);
	vg->outfile_cachename_to_node = hash_table_create(0, 0);

	cctools_uuid_t proxy_library_name_id;
	cctools_uuid_create(&proxy_library_name_id);
	vg->proxy_library_name = xxstrdup(proxy_library_name_id.str);

	vg->proxy_function_name = NULL;

	vg->prune_depth = 1;

	vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	vg->failure_injection_step_percent = -1.0;

	vg->progress_bar_update_interval_sec = 0.1;

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", vg->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	vg->time_metrics_filename = NULL;

	vg->enable_debug_log = 1;

	return vg;
}

/**
 * Add a dependency between two nodes in the vine graph. Note that the input-output file relationship
 * is not handled here, because their file names might not have been determined yet.
 * @param vg Reference to the vine graph.
 * @param parent_id Reference to the parent node id.
 * @param child_id Reference to the child node id.
 */
void vine_graph_add_dependency(struct vine_graph *vg, uint64_t parent_id, uint64_t child_id)
{
	if (!vg) {
		return;
	}

	struct vine_node *parent_node = itable_lookup(vg->nodes, parent_id);
	struct vine_node *child_node = itable_lookup(vg->nodes, child_id);
	if (!parent_node) {
		debug(D_ERROR, "parent node %" PRIu64 " not found", parent_id);
		uint64_t nid;
		struct vine_node *node;
		printf("parent_ids:\n");
		ITABLE_ITERATE(vg->nodes, nid, node)
		{
			printf("  %" PRIu64 "\n", node->node_id);
		}
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %" PRIu64 " not found", child_id);
		exit(1);
	}

	list_push_tail(child_node->parents, parent_node);
	list_push_tail(parent_node->children, child_node);

	return;
}

/**
 * Execute the vine graph. This must be called after all nodes and dependencies are added and the topology metrics are computed.
 * @param vg Reference to the vine graph.
 */
void vine_graph_execute(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	signal(SIGINT, handle_sigint);

	debug(D_VINE, "start executing vine graph");

	/* print the info of all nodes */
	uint64_t nid_iter;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid_iter, node)
	{
		vine_node_debug_print(node);
	}

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(vg->manager);

	/* create mappings from task IDs and outfile cache names to nodes */
	ITABLE_ITERATE(vg->nodes, nid_iter, node)
	{
		if (node->outfile) {
			hash_table_insert(vg->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
	}

	/* add the parents' outfiles as inputs to the task */
	struct list *topo_order = get_topological_order(vg);
	LIST_ITERATE(topo_order, node)
	{
		struct vine_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->outfile) {
				vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			}
		}
	}

	/* initialize pending_parents for all nodes */
	ITABLE_ITERATE(vg->nodes, nid_iter, node)
	{
		struct vine_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->pending_parents) {
				/* Use parent pointer to ensure pointer consistency */
				set_insert(node->pending_parents, parent_node);
			}
		}
	}

	/* enqueue those without dependencies */
	ITABLE_ITERATE(vg->nodes, nid_iter, node)
	{
		if (!node->pending_parents || set_size(node->pending_parents) == 0) {
			submit_node_task(vg, node);
		}
	}

	/* calculate steps to inject failure */
	double next_failure_threshold = -1.0;
	if (vg->failure_injection_step_percent > 0) {
		next_failure_threshold = vg->failure_injection_step_percent / 100.0;
	}

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, vg->progress_bar_update_interval_sec);

	struct ProgressBarPart *regular_tasks_part = progress_bar_create_part("Regular", itable_size(vg->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, regular_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	int wait_timeout = 1;

	while (regular_tasks_part->current < regular_tasks_part->total) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(vg->manager, wait_timeout);
		progress_bar_set_part_total(pbar, recovery_tasks_part, vg->manager->num_submitted_recovery_tasks);
		if (task) {
			/* retrieve all possible tasks */
			wait_timeout = 0;

			timestamp_t time_when_postprocessing_start = timestamp_get();

			/* get the original node by task id */
			struct vine_node *node = get_node_by_task(vg, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			/* in case of failure, resubmit this task */
			if (node->task->result != VINE_RESULT_SUCCESS || node->task->exit_code != 0) {
				if (node->retry_attempts_left <= 0) {
					debug(D_ERROR, "Task %d failed (result=%d, exit=%d). Node %" PRIu64 " has no retries left. Aborting.", task->task_id, node->task->result, node->task->exit_code, node->node_id);
					vine_graph_delete(vg);
					exit(1);
				}
				node->retry_attempts_left--;
				debug(D_VINE | D_NOTICE, "Task %d failed (result=%d, exit=%d). Retrying node %" PRIu64 " (remaining=%d)...", task->task_id, node->task->result, node->task->exit_code, node->node_id, node->retry_attempts_left);
				vine_task_reset(node->task);
				submit_node_task(vg, node);
				continue;
			}

			/* if the outfile is set to save on the sharedfs, stat to get the size of the file */
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
				struct stat info;
				int result = stat(node->outfile_remote_name, &info);
				if (result < 0) {
					if (node->retry_attempts_left <= 0) {
						debug(D_ERROR, "Task %d succeeded but missing sharedfs output %s; no retries left for node %" PRIu64 ". Aborting.", task->task_id, node->outfile_remote_name, node->node_id);
						vine_graph_delete(vg);
						exit(1);
					}
					node->retry_attempts_left--;
					debug(D_VINE | D_NOTICE, "Task %d succeeded but missing sharedfs output %s; retrying node %" PRIu64 " (remaining=%d)...", task->task_id, node->outfile_remote_name, node->node_id, node->retry_attempts_left);
					vine_task_reset(node->task);
					submit_node_task(vg, node);
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
			debug(D_VINE, "Node %" PRIu64 " completed with outfile %s size: %zu bytes", node->node_id, node->outfile_remote_name, node->outfile_size_bytes);

			/* mark the node as completed */
			node->completed = 1;
			node->scheduling_time = task->time_when_scheduling_end - task->time_when_scheduling_start;
			node->commit_time = task->time_when_commit_end - task->time_when_commit_start;
			node->execution_time = task->time_workers_execute_last;
			node->retrieval_time = task->time_when_get_result_end - task->time_when_get_result_start;

			/* prune nodes on task completion */
			prune_ancestors_of_node(vg, node);

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
			vine_node_update_critical_path_time(node, node->execution_time);

			/* mark this regular task as completed */
			progress_bar_update_part(pbar, regular_tasks_part, 1);

			/* inject failure */
			if (vg->failure_injection_step_percent > 0) {
				double progress = (double)regular_tasks_part->current / (double)regular_tasks_part->total;
				if (progress >= next_failure_threshold && evict_random_worker(vg->manager)) {
					debug(D_VINE, "evicted a worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += vg->failure_injection_step_percent / 100.0;
				}
			}

			/* enqueue the output file for replication */
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_TEMP:
				/* replicate the outfile of the temp node */
				vine_temp_replicate_file_later(vg->manager, node->outfile);
				break;
			case NODE_OUTFILE_TYPE_LOCAL:
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_unblocked_children(vg, node);

			timestamp_t time_when_postprocessing_end = timestamp_get();
			node->postprocessing_time = time_when_postprocessing_end - time_when_postprocessing_start;
		} else {
			wait_timeout = 1;
			progress_bar_update_part(pbar, recovery_tasks_part, 0); // refresh the time and total for recovery tasks
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	double total_time_spent_on_unlink_local_files = 0;
	double total_time_spent_on_prune_ancestors_of_temp_node = 0;
	double total_time_spent_on_prune_ancestors_of_persisted_node = 0;
	ITABLE_ITERATE(vg->nodes, nid_iter, node)
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

	if (vg->time_metrics_filename) {
		print_time_metrics(vg, vg->time_metrics_filename);
	}

	return;
}

/**
 * Delete a vine graph instance.
 * @param vg Reference to the vine graph.
 */
void vine_graph_delete(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		if (node->infile) {
			vine_prune_file(vg->manager, node->infile);
			hash_table_remove(vg->manager->file_table, node->infile->cached_name);
		}
		if (node->outfile) {
			vine_prune_file(vg->manager, node->outfile);
			hash_table_remove(vg->outfile_cachename_to_node, node->outfile->cached_name);
			hash_table_remove(vg->manager->file_table, node->outfile->cached_name);
		}
		if (node->outfile_type == NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM) {
			unlink(node->outfile_remote_name);
		}
		vine_node_delete(node);
	}

	free(vg->proxy_library_name);
	free(vg->proxy_function_name);

	itable_delete(vg->nodes);
	itable_delete(vg->task_id_to_node);
	hash_table_delete(vg->outfile_cachename_to_node);
	free(vg);
}
