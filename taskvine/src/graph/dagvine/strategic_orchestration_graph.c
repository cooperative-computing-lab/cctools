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

#include "strategic_orchestration_graph.h"
#include "vine_manager.h"
#include "vine_task_node.h"
#include "vine_worker_info.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "taskvine.h"


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
static double vine_task_node_calculate_priority(struct vine_task_node *node, task_priority_mode_t priority_mode)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();

	struct vine_task_node *parent_node;

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
static void submit_node_task(struct strategic_orchestration_graph *sog, struct vine_task_node *node)
{
	if (!sog || !node) {
		return;
	}

	/* calculate the priority of the node */
	double priority = vine_task_node_calculate_priority(node, sog->task_priority_mode);
	vine_task_set_priority(node->task, priority);
	/* submit the task to the manager */
	int task_id = vine_submit(sog->manager, node->task);
	/* insert the task id to the task id to node map */
	itable_insert(sog->task_id_to_node, task_id, node);

	return;
}

/**
 * Submit the children of a node if all its dependencies are resolved.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node Reference to the node object.
 */
static void submit_unblocked_children(struct strategic_orchestration_graph *sog, struct vine_task_node *node)
{
	if (!sog || !node) {
		return;
	}

	struct vine_task_node *child_node; 
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
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(sog->nodes, key, node)
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
		debug(D_ERROR, "Error: strategic orchestration graph contains cycles or is malformed.\n");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.\n", total_nodes, list_size(topo_order));

		HASH_TABLE_ITERATE(sog->nodes, key, node)
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
	struct vine_task_node *node;
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

/**
 * Compute the heavy score of a node in the strategic orchestration graph.
 * @param node Reference to the node object.
 * @return The heavy score.
 */
static double compute_node_heavy_score(struct vine_task_node *node)
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
static struct vine_task_node *get_node_by_task(struct strategic_orchestration_graph *sog, struct vine_task *task)
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

	} else if (strcmp(name, "target-results-dir") == 0) {
		if (sog->target_results_dir) {
			free(sog->target_results_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		sog->target_results_dir = xxstrdup(value);

	} else if (strcmp(name, "proxy-function-name") == 0) {
		if (sog->proxy_function_name) {
			free(sog->proxy_function_name);
		}
		sog->proxy_function_name = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		sog->prune_depth = atoi(value);
		
	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;

	}

	return 0;
}

void sog_set_proxy_library_name(struct strategic_orchestration_graph *sog, const char *proxy_library_name)
{
	if (!sog || !proxy_library_name) {
		return;
	}

	if (sog->proxy_library_name) {
		free(sog->proxy_library_name);
	}

	sog->proxy_library_name = xxstrdup(proxy_library_name);
}

/**
 * Get the proxy library name (Python-side), shared by all tasks.
 * @param sog Reference to the strategic orchestration graph object.
 * @return The library name.
 */
const char *sog_get_proxy_library_name(const struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return NULL;
	}

	return sog->proxy_library_name;
}

/**
 * Get the proxy function name (Python-side), shared by all tasks.
 * @param sog Reference to the strategic orchestration graph object.
 * @return The function name.
 */
const char *sog_get_proxy_function_name(const struct strategic_orchestration_graph *sog)
{
	if (!sog) {
		return NULL;
	}

	return sog->proxy_function_name;
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

	struct vine_task_node *node = hash_table_lookup(sog->nodes, node_key);
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

	struct vine_task_node *node = hash_table_lookup(sog->nodes, node_key);
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

/**
 * Compute the topology metrics of the strategic orchestration graph, including depth, height, upstream and downstream counts,
 * heavy scores, and weakly connected components.
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
 * @return A new node object.
 */
void sog_add_node(struct strategic_orchestration_graph *sog, const char *node_key)
{
	if (!sog || !node_key) {
		return;
	}

	/* if the node already exists, skip creating a new one */
	struct vine_task_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		node = vine_task_node_create(sog->manager,
				node_key,
				sog->proxy_library_name,
				sog->proxy_function_name,
				sog->target_results_dir,
				sog->prune_depth);

		if (!node) {
			debug(D_ERROR, "failed to create node %s", node_key);
			sog_delete(sog);
			exit(1);
		}

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
	struct strategic_orchestration_graph *sog = xxmalloc(sizeof(struct strategic_orchestration_graph));

	sog->manager = q;
	
	sog->nodes = hash_table_create(0, 0);
	sog->task_id_to_node = itable_create(0);
	sog->outfile_cachename_to_node = hash_table_create(0, 0);

	sog->target_results_dir = xxstrdup(sog->manager->runtime_directory);  // default to current working directory
	sog->proxy_library_name = NULL;
	sog->proxy_function_name = NULL;

	sog->prune_depth = 1;

	sog->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	sog->failure_injection_step_percent = -1.0;

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

	struct vine_task_node *parent_node = hash_table_lookup(sog->nodes, parent_key);
	struct vine_task_node *child_node = hash_table_lookup(sog->nodes, child_key);
	if (!parent_node) {
		debug(D_ERROR, "parent node %s not found", parent_key);
		char *node_key = NULL;
		struct vine_task_node *node;
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
 * Set the outfile of a node in the strategic orchestration graph.
 * This involves declaring the output file and adding it to the task.
 * @param sog Reference to the strategic orchestration graph object.
 * @param node_key Reference to the node key.
 * @param outfile_type Reference to the outfile type.
 * @param outfile_remote_name Reference to the outfile remote name that the task will generate remotely.
 */
void sog_set_node_outfile(struct strategic_orchestration_graph *sog, const char *node_key, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name)
{
	if (!sog || !node_key || !outfile_remote_name) {
		return;
	}

	struct vine_task_node *node = hash_table_lookup(sog->nodes, node_key);
	if (!node) {
		return;
	}

	vine_task_node_set_outfile(node, outfile_type, outfile_remote_name);

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
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(sog->nodes, node_key, node)
	{
		vine_task_node_print_info(node);
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
		struct vine_task_node *parent_node;
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
		struct vine_task_node *parent_node;
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
			struct vine_task_node *node = get_node_by_task(sog, task);
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
			case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
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
			case VINE_NODE_OUTFILE_TYPE_LOCAL:
			case VINE_NODE_OUTFILE_TYPE_TEMP:
				node->outfile_size_bytes = node->outfile->size;
				break;
			}
			debug(D_VINE, "Node %s completed with outfile %s size: %zu bytes", node->node_key, node->outfile_remote_name, node->outfile_size_bytes);

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
			if (sog->failure_injection_step_percent > 0) {
				double progress = (double)regular_tasks_part->current / (double)regular_tasks_part->total;
				if (progress >= next_failure_threshold && evict_random_worker(sog->manager)) {
					debug(D_VINE, "evicted a worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += sog->failure_injection_step_percent / 100.0;
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
	struct vine_task_node *node;
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
		vine_task_node_delete(node);
	}

	free(sog->proxy_library_name);
	free(sog->proxy_function_name);

	hash_table_delete(sog->nodes);
	itable_delete(sog->task_id_to_node);
	hash_table_delete(sog->outfile_cachename_to_node);
	free(sog);
}
