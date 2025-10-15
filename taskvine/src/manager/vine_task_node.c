#include "vine_task_node.h"
#include "vine_manager.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_task_graph.h"
#include "jx.h"
#include "jx_print.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "taskvine.h"
#include "timestamp.h"
#include "set.h"
#include "hash_table.h"
#include "debug.h"
#include "assert.h"
#include "vine_worker_info.h"
#include "vine_temp.h"
#include "random.h"

#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

/**
 * Compute a lexicographic priority score from the node key.
 * Used during topological sorting to break ties deterministically.
 * @param key Reference to the node key.
 * @return The lexical priority.
 */
double compute_lex_priority(const char *key)
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
 * Create a new node object.
 * @param manager Reference to the manager object.
 * @param node_key Reference to the node key.
 * @param proxy_library_name Reference to the proxy library name.
 * @param proxy_function_name Reference to the proxy function name.
 * @param staging_dir Reference to the staging directory.
 * @param prune_depth Reference to the prune depth.
 * @param priority_mode Reference to the priority mode.
 * @return A new node object.
 */
struct vine_task_node *vine_task_node_create(
		struct vine_manager *manager,
		const char *node_key,
		const char *proxy_library_name,
		const char *proxy_function_name,
		const char *staging_dir,
		int prune_depth,
		vine_task_node_priority_mode_t priority_mode)
{
	if (!manager || !node_key || !proxy_library_name || !proxy_function_name || !staging_dir) {
		return NULL;
	}

	struct vine_task_node *node = xxmalloc(sizeof(struct vine_task_node));

	node->manager = manager;
	node->node_key = xxstrdup(node_key);
	node->staging_dir = xxstrdup(staging_dir);

	node->priority_mode = priority_mode;
	node->prune_status = PRUNE_STATUS_NOT_PRUNED;
	node->parents = list_create();
	node->children = list_create();
	node->pending_parents = set_create(0);
	node->completed = 0;
	node->prune_depth = prune_depth;
	node->retry_attempts_left = 1;
	node->outfile_size_bytes = 0;

	node->depth = -1;
	node->height = -1;
	node->upstream_subgraph_size = -1;
	node->downstream_subgraph_size = -1;
	node->fan_in = -1;
	node->fan_out = -1;
	node->heavy_score = -1;

	node->time_spent_on_unlink_local_files = 0;
	node->time_spent_on_prune_ancestors_of_temp_node = 0;
	node->time_spent_on_prune_ancestors_of_persisted_node = 0;

	node->critical_time = -1;

	/* create the task */
	node->task = vine_task_create(proxy_function_name);
	vine_task_set_library_required(node->task, proxy_library_name);
	vine_task_addref(node->task);

	/* build JSON infile expected by library: {"fn_args": [key], "fn_kwargs": {}} */
	struct jx *event = jx_object(NULL);
	struct jx *args = jx_array(NULL);
	jx_array_append(args, jx_string(node->node_key));
	jx_insert(event, jx_string("fn_args"), args);
	jx_insert(event, jx_string("fn_kwargs"), jx_object(NULL));

	char *infile_content = jx_print_string(event);
	jx_delete(event);

	node->infile = vine_declare_buffer(node->manager, infile_content, strlen(infile_content), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(infile_content);
	vine_task_add_input(node->task, node->infile, "infile", VINE_TRANSFER_ALWAYS);

	return node;
}

/**
 * Set the outfile of a node in the task graph.
 * This involves declaring the output file and adding it to the task.
 * @param node Reference to the node object.
 * @param outfile_type Reference to the outfile type.
 * @param outfile_remote_name Reference to the outfile remote name.
 */
void vine_task_node_set_outfile(struct vine_task_node *node, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name)
{
	if (!node) {
		return;
	}

	assert(outfile_remote_name != NULL);

	node->outfile_type = outfile_type;
	node->outfile_remote_name = xxstrdup(outfile_remote_name);

	/* create the output file */
	switch (node->outfile_type) {
	case VINE_NODE_OUTFILE_TYPE_LOCAL: {
		char *local_output_dir = string_format("%s/outputs", node->staging_dir);
		if (mkdir(local_output_dir, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", local_output_dir, errno);
		}
		char *local_output_path = string_format("%s/%s", local_output_dir, node->outfile_remote_name);
		node->outfile = vine_declare_file(node->manager, local_output_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(local_output_dir);
		free(local_output_path);
		break;
	}
	case VINE_NODE_OUTFILE_TYPE_TEMP:
		node->outfile = vine_declare_temp(node->manager);
		break;
	case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		/* no explicit output file declaration needed */
		node->outfile = NULL;
		break;
	}
	if (node->outfile) {
		vine_task_add_output(node->task, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
	}

	return;
}

/**
 * Check if the outfile of a node is persisted.
 * A node is considered persisted if it has completed and 1) the outfile is written to the shared file system,
 * 2) the outfile is written to the local staging directory.
 * @param node Reference to the node object.
 * @return 1 if the outfile is persisted, 0 otherwise.
 */
static int _node_outfile_is_persisted(struct vine_task_node *node)
{
	if (!node) {
		return 0;
	}

	/* if the node is not completed then the outfile is definitely not persisted */
	if (!node->completed) {
		return 0;
	}

	switch (node->outfile_type) {
	case VINE_NODE_OUTFILE_TYPE_LOCAL:
		return 1;
	case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		return 1;
	case VINE_NODE_OUTFILE_TYPE_TEMP:
		return 0;
	}

	return 0;
}

/**
 * Calculate the priority of a node given the priority mode.
 * @param node Reference to the node object.
 * @return The priority.
 */
double vine_task_node_calculate_priority(struct vine_task_node *node)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();

	struct vine_task_node *parent_node;

	switch (node->priority_mode) {
	case VINE_TASK_PRIORITY_MODE_RANDOM:
		priority = random_double();
		break;
	case VINE_TASK_PRIORITY_MODE_DEPTH_FIRST:
		priority = (double)node->depth;
		break;
	case VINE_TASK_PRIORITY_MODE_BREADTH_FIRST:
		priority = -(double)node->depth;
		break;
	case VINE_TASK_PRIORITY_MODE_FIFO:
		priority = -(double)current_time;
		break;
	case VINE_TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case VINE_TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->outfile) {
				continue;
			}
			priority += (double)vine_file_size(parent_node->outfile);
		}
		break;
	case VINE_TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
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
 * Update the critical time of a node.
 * @param node Reference to the node object.
 * @param execution_time Reference to the execution time of the node.
 */
void vine_task_node_update_critical_time(struct vine_task_node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_time = 0;
	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->critical_time > max_parent_critical_time) {
			max_parent_critical_time = parent_node->critical_time;
		}
	}
	node->critical_time = max_parent_critical_time + execution_time;
}

/**
 * The dfs helper function for finding parents in a specific depth.
 * @param node Reference to the node object.
 * @param remaining_depth Reference to the remaining depth.
 * @param result Reference to the result list.
 * @param visited Reference to the visited set.
 */
static void _find_parents_dfs(struct vine_task_node *node, int remaining_depth, struct list *result, struct set *visited)
{
	if (!node || set_lookup(visited, node)) {
		return;
	}

	set_insert(visited, node);
	if (remaining_depth == 0) {
		list_push_tail(result, node);
		return;
	}
	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		_find_parents_dfs(parent_node, remaining_depth - 1, result, visited);
	}
}

/**
 * Find all parents in a specific depth of the node.
 * @param node Reference to the node object.
 * @param depth Reference to the depth.
 * @return The list of parents.
 */
static struct list *_find_parents_in_depth(struct vine_task_node *node, int depth)
{
	if (!node || depth < 0) {
		return NULL;
	}

	struct list *result = list_create();

	struct set *visited = set_create(0);
	_find_parents_dfs(node, depth, result, visited);
	set_delete(visited);

	return result;
}

/**
 * Prune the ancestors of a temp node. This is only used for temp nodes that produce temp files.
 * All ancestors of this node we consider here are temp nodes, we can not safely prune those stored in the shared file system
 * because temp nodes are not considered safe enough to trigger the deletion of upstream persisted files.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_temp_node(struct vine_task_node *node)
{
	if (!node || !node->outfile || node->prune_depth <= 0) {
		return 0;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	struct list *parents = _find_parents_in_depth(node, node->prune_depth);

	struct vine_task_node *parent_node;
	LIST_ITERATE(parents, parent_node)
	{
		/* skip if the parent produces a shared file system file */
		if (!parent_node->outfile) {
			continue;
		}
		/* skip if the parent produces a non-temp file */
		if (parent_node->outfile->type != VINE_TEMP) {
			continue;
		}

		/* a file is prunable if its outfile is no longer needed by any child node:
		 * 1. it has no pending dependents
		 * 2. all completed dependents have also completed their corresponding recovery tasks, if any */
		int all_children_completed = 1;
		struct vine_task_node *child_node;
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

		pruned_replica_count += vine_prune_file(node->manager, parent_node->outfile);
		/* this parent is pruned because a successor that produces a temp file is completed, it is unsafe because the
		 * manager may submit a recovery task to bring it back in case of worker failures. */
		parent_node->prune_status = PRUNE_STATUS_UNSAFE;
	}

	list_delete(parents);

	node->time_spent_on_prune_ancestors_of_temp_node += timestamp_get() - start_time;

	return pruned_replica_count;
}

/**
 * Find all safe ancestors of a node.
 * @param start_node Reference to the start node.
 * @return The set of safe ancestors.
 */
static struct set *_find_safe_ancestors(struct vine_task_node *start_node)
{
	if (!start_node) {
		return NULL;
	}

	struct set *visited_nodes = set_create(0);
	struct list *bfs_nodes = list_create();

	list_push_tail(bfs_nodes, start_node);
	set_insert(visited_nodes, start_node);

	while (list_size(bfs_nodes) > 0) {
		struct vine_task_node *current = list_pop_head(bfs_nodes);

		struct vine_task_node *parent_node;
		LIST_ITERATE(current->parents, parent_node)
		{
			if (set_lookup(visited_nodes, parent_node)) {
				continue;
			}

			/* shortcut if this parent was already safely pruned */
			if (parent_node->prune_status == PRUNE_STATUS_SAFE) {
				continue;
			}

			/* check if all children are safe */
			int all_children_safe = 1;
			struct vine_task_node *child_node;
			LIST_ITERATE(parent_node->children, child_node)
			{
				/* shortcut if this child is part of the recovery subgraph */
				if (set_lookup(visited_nodes, child_node)) {
					continue;
				}
				/* shortcut if this outside child is not persisted */
				if (!_node_outfile_is_persisted(child_node)) {
					all_children_safe = 0;
					break;
				}
				/* shortcut if this outside child is unsafely pruned */
				if (child_node->prune_status == PRUNE_STATUS_UNSAFE) {
					all_children_safe = 0;
					break;
				}
			}

			if (!all_children_safe) {
				continue;
			}

			set_insert(visited_nodes, parent_node);
			list_push_tail(bfs_nodes, parent_node);
		}
	}

	list_delete(bfs_nodes);
	set_remove(visited_nodes, start_node);

	return visited_nodes;
}

/**
 * Prune the ancestors of a persisted node. This is only used for persisted nodes that produce persisted files.
 * All ancestors we consider here include both temp nodes and persisted nodes, becasue data written to shared file system
 * is safe and can definitely trigger upstream data redundancy to be released.
 * @param node Reference to the node object.
 * @return The number of pruned replicas.
 */
static int prune_ancestors_of_persisted_node(struct vine_task_node *node)
{
	if (!node) {
		return 0;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	/* find all safe ancestors */
	struct set *safe_ancestors = _find_safe_ancestors(node);
	if (!safe_ancestors) {
		return 0;
	}

	/* prune all safe ancestors */
	struct vine_task_node *ancestor_node;
	SET_ITERATE(safe_ancestors, ancestor_node)
	{
		/* unlink the shared file system file */
		if (!ancestor_node->outfile) {
			timestamp_t unlink_start = timestamp_get();
			if (ancestor_node->outfile_remote_name) {
				unlink(ancestor_node->outfile_remote_name); // system call
			}
			node->time_spent_on_unlink_local_files += timestamp_get() - unlink_start;
			debug(D_VINE, "unlinked %s size: %zu bytes, time: %" PRIu64, ancestor_node->outfile_remote_name ? ancestor_node->outfile_remote_name : "(null)", ancestor_node->outfile_size_bytes, (uint64_t)node->time_spent_on_unlink_local_files);
		} else {
			switch (ancestor_node->outfile->type) {
			case VINE_FILE:
				/* do not prune the staging dir file */
				break;
			case VINE_TEMP:
				/* prune the temp file */
				vine_prune_file(node->manager, ancestor_node->outfile);
				break;
			default:
				debug(D_ERROR, "unsupported outfile type: %d", ancestor_node->outfile->type);
				break;
			}
		}
		ancestor_node->prune_status = PRUNE_STATUS_SAFE;
		pruned_replica_count++;
	}

	set_delete(safe_ancestors);

	node->time_spent_on_prune_ancestors_of_persisted_node += timestamp_get() - start_time;

	return pruned_replica_count;
}

/**
 * Submit a node to the taskvine manager.
 * @param node Reference to the node object.
 * @return The task id.
 */
int vine_task_node_submit(struct vine_task_node *node)
{
	if (!node) {
		return -1;
	}

	double priority = vine_task_node_calculate_priority(node);
	vine_task_set_priority(node->task, priority);

	return vine_submit(node->manager, node->task);
}

/**
 * Print the info of the node.
 * @param node Reference to the node object.
 */
void vine_task_node_print_info(struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	if (!node->task) {
		debug(D_ERROR, "node %s has no task", node->node_key);
		return;
	}

	debug(D_VINE, "---------------- Node Info ----------------");
	debug(D_VINE, "key: %s", node->node_key);
	debug(D_VINE, "task_id: %d", node->task->task_id);
	debug(D_VINE, "staging_dir: %s", node->staging_dir ? node->staging_dir : "(null)");
	debug(D_VINE, "depth: %d", node->depth);
	debug(D_VINE, "height: %d", node->height);
	debug(D_VINE, "prune_depth: %d", node->prune_depth);
	debug(D_VINE, "priority_mode: %d", node->priority_mode);

	if (node->outfile_remote_name) {
		debug(D_VINE, "outfile_remote_name: %s", node->outfile_remote_name);
	}

	if (node->outfile) {
		const char *type_str = "UNKNOWN";
		switch (node->outfile->type) {
		case VINE_FILE:
			type_str = "VINE_FILE";
			break;
		case VINE_TEMP:
			type_str = "VINE_TEMP";
			break;
		case VINE_URL:
			type_str = "VINE_URL";
			break;
		case VINE_BUFFER:
			type_str = "VINE_BUFFER";
			break;
		case VINE_MINI_TASK:
			type_str = "VINE_MINI_TASK";
			break;
		}
		debug(D_VINE, "outfile_type: %s", type_str);
		debug(D_VINE, "outfile_cached_name: %s", node->outfile->cached_name ? node->outfile->cached_name : "(null)");
	} else {
		debug(D_VINE, "outfile_type: SHARED_FILE_SYSTEM or none");
	}

	/* print parent and child node keys */
	char *parent_keys = NULL;
	struct vine_task_node *p;
	LIST_ITERATE(node->parents, p)
	{
		if (!parent_keys) {
			parent_keys = string_format("%s", p->node_key);
		} else {
			char *tmp = string_format("%s, %s", parent_keys, p->node_key);
			free(parent_keys);
			parent_keys = tmp;
		}
	}

	char *child_keys = NULL;
	struct vine_task_node *c;
	LIST_ITERATE(node->children, c)
	{
		if (!child_keys) {
			child_keys = string_format("%s", c->node_key);
		} else {
			char *tmp = string_format("%s, %s", child_keys, c->node_key);
			free(child_keys);
			child_keys = tmp;
		}
	}

	debug(D_VINE, "parents: %s", parent_keys ? parent_keys : "(none)");
	debug(D_VINE, "children: %s", child_keys ? child_keys : "(none)");

	free(parent_keys);
	free(child_keys);

	debug(D_VINE, "-------------------------------------------");
}

/**
 * Prune the ancestors of a node when it is completed.
 * @param node Reference to the node object.
 */
void vine_task_node_prune_ancestors(struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	timestamp_t start_time = timestamp_get();

	int pruned_replica_count = 0;

	if (_node_outfile_is_persisted(node)) {
		pruned_replica_count = prune_ancestors_of_persisted_node(node);
	} else {
		pruned_replica_count = prune_ancestors_of_temp_node(node);
	}

	timestamp_t elapsed_time = timestamp_get() - start_time;

	debug(D_VINE, "pruned %d ancestors of node %s in %.6f seconds", pruned_replica_count, node->node_key, elapsed_time / 1000000.0);
}

/**
 * Replicate the outfile of a node if it is a temp file.
 * @param node Reference to the node object.
 */
void vine_task_node_replicate_outfile(struct vine_task_node *node)
{
	if (!node || !node->outfile) {
		return;
	}

	if (node->outfile->type != VINE_TEMP) {
		return;
	}

	vine_temp_replicate_file_later(node->manager, node->outfile);
}

/**
 * Delete the node and all of its associated resources.
 * @param node Reference to the node object.
 */
void vine_task_node_delete(struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	if (node->node_key) {
		free(node->node_key);
	}
	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}
	if (node->staging_dir) {
		free(node->staging_dir);
	}

	vine_task_delete(node->task);
	node->task = NULL;

	if (node->infile) {
		vine_file_delete(node->infile);
		node->infile = NULL;
	}
	if (node->outfile) {
		vine_file_delete(node->outfile);
		node->outfile = NULL;
	}

	list_delete(node->parents);
	list_delete(node->children);

	if (node->pending_parents) {
		set_delete(node->pending_parents);
	}
	free(node);
}