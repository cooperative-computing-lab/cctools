#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "xxmalloc.h"
#include "stringtools.h"
#include "timestamp.h"
#include "set.h"
#include "hash_table.h"
#include "debug.h"
#include "random.h"
#include "uuid.h"

#include "vine_file.h"
#include "vine_task.h"
#include "vine_worker_info.h"
#include "vine_temp.h"
#include "vine_node.h"
#include "taskvine.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
 * Check if the outfile of a node is persisted.
 * A node is considered persisted if it has completed and 1) the outfile is written to the shared file system,
 * 2) the outfile is written to the local staging directory.
 * @param node Reference to the node object.
 * @return 1 if the outfile is persisted, 0 otherwise.
 */
static int node_outfile_has_been_persisted(struct vine_node *node)
{
	if (!node) {
		return 0;
	}

	/* if the node is not completed then the outfile is definitely not persisted */
	if (!node->completed) {
		return 0;
	}

	switch (node->outfile_type) {
	case NODE_OUTFILE_TYPE_LOCAL:
		return 1;
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		return 1;
	case NODE_OUTFILE_TYPE_TEMP:
		return 0;
	}

	return 0;
}

/**
 * Update the critical path time of a node.
 * @param node Reference to the node object.
 * @param execution_time Reference to the execution time of the node.
 */
void vine_node_update_critical_path_time(struct vine_node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_path_time = 0;
	struct vine_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->critical_path_time > max_parent_critical_path_time) {
			max_parent_critical_path_time = parent_node->critical_path_time;
		}
	}
	node->critical_path_time = max_parent_critical_path_time + execution_time;
}

/**
 * The dfs helper function for finding parents in a specific depth.
 * @param node Reference to the node object.
 * @param remaining_depth Reference to the remaining depth.
 * @param result Reference to the result list.
 * @param visited Reference to the visited set.
 */
static void find_parents_dfs(struct vine_node *node, int remaining_depth, struct list *result, struct set *visited)
{
	if (!node || set_lookup(visited, node)) {
		return;
	}

	set_insert(visited, node);
	if (remaining_depth == 0) {
		list_push_tail(result, node);
		return;
	}
	struct vine_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		find_parents_dfs(parent_node, remaining_depth - 1, result, visited);
	}
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/**
 * Create a new vine node owned by the C-side graph.
 * @param node_id Graph-assigned identifier that keeps C and Python in sync.
 * @return Newly allocated vine node.
 */
struct vine_node *vine_node_create(uint64_t node_id)
{
	struct vine_node *node = xxmalloc(sizeof(struct vine_node));

	node->is_target = 0;
	node->node_id = node_id;

	node->outfile_remote_name = string_format("outfile_node_%" PRIu64, node->node_id);

	node->prune_status = PRUNE_STATUS_NOT_PRUNED;
	node->parents = list_create();
	node->children = list_create();
	node->remaining_parents_count = 0;
	node->fired_parents = NULL;
	node->completed = 0;
	node->prune_depth = 0;
	node->outfile_size_bytes = 0;
	node->retry_attempts_left = 0;
	node->in_resubmit_queue = 0;

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

	node->submission_time = 0;
	node->scheduling_time = 0;
	node->commit_time = 0;
	node->execution_time = 0;
	node->retrieval_time = 0;
	node->postprocessing_time = 0;

	node->critical_path_time = -1;
	node->last_failure_time = 0;

	return node;
}

/**
 * Construct the task arguments for the node.
 * @param node Reference to the node object.
 * @return The task arguments in JSON format: {"fn_args": [node_id], "fn_kwargs": {}}.
 */
char *vine_node_construct_task_arguments(struct vine_node *node)
{
	if (!node) {
		return NULL;
	}
	return string_format("{\"fn_args\":[%" PRIu64 "],\"fn_kwargs\":{}}", node->node_id);
}

/**
 * Find all parents in a specific depth of the node.
 * @param node Reference to the node object.
 * @param depth Reference to the depth.
 * @return The list of parents.
 */
struct list *vine_node_find_parents_by_depth(struct vine_node *node, int depth)
{
	if (!node || depth < 0) {
		return NULL;
	}

	struct list *result = list_create();

	struct set *visited = set_create(0);
	find_parents_dfs(node, depth, result, visited);
	set_delete(visited);

	return result;
}

/**
 * Perform a reverse BFS traversal to identify all ancestors of a given node
 * whose outputs can be safely pruned.
 *
 * A parent node is considered "safe" if:
 *   1. All of its child nodes are either:
 *        - already persisted (their outputs are stored in a reliable location), or
 *        - already marked as safely pruned.
 *   2. None of its child nodes remain in an unsafe or incomplete state.
 *
 * This function starts from the given node and iteratively walks up the DAG,
 * collecting all such "safe" ancestors into a set. Nodes that have already
 * been marked as PRUNE_STATUS_SAFE are skipped early.
 *
 * The returned set contains all ancestors that can be safely pruned once the
 * current node’s output has been persisted.
 *
 * @param start_node  The node from which to begin the reverse search.
 * @return A set of ancestor nodes that are safe to prune (excluding start_node).
 */
struct set *vine_node_find_safe_ancestors(struct vine_node *start_node)
{
	if (!start_node) {
		return NULL;
	}

	struct set *visited_nodes = set_create(0);
	struct set *safe_ancestors = set_create(0);

	struct list *queue = list_create();

	list_push_tail(queue, start_node);
	set_insert(visited_nodes, start_node);

	while (list_size(queue) > 0) {
		struct vine_node *current_node = list_pop_head(queue);
		struct vine_node *parent_node;

		LIST_ITERATE(current_node->parents, parent_node)
		{
			if (set_lookup(visited_nodes, parent_node)) {
				continue;
			}

			set_insert(visited_nodes, parent_node);

			/* shortcut if this parent has already been marked as safely pruned */
			if (parent_node->prune_status == PRUNE_STATUS_SAFE) {
				continue;
			}

			/* check if all children of this parent are safe */
			int all_children_safe = 1;
			struct vine_node *child_node;
			LIST_ITERATE(parent_node->children, child_node)
			{
				/* shortcut if this child is part of the recovery subgraph */
				if (set_lookup(visited_nodes, child_node)) {
					continue;
				}
				/* shortcut if this outside child is not persisted */
				if (!node_outfile_has_been_persisted(child_node)) {
					all_children_safe = 0;
					break;
				}
				/* shortcut if this outside child is unsafely pruned */
				if (child_node->prune_status == PRUNE_STATUS_UNSAFE) {
					all_children_safe = 0;
					break;
				}
			}

			if (all_children_safe) {
				set_insert(safe_ancestors, parent_node);
				list_push_tail(queue, parent_node);
			}
		}
	}

	list_delete(queue);
	set_delete(visited_nodes);

	return safe_ancestors;
}

/**
 * Print the info of the node.
 * @param node Reference to the node object.
 */
void vine_node_debug_print(struct vine_node *node)
{
	if (!node) {
		return;
	}

	if (!node->task) {
		debug(D_ERROR, "node %" PRIu64 " has no task", node->node_id);
		return;
	}

	debug(D_VINE, "---------------- Node Info ----------------");
	debug(D_VINE, "node_id: %" PRIu64, node->node_id);
	debug(D_VINE, "task_id: %d", node->task->task_id);
	debug(D_VINE, "depth: %d", node->depth);
	debug(D_VINE, "height: %d", node->height);
	debug(D_VINE, "prune_depth: %d", node->prune_depth);

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

	/* print parent and child node ids */
	char *parent_ids = NULL;
	struct vine_node *p;
	LIST_ITERATE(node->parents, p)
	{
		if (!parent_ids) {
			parent_ids = string_format("%" PRIu64, p->node_id);
		} else {
			char *tmp = string_format("%s, %" PRIu64, parent_ids, p->node_id);
			free(parent_ids);
			parent_ids = tmp;
		}
	}

	char *child_ids = NULL;
	struct vine_node *c;
	LIST_ITERATE(node->children, c)
	{
		if (!child_ids) {
			child_ids = string_format("%" PRIu64, c->node_id);
		} else {
			char *tmp = string_format("%s, %" PRIu64, child_ids, c->node_id);
			free(child_ids);
			child_ids = tmp;
		}
	}

	debug(D_VINE, "parents: %s", parent_ids ? parent_ids : "(none)");
	debug(D_VINE, "children: %s", child_ids ? child_ids : "(none)");

	free(parent_ids);
	free(child_ids);

	debug(D_VINE, "-------------------------------------------");
}

/**
 * Delete the node and all of its associated resources.
 * @param node Reference to the node object.
 */
void vine_node_delete(struct vine_node *node)
{
	if (!node) {
		return;
	}

	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
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

	if (node->fired_parents) {
		set_delete(node->fired_parents);
	}
	free(node);
}