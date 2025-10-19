#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "jx.h"
#include "jx_print.h"
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
#include "strategic_orchestration_node.h"
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
static int node_outfile_has_been_persisted(struct strategic_orchestration_node *node)
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
void son_update_critical_path_time(struct strategic_orchestration_node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_path_time = 0;
	struct strategic_orchestration_node *parent_node;
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
static void find_parents_dfs(struct strategic_orchestration_node *node, int remaining_depth, struct list *result, struct set *visited)
{
	if (!node || set_lookup(visited, node)) {
		return;
	}

	set_insert(visited, node);
	if (remaining_depth == 0) {
		list_push_tail(result, node);
		return;
	}
	struct strategic_orchestration_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		find_parents_dfs(parent_node, remaining_depth - 1, result, visited);
	}
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/**
 * Create a new node object.
 * @param node_key Reference to the node key.
 * @param is_target_key Reference to whether the node is a target key.
 * @return A new node object.
 */
struct strategic_orchestration_node *son_create(const char *node_key, int is_target_key)
{
	if (!node_key) {
		debug(D_ERROR, "Cannot create node because node_key is NULL");
		return NULL;
	}
	if (is_target_key != 0 && is_target_key != 1) {
		debug(D_ERROR, "Cannot create node because is_target_key is not 0 or 1");
		return NULL;
	}

	struct strategic_orchestration_node *node = xxmalloc(sizeof(struct strategic_orchestration_node));

	node->is_target_key = is_target_key;
	node->node_key = xxstrdup(node_key);

	/* create a unique outfile remote name for the node */
	cctools_uuid_t id;
	cctools_uuid_create(&id);
	node->outfile_remote_name = xxstrdup(id.str);

	node->prune_status = PRUNE_STATUS_NOT_PRUNED;
	node->parents = list_create();
	node->children = list_create();
	node->pending_parents = set_create(0);
	node->completed = 0;
	node->prune_depth = 0;
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

	node->critical_path_time = -1;

	return node;
}

/**
 * Construct the task arguments for the node.
 * @param node Reference to the node object.
 * @return The task arguments in JSON format: {"fn_args": [key], "fn_kwargs": {}}.
 */
char *son_construct_task_arguments(struct strategic_orchestration_node *node)
{
	if (!node) {
		return NULL;
	}

	struct jx *event = jx_object(NULL);
	struct jx *args = jx_array(NULL);
	jx_array_append(args, jx_string(node->node_key));
	jx_insert(event, jx_string("fn_args"), args);
	jx_insert(event, jx_string("fn_kwargs"), jx_object(NULL));

	char *infile_content = jx_print_string(event);
	jx_delete(event);

	return infile_content;
}

/**
 * Find all parents in a specific depth of the node.
 * @param node Reference to the node object.
 * @param depth Reference to the depth.
 * @return The list of parents.
 */
struct list *son_find_parents_by_depth(struct strategic_orchestration_node *node, int depth)
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
struct set *son_find_safe_ancestors(struct strategic_orchestration_node *start_node)
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
		struct strategic_orchestration_node *current_node = list_pop_head(queue);
		struct strategic_orchestration_node *parent_node;

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
			struct strategic_orchestration_node *child_node;
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
void son_debug_print(struct strategic_orchestration_node *node)
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

	/* print parent and child node keys */
	char *parent_keys = NULL;
	struct strategic_orchestration_node *p;
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
	struct strategic_orchestration_node *c;
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
 * Delete the node and all of its associated resources.
 * @param node Reference to the node object.
 */
void son_delete(struct strategic_orchestration_node *node)
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