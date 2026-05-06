#include <inttypes.h>
#include <stdlib.h>

#include "debug.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "node.h"

/*************************************************************/
/* Public APIs */
/*************************************************************/

/**
 * Update the critical path time of a node.
 * @param node Reference to the node object.
 * @param execution_time Reference to the execution time of the node.
 */
void node_update_critical_path_time(struct node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_path_time = 0;
	struct node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->critical_path_time > max_parent_critical_path_time) {
			max_parent_critical_path_time = parent_node->critical_path_time;
		}
	}
	node->critical_path_time = max_parent_critical_path_time + execution_time;
}

/**
 * Create a new node owned by the C-side graph.
 * @param node_id Graph-assigned identifier that keeps C and Python in sync.
 * @return Newly allocated node.
 */
struct node *node_create(uint64_t node_id)
{
	struct node *node = xxmalloc(sizeof(struct node));

	node->is_target = 0;
	node->node_id = node_id;

	node->task = NULL;
	node->task_runner_arg_file = NULL;
	node->outfile = NULL;
	node->outfile_remote_name = string_format("outfile_node_%" PRIu64, node->node_id);
	node->outfile_type = NODE_OUTFILE_TYPE_TEMP;

	node->parents = list_create();
	node->children = list_create();
	node->extra_outputs = list_create();
	node->extra_inputs = list_create();
	node->remaining_parents_count = 0;
	node->fired_parents = NULL;
	node->completed = 0;
	node->cut = 0;
	node->released_by_prune_depth = 0;
	node->outfile_size_bytes = 0;
	node->pfs_credited_bytes = 0;
	node->in_resubmit_queue = 0;
	node->last_failure_time = 0;

	node->depth = -1;
	node->height = -1;
	node->upstream_subgraph_size = -1;
	node->downstream_subgraph_size = -1;
	node->fan_in = -1;
	node->fan_out = -1;
	node->heavy_score = -1;

	node->critical_path_time = -1;
	node->preprocessing_time_us = 0;
	node->postprocessing_time_us = 0;

	return node;
}

/**
 * Construct the task arguments for the node.
 * @param node Reference to the node object.
 * @return The task arguments in JSON format: {"fn_args": ["node_id"], "fn_kwargs": {}} (string for run_scheduler_keys).
 */
char *node_construct_task_arguments(struct node *node)
{
	if (!node) {
		return NULL;
	}
	return string_format("{\"fn_args\":[\"%" PRIu64 "\"],\"fn_kwargs\":{}}", node->node_id);
}

/**
 * Print the info of the node.
 * @param node Reference to the node object.
 */
void node_debug_print(struct node *node)
{
	if (!node) {
		return;
	}

	debug(D_VINE, "---------------- Node Info ----------------");
	debug(D_VINE, "node_id: %" PRIu64, node->node_id);
	debug(D_VINE, "preprocessing_time_us (last): %" PRIu64, node->preprocessing_time_us);
	debug(D_VINE, "postprocessing_time_us (last): %" PRIu64, node->postprocessing_time_us);

	if (!node->task) {
		debug(D_VINE, "task: (none yet)");
		debug(D_VINE, "-------------------------------------------");
		return;
	}

	debug(D_VINE, "task_id: %d", node->task->task_id);
	debug(D_VINE, "depth: %d", node->depth);
	debug(D_VINE, "height: %d", node->height);

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

	char *parent_ids = NULL; // comma separated parent ids for logging
	struct node *p;
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

	char *child_ids = NULL; // comma separated child ids for logging
	struct node *c;
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
void node_delete(struct node *node)
{
	if (!node) {
		return;
	}

	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}

	vine_task_delete(node->task);
	node->task = NULL;

	if (node->task_runner_arg_file) {
		vine_file_delete(node->task_runner_arg_file);
		node->task_runner_arg_file = NULL;
	}
	if (node->outfile) {
		vine_file_delete(node->outfile);
		node->outfile = NULL;
	}

	list_delete(node->parents);
	list_delete(node->children);

	while (list_size(node->extra_inputs) > 0) {
		struct extra_io_mount *m = list_pop_head(node->extra_inputs);
		free(m->remote_name);
		free(m);
	}
	list_delete(node->extra_inputs);
	while (list_size(node->extra_outputs) > 0) {
		struct extra_io_mount *m = list_pop_head(node->extra_outputs);
		free(m->remote_name);
		free(m);
	}
	list_delete(node->extra_outputs);

	if (node->fired_parents) {
		set_delete(node->fired_parents);
	}
	free(node);
}