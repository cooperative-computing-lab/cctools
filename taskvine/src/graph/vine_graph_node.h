#ifndef VINE_GRAPH_NODE_H
#define VINE_GRAPH_NODE_H

#include <stddef.h>

#include "set.h"
#include "timestamp.h"

#include "list.h"
#include "vine_file.h"
#include "vine_task.h"

/**
 * One element of @c extra_outputs or @c extra_inputs: a logical filename plus its @c vine_file
 * (declared during graph build; attached to @c vine_task in @c vine_graph_executor_materialize_node).
 */
struct vine_graph_io_mount {
	struct vine_file *file;
	char *remote_name;
};

/** The storage type of the node's output file. */
typedef enum {
	VINE_GRAPH_NODE_OUTFILE_TYPE_LOCAL = 0,		 // staged file under graph output_dir
	VINE_GRAPH_NODE_OUTFILE_TYPE_TEMP,		 // TaskVine temp blob
	VINE_GRAPH_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM, // path on shared storage, no vine_file
} vine_graph_node_outfile_type_t;

/** The node object. */
struct vine_graph_node {
	uint64_t node_id; // graph assigned id
	/**
	 * Supernode leader id for scheduling: equals @c node_id for a single-node group.
	 * After @c vine_graph_supernode_register, every member shares the same leader id.
	 */
	uint64_t super_leader_id;
	int is_target; // if set, output is retrieved when the task completes

	struct vine_task *task;
	struct vine_file *task_runner_arg_file; // JSON args buffer for the runner
	struct vine_file *outfile;		// NULL when output is PFS only
	char *outfile_remote_name;
	size_t outfile_size_bytes;
	vine_graph_node_outfile_type_t outfile_type;
	size_t pfs_credited_bytes; // contribution to executor pfs_usage_bytes

	struct list *parents;
	struct list *children;
	/**
	 * @c Workflow.task_produces / @c vine_graph_executor_add_task_output: extra outputs beyond this node’s primary
	 * @c outfile (the runner’s standard @c outfile_node_* result). Each list item is a declared
	 * @c vine_file keyed by the same logical path string used by consumers. Filled before
	 * @c node->task exists; consumed when building the task at submit / materialize time.
	 */
	struct list *extra_outputs;
	/**
	 * @c Workflow.task_consumes / @c vine_graph_executor_add_task_input: extra inputs beyond those implied by
	 * the DAG via parent @c outfile edges (paired logical files shared with producers). Same lifecycle
	 * as @c extra_outputs: queued at graph build, wired on @c vine_task at materialize.
	 */
	struct list *extra_inputs;

	int remaining_parents_count; // parents not yet satisfied for scheduling
	struct set *fired_parents;   // parents already counted toward that count
	int completed;
	int cut; // return released by cut, cleared if recovery restores file
	/** Non-zero after this node's temp output was released under @c graph->prune_depth; cleared on recovery. */
	int released_by_prune_depth;
	int in_resubmit_queue;
	timestamp_t last_failure_time; // last enqueue to resubmit queue

	int depth;
	int height;
	int upstream_subgraph_size;
	int downstream_subgraph_size;
	int fan_in;
	int fan_out;
	double heavy_score;

	timestamp_t critical_path_time;
	/** Latest @c vine_graph_executor_submit_node interval for this node (microseconds); graph total is on @c struct vine_graph_executor. */
	uint64_t preprocessing_time_us;
	/** Latest @c vine_graph_executor_run_completion_postprocess interval for this node (microseconds); graph total on executor. */
	uint64_t postprocessing_time_us;
};

/** Create a new node.
@param node_id Unique node identifier supplied by the owning graph.
@return Newly allocated node instance.
*/
struct vine_graph_node *vine_graph_node_create(uint64_t node_id);

/**
 * Remove parent->child from both endpoints' parents/children lists (no-op if NULL).
 * Used when rewiring supernodes so stale edges do not corrupt fan-in/out.
 */
void vine_graph_node_remove_dependency(struct vine_graph_node *parent, struct vine_graph_node *child);

/**
 * Add parent->child if that edge is not already present (idempotent).
 */
void vine_graph_node_ensure_dependency(struct vine_graph_node *parent, struct vine_graph_node *child);

/**
 * Drop fired_parents so executor scheduling can recount parents (e.g. after supernode merge rewires edges).
 */
void vine_graph_node_clear_fired_parents(struct vine_graph_node *n);

/** Create the task arguments for a node.
@param node Reference to the node.
@return The task arguments in JSON format: {"fn_args": ["node_id"], "fn_kwargs": {}} (string id for run_scheduler_keys).
*/
char *vine_graph_node_construct_task_arguments(struct vine_graph_node *node);

/** Delete a node and release owned resources.
@param node Reference to the node.
*/
void vine_graph_node_delete(struct vine_graph_node *node);

/** Print information about a node.
@param node Reference to the node.
*/
void vine_graph_node_debug_print(struct vine_graph_node *node);

/** Update the critical path time of a node.
@param node Reference to the node.
@param execution_time Reference to the execution time of the node.
*/
void vine_graph_node_update_critical_path_time(struct vine_graph_node *node, timestamp_t execution_time);

#endif // VINE_GRAPH_NODE_H
