#ifndef NODE_H
#define NODE_H

#include <stddef.h>

#include "set.h"
#include "timestamp.h"

#include "vine_file.h"
#include "vine_task.h"

/** The storage type of the node's output file. */
typedef enum {
	NODE_OUTFILE_TYPE_LOCAL = 0,	      // staged file under graph output_dir
	NODE_OUTFILE_TYPE_TEMP,		      // TaskVine temp blob
	NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM, // path on shared storage, no vine_file
} node_outfile_type_t;

/** The node object. */
struct node {
	uint64_t node_id; // graph assigned id
	int is_target;	  // if set, output is retrieved when the task completes

	struct vine_task *task;
	struct vine_file *task_runner_arg_file; // JSON args buffer for the runner
	struct vine_file *outfile;		// NULL when output is PFS only
	char *outfile_remote_name;
	size_t outfile_size_bytes;
	node_outfile_type_t outfile_type;
	size_t pfs_credited_bytes; // contribution to executor pfs_usage_bytes

	struct list *parents;
	struct list *children;

	int remaining_parents_count; // parents not yet satisfied for scheduling
	struct set *fired_parents;   // parents already counted toward that count
	int completed;
	int cut;		// return released by cut, cleared if recovery restores file
	int prune_depth_pruned; // temp released by prune-depth
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
};

/** Create a new node.
@param node_id Unique node identifier supplied by the owning graph.
@return Newly allocated node instance.
*/
struct node *node_create(uint64_t node_id);

/** Create the task arguments for a node.
@param node Reference to the node.
@return The task arguments in JSON format: {"fn_args": ["node_id"], "fn_kwargs": {}} (string id for run_scheduler_keys).
*/
char *node_construct_task_arguments(struct node *node);

/** Delete a node and release owned resources.
@param node Reference to the node.
*/
void node_delete(struct node *node);

/** Print information about a node.
@param node Reference to the node.
*/
void node_debug_print(struct node *node);

/** Update the critical path time of a node.
@param node Reference to the node.
@param execution_time Reference to the execution time of the node.
*/
void node_update_critical_path_time(struct node *node, timestamp_t execution_time);

#endif // NODE_H
