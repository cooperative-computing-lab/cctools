#ifndef STRATEGIC_ORCHESTRATION_NODE_H
#define STRATEGIC_ORCHESTRATION_NODE_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "set.h"
#include "taskvine.h"

/** Select the type of the node-output file. */
typedef enum {
	NODE_OUTFILE_TYPE_LOCAL = 0,	      /* Node-output file will be stored locally on the local staging directory */
	NODE_OUTFILE_TYPE_TEMP,		      /* Node-output file will be stored in the temporary node-local storage */
	NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM, /* Node-output file will be stored in the persistent shared file system */
} node_outfile_type_t;

/** The status of an output file of a node. */
typedef enum {
	PRUNE_STATUS_NOT_PRUNED = 0,
	PRUNE_STATUS_SAFE,
	PRUNE_STATUS_UNSAFE
} prune_status_t;

/** The strategic orchestration node object. */
struct strategic_orchestration_node {
	char *node_key;

	/* Whether the node is a target key.
	 * If true, the output of the node will be declared as a vine_file and retrieved through the network. */
	int is_target_key;

	struct vine_task *task;
	struct vine_file *infile;
	struct vine_file *outfile;
	char *outfile_remote_name;
	size_t outfile_size_bytes;

	struct list *parents;
	struct list *children;
	struct set *pending_parents;

	int retry_attempts_left;
	int completed;
	int prune_depth;

	int depth;
	int height;
	int upstream_subgraph_size;
	int downstream_subgraph_size;
	int fan_in;
	int fan_out;
	double heavy_score;

	timestamp_t critical_path_time;
	timestamp_t time_spent_on_unlink_local_files;
	timestamp_t time_spent_on_prune_ancestors_of_temp_node;
	timestamp_t time_spent_on_prune_ancestors_of_persisted_node;

	timestamp_t submission_time;
	timestamp_t scheduling_time;
	timestamp_t execution_time;
	timestamp_t retrieval_time;

	node_outfile_type_t outfile_type;
	prune_status_t prune_status;
};

/** Create a new strategic orchestration node object.
@param node_key Reference to the node key.
@param is_target_key Reference to whether the node is a target key.
@return A new strategic orchestration node object.
*/
struct strategic_orchestration_node *son_create(const char *node_key, int is_target_key);

/** Create the task arguments for a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
@return The task arguments in JSON format: {"fn_args": [key], "fn_kwargs": {}}.
*/
char *son_construct_task_arguments(struct strategic_orchestration_node *node);

/** Delete a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_delete(struct strategic_orchestration_node *node);

/** Print information about a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_debug_print(struct strategic_orchestration_node *node);

/** Find all safe ancestors of a strategic orchestration node object.
@param start_node Reference to the start node.
@return The set of safe ancestors.
*/
struct set *son_find_safe_ancestors(struct strategic_orchestration_node *start_node);

/** Find all parents in a specific depth of a strategic orchestration node object.
@param node Reference to the node object.
@param depth Reference to the depth.
@return The list of parents.
*/
struct list *son_find_parents_by_depth(struct strategic_orchestration_node *node, int depth);

/** Update the critical time of a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
@param execution_time Reference to the execution time of the node.
*/
void son_update_critical_path_time(struct strategic_orchestration_node *node, timestamp_t execution_time);

#endif