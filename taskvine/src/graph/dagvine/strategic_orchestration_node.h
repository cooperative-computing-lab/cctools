#ifndef STRATEGIC_ORCHESTRATION_NODE_H
#define STRATEGIC_ORCHESTRATION_NODE_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "taskvine.h"

/** Select the type of the node-output file. */
typedef enum {
	VINE_NODE_OUTFILE_TYPE_LOCAL = 0,	   /* Node-output file will be stored locally on the manager's staging directory */
	VINE_NODE_OUTFILE_TYPE_TEMP,		   /* Node-output file will be stored in the temporary node-local storage */
	VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM, /* Node-output file will be stored in the persistent shared file system */
} node_outfile_type_t;

typedef enum {
	PRUNE_STATUS_NOT_PRUNED = 0,
	PRUNE_STATUS_SAFE,
	PRUNE_STATUS_UNSAFE
} prune_status_t;

struct strategic_orchestration_node {
	char *node_key;

	/* Whether the node is a target key.
	 * If true, the output of the node will be declared as a vine_file and retrieved through the network. */
	int is_target_key;

	struct vine_manager *manager;
	struct vine_task *task;
	struct vine_file *infile;
	struct vine_file *outfile;
	char *outfile_remote_name;
	char *target_results_dir;
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

	timestamp_t critical_time;
	timestamp_t time_spent_on_unlink_local_files;
	timestamp_t time_spent_on_prune_ancestors_of_temp_node;
	timestamp_t time_spent_on_prune_ancestors_of_persisted_node;

	node_outfile_type_t outfile_type;
	prune_status_t prune_status;
};

/** Create a new strategic orchestration node object.
@param manager Reference to the manager object.
@param node_key Reference to the node key.
@param is_target_key Reference to whether the node is a target key.
@param proxy_library_name Reference to the proxy library name.
@param proxy_function_name Reference to the proxy function name.
@param target_results_dir Reference to the target results directory.
@param prune_depth Reference to the prune depth.
@return A new strategic orchestration node object.
*/
struct strategic_orchestration_node *son_create(
		struct vine_manager *manager,
		const char *node_key,
		int is_target_key,
		const char *proxy_library_name,
		const char *proxy_function_name,
		const char *target_results_dir,
		int prune_depth);

/** Delete a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_delete(struct strategic_orchestration_node *node);

/** Prune the ancestors of a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_prune_ancestors(struct strategic_orchestration_node *node);

/** Print information about a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_print_info(struct strategic_orchestration_node *node);

/** Update the critical time of a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
@param execution_time Reference to the execution time of the node.
*/
void son_update_critical_time(struct strategic_orchestration_node *node, timestamp_t execution_time);

/** Replicate the outfile of a strategic orchestration node object.
@param node Reference to the strategic orchestration node object.
*/
void son_replicate_outfile(struct strategic_orchestration_node *node);

#endif