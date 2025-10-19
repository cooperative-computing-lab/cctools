#ifndef STRATEGIC_ORCHESTRATION_GRAPH_H
#define STRATEGIC_ORCHESTRATION_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "strategic_orchestration_node.h"
#include "taskvine.h"

/** Select priority algorithm for strategic orchestration graph task scheduling. */
typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,			   /**< Assign random priority to tasks */
	TASK_PRIORITY_MODE_DEPTH_FIRST,			   /**< Prioritize deeper tasks first */
	TASK_PRIORITY_MODE_BREADTH_FIRST,		   /**< Prioritize shallower tasks first */
	TASK_PRIORITY_MODE_FIFO,			   /**< First in, first out priority */
	TASK_PRIORITY_MODE_LIFO,			   /**< Last in, first out priority */
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,		   /**< Prioritize tasks with larger inputs first */
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST /**< Prioritize tasks with larger storage footprint first */
} task_priority_mode_t;

/** The strategic orchestration graph object (logical scheduling layer). */
struct strategic_orchestration_graph {
	struct vine_manager *manager;
	struct hash_table *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;

	/* Results of target keys will be stored in this directory.
	 * This dir path can not necessarily be a shared file system directory,
	 * output files will be retrieved through the network instead,
	 * as long as the manager can access it. */
	char *target_results_dir;

	char *proxy_library_name;  // Python-side proxy library name (shared by all tasks)
	char *proxy_function_name; // Python-side proxy function name (shared by all tasks)

	int prune_depth;
	double checkpoint_fraction; // 0 - 1, the fraction of intermediate results to checkpoint

	task_priority_mode_t task_priority_mode; // priority mode for task graph task scheduling
	double failure_injection_step_percent;	 // 0 - 100, the percentage of steps to inject failure
};

/* Public APIs for operating the strategic orchestration graph */

/** Create a strategic orchestration graph object and return it.
@param q Reference to the current manager object.
@return A new strategic orchestration graph object.
*/
struct strategic_orchestration_graph *sog_create(struct vine_manager *q);

/** Create a new node in the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param node_key Reference to the node key.
@param is_target_key Reference to whether the node is a target key.
*/
void sog_add_node(struct strategic_orchestration_graph *sog, const char *node_key, int is_target_key);

/** Add a dependency between two nodes in the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param parent_key Reference to the parent node key.
@param child_key Reference to the child node key.
*/
void sog_add_dependency(struct strategic_orchestration_graph *sog, const char *parent_key, const char *child_key);

/** Finalize the metrics of the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
*/
void sog_compute_topology_metrics(struct strategic_orchestration_graph *sog);

/** Get the heavy score of a node in the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param node_key Reference to the node key.
@return The heavy score.
*/
double sog_get_node_heavy_score(const struct strategic_orchestration_graph *sog, const char *node_key);

/** Execute the task graph.
@param sog Reference to the strategic orchestration graph object.
*/
void sog_execute(struct strategic_orchestration_graph *sog);

/** Get the outfile remote name of a node in the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param node_key Reference to the node key.
@return The outfile remote name.
*/
const char *sog_get_node_outfile_remote_name(const struct strategic_orchestration_graph *sog, const char *node_key);

/** Get the local outfile source of a node in the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param node_key Reference to the node key.
@return The local outfile source.
*/
const char *sog_get_node_local_outfile_source(const struct strategic_orchestration_graph *sog, const char *node_key);

/** Delete a strategic orchestration graph object.
@param sog Reference to the strategic orchestration graph object.
*/
void sog_delete(struct strategic_orchestration_graph *sog);

/** Get the proxy library name of the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@return The proxy library name.
*/
const char *sog_get_proxy_library_name(const struct strategic_orchestration_graph *sog);

/** Set the proxy function name of the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param proxy_function_name Reference to the proxy function name.
*/
void sog_set_proxy_function_name(struct strategic_orchestration_graph *sog, const char *proxy_function_name);

/** Tune the strategic orchestration graph.
@param sog Reference to the strategic orchestration graph object.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int sog_tune(struct strategic_orchestration_graph *sog, const char *name, const char *value);

#endif // STRATEGIC_ORCHESTRATION_GRAPH_H
