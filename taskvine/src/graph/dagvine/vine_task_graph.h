#ifndef VINE_TASK_GRAPH_H
#define VINE_TASK_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "vine_task_node.h"
#include "taskvine.h"


/** Select priority algorithm for task graph task scheduling. */
typedef enum {
    VINE_TASK_GRAPH_PRIORITY_MODE_RANDOM = 0,          /**< Assign random priority to tasks */
    VINE_TASK_GRAPH_PRIORITY_MODE_DEPTH_FIRST,         /**< Prioritize deeper tasks first */
    VINE_TASK_GRAPH_PRIORITY_MODE_BREADTH_FIRST,       /**< Prioritize shallower tasks first */
    VINE_TASK_GRAPH_PRIORITY_MODE_FIFO,                /**< First in, first out priority */
    VINE_TASK_GRAPH_PRIORITY_MODE_LIFO,                /**< Last in, first out priority */
    VINE_TASK_GRAPH_PRIORITY_MODE_LARGEST_INPUT_FIRST, /**< Prioritize tasks with larger inputs first */
    VINE_TASK_GRAPH_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST /**< Prioritize tasks with larger storage footprint first */
} vine_task_graph_priority_mode_t;

/** The task graph object. */
struct vine_task_graph {
    struct vine_manager *manager;
	struct hash_table *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;

    char *proxy_library_name;
    char *proxy_function_name;

    vine_task_graph_priority_mode_t task_priority_mode;  // priority mode for task graph task scheduling
    double failure_injection_step_percent;  // 0 - 100, the percentage of steps to inject failure
};


/* Public APIs for operating the task graph */

/** Create a task graph object and return it.
@param q Reference to the current manager object.
@return A new task graph object.
*/
struct vine_task_graph *vine_task_graph_create(struct vine_manager *q);

/** Create a new node in the task graph.
@param tg Reference to the task graph object.
@param node_key Reference to the node key.
@param staging_dir Reference to the staging directory.
@param prune_depth Reference to the prune depth.
@return A new node object.
*/
struct vine_task_node *vine_task_graph_add_node(struct vine_task_graph *tg,
	const char *node_key,
	const char *staging_dir,
	int prune_depth);

/** Add a dependency between two nodes in the task graph.
@param tg Reference to the task graph object.
@param parent_key Reference to the parent node key.
@param child_key Reference to the child node key.
*/
void vine_task_graph_add_dependency(struct vine_task_graph *tg, const char *parent_key, const char *child_key);

/** Finalize the metrics of the task graph.
@param tg Reference to the task graph object.
*/
void vine_task_graph_compute_topology_metrics(struct vine_task_graph *tg);

/** Get the heavy score of a node in the task graph.
@param tg Reference to the task graph object.
@param node_key Reference to the node key.
@return The heavy score.
*/
double vine_task_graph_get_node_heavy_score(const struct vine_task_graph *tg, const char *node_key);

/** Set the type of the node-output file.
@param tg Reference to the task graph object.
@param node_key Reference to the node key.
@param outfile_type Reference to the output file type.
@param outfile_remote_name Reference to the output file remote name.
*/
void vine_task_graph_set_node_outfile(struct vine_task_graph *tg, const char *node_key, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name);

/** Execute the task graph.
@param tg Reference to the task graph object.
*/
void vine_task_graph_execute(struct vine_task_graph *tg);

/** Get the local outfile source of a node in the task graph.
@param tg Reference to the task graph object.
@param node_key Reference to the node key.
@return The local outfile source.
*/
const char *vine_task_graph_get_node_local_outfile_source(const struct vine_task_graph *tg, const char *node_key);

/** Delete a task graph object.
@param tg Reference to the task graph object.
*/
void vine_task_graph_delete(struct vine_task_graph *tg);

/** Get the library name of the task graph.
@param tg Reference to the task graph object.
@return The library name.
*/
const char *vine_task_graph_get_proxy_library_name(const struct vine_task_graph *tg);

/** Get the function name of the task graph.
@param tg Reference to the task graph object.
@return The function name.
*/
const char *vine_task_graph_get_proxy_function_name(const struct vine_task_graph *tg);

/** Set the proxy library name (Python-side), shared by all tasks.
@param tg Reference to the task graph object.
@param proxy_library_name Reference to the proxy library name.
*/
void vine_task_graph_set_proxy_library_name(struct vine_task_graph *tg, const char *proxy_library_name);

/** Set the proxy function name (Python-side), shared by all tasks.
@param tg Reference to the task graph object.
@param proxy_function_name Reference to the proxy function name.
*/
void vine_task_graph_set_proxy_function_name(struct vine_task_graph *tg, const char *proxy_function_name);

/** Tune the task graph.
@param tg Reference to the task graph object.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int vine_task_graph_tune(struct vine_task_graph *tg, const char *name, const char *value);

#endif // VINE_TASK_GRAPH_H
