#ifndef VINE_GRAPH_H
#define VINE_GRAPH_H

#include <stdint.h>

#include "vine_task.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "vine_node.h"
#include "taskvine.h"
#include "timestamp.h"

/** The task priority algorithm used for vine graph scheduling. */
typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,			   /**< Assign random priority to tasks */
	TASK_PRIORITY_MODE_DEPTH_FIRST,			   /**< Prioritize deeper tasks first */
	TASK_PRIORITY_MODE_BREADTH_FIRST,		   /**< Prioritize shallower tasks first */
	TASK_PRIORITY_MODE_FIFO,			   /**< First in, first out priority */
	TASK_PRIORITY_MODE_LIFO,			   /**< Last in, first out priority */
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,		   /**< Prioritize tasks with larger inputs first */
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST /**< Prioritize tasks with larger storage footprint first */
} task_priority_mode_t;

/** The vine graph (logical scheduling layer). */
struct vine_graph {
	struct vine_manager *manager;
	struct itable *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;
	/* Maps a logical in/out filename (remote_name) to a stable cached_name. */
	struct hash_table *inout_filename_to_cached_name;

	/* Unsuccessful tasks are appended to this list to be resubmitted later. */
	struct list *resubmit_queue;

	/* The directory to store the checkpointed results.
	 * Only intermediate results can be checkpointed, the fraction of intermediate results to checkpoint is controlled by the checkpoint-fraction parameter. */
	char *checkpoint_dir;

	/* Results of target nodes will be stored in this directory.
	 * This dir path can not necessarily be a shared file system directory,
	 * output files will be retrieved through the network instead,
	 * as long as the manager can access it. */
	char *output_dir;

	/* Python-side proxy library name. The context_graph runtime owns this library and sends calls into the vine graph
	 * so the manager can execute them through the proxy function. */
	char *proxy_library_name;

	/* The proxy function lives inside that library. It receives vine node IDs, looks up the
	 * Python callable and arguments inside the context_graph runtime, and executes the work. The runtime generates the name
	 * and shares it with the vine graph. */
	char *proxy_function_name;

	/* The depth of the pruning strategy. 0 means no pruning, 1 means the most aggressive pruning. */
	int prune_depth;
	double checkpoint_fraction; /* 0 - 1, the fraction of intermediate results to checkpoint */

	task_priority_mode_t task_priority_mode; /* priority mode for task graph task scheduling */
	double failure_injection_step_percent;	 /* 0 - 100, the percentage of steps to inject failure */

	double progress_bar_update_interval_sec; /* update interval for the progress bar in seconds */

	/* The filename of the csv file to store the time metrics of the vine graph. */
	char *time_metrics_filename;

	int enable_debug_log;	 /* whether to enable debug log */
	int print_graph_details; /* whether to print the graph details */

	int auto_recovery; /* whether to enable auto recovery */

	int max_retry_attempts;	   /* the maximum number of times to retry a task */
	double retry_interval_sec; /* the interval between retries in seconds, 0 means no retry interval */

	timestamp_t time_first_task_dispatched; /* the time when the first task is dispatched */
	timestamp_t time_last_task_retrieved;	/* the time when the last task is retrieved */
	timestamp_t makespan_us;		/* the makespan of the vine graph in microseconds */
};

/* Public APIs for operating the vine graph */

/** Create a vine graph and return it.
@param q Reference to the current manager object.
@return A new vine graph.
*/
struct vine_graph *vine_graph_create(struct vine_manager *q);

/** Create a new node in the vine graph.
@param vg Reference to the vine graph.
@return The auto-assigned node id.
*/
uint64_t vine_graph_add_node(struct vine_graph *vg);

/** Mark a node as a retrieval target.
@param vg Reference to the vine graph.
@param node_id Identifier of the node to mark as target.
*/
void vine_graph_set_target(struct vine_graph *vg, uint64_t node_id);

/** Add a dependency between two nodes in the vine graph.
@param vg Reference to the vine graph.
@param parent_id Identifier of the parent node.
@param child_id Identifier of the child node.
*/
void vine_graph_add_dependency(struct vine_graph *vg, uint64_t parent_id, uint64_t child_id);

/** Finalize the metrics of the vine graph.
@param vg Reference to the vine graph.
*/
void vine_graph_finalize(struct vine_graph *vg);

/** Get the heavy score of a node in the vine graph.
@param vg Reference to the vine graph.
@param node_id Identifier of the node.
@return The heavy score.
*/
double vine_graph_get_node_heavy_score(const struct vine_graph *vg, uint64_t node_id);

/** Execute the task graph.
@param vg Reference to the vine graph.
*/
void vine_graph_execute(struct vine_graph *vg);

/** Get the outfile remote name of a node in the vine graph.
@param vg Reference to the vine graph.
@param node_id Identifier of the node.
@return The outfile remote name.
*/
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *vg, uint64_t node_id);

/** Get the local outfile source of a node in the vine graph.
@param vg Reference to the vine graph.
@param node_id Identifier of the node.
@return The local outfile source, or NULL if the node does not produce a local file.
*/
const char *vine_graph_get_node_local_outfile_source(const struct vine_graph *vg, uint64_t node_id);

/** Delete a vine graph.
@param vg Reference to the vine graph.
*/
void vine_graph_delete(struct vine_graph *vg);

/** Get the proxy library name of the vine graph.
@param vg Reference to the vine graph.
@return The proxy library name.
*/
const char *vine_graph_get_proxy_library_name(const struct vine_graph *vg);

/** Add an input file to a task. The input file will be declared as a temp file.
@param vg Reference to the vine graph.
@param task_id Identifier of the task.
@param filename Reference to the filename.
*/
void vine_graph_add_task_input(struct vine_graph *vg, uint64_t task_id, const char *filename);

/** Add an output file to a task. The output file will be declared as a temp file.
@param vg Reference to the vine graph.
@param task_id Identifier of the task.
@param filename Reference to the filename.
*/
void vine_graph_add_task_output(struct vine_graph *vg, uint64_t task_id, const char *filename);

/** Set the proxy function name of the vine graph.
@param vg Reference to the vine graph.
@param proxy_function_name Reference to the proxy function name.
*/
void vine_graph_set_proxy_function_name(struct vine_graph *vg, const char *proxy_function_name);

/** Tune the vine graph.
@param vg Reference to the vine graph.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int vine_graph_tune(struct vine_graph *vg, const char *name, const char *value);

/** Get the makespan of the vine graph in microseconds.
@param vg Reference to the vine graph.
@return The makespan in microseconds.
*/
uint64_t vine_graph_get_makespan_us(const struct vine_graph *vg);

#endif // VINE_GRAPH_H
