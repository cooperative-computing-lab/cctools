#ifndef VINE_TASK_GRAPH_H
#define VINE_TASK_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "vine_task_node.h"

struct vine_task_graph {
    struct vine_manager *manager;
	struct hash_table *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;

    char *proxy_library_name;
    char *proxy_function_name;

    double failure_injection_step_percent;  // 0 - 100, the percentage of steps to inject failure
};


/* Public APIs for operating the task graph */
const char *vine_task_graph_get_proxy_library_name(const struct vine_task_graph *tg);
const char *vine_task_graph_get_proxy_function_name(const struct vine_task_graph *tg);
double vine_task_graph_get_node_heavy_score(const struct vine_task_graph *tg, const char *node_key);
const char *vine_task_graph_get_node_local_outfile_source(const struct vine_task_graph *tg, const char *node_key);
void vine_task_graph_compute_topology_metrics(struct vine_task_graph *tg);
struct vine_task_node *vine_task_graph_add_node(struct vine_task_graph *tg,
    const char *node_key,
    const char *staging_dir,
    int prune_depth,
    vine_task_node_priority_mode_t priority_mode);
struct vine_task_graph *vine_task_graph_create(struct vine_manager *q);
void vine_task_graph_set_failure_injection_step_percent(struct vine_task_graph *tg, double percent);
void vine_task_graph_add_dependency(struct vine_task_graph *tg, const char *parent_key, const char *child_key);
void vine_task_graph_set_node_outfile(struct vine_task_graph *tg, const char *node_key, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name);
void vine_task_graph_execute(struct vine_task_graph *tg);
void vine_task_graph_delete(struct vine_task_graph *tg);
void vine_task_graph_set_proxy_library_and_function_names(struct vine_task_graph *tg,
	const char *proxy_library_name,
	const char *proxy_function_name);

#endif // VINE_TASK_GRAPH_H
