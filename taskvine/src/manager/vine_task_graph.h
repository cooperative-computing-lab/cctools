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


#endif // VINE_TASK_GRAPH_H
