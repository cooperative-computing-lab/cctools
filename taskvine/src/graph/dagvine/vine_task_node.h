#ifndef VINE_TASK_NODE_H
#define VINE_TASK_NODE_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "taskvine.h"

/** Select the type of the node-output file. */
typedef enum {
    VINE_NODE_OUTFILE_TYPE_LOCAL = 0,               /* Node-output file will be stored locally on the manager's staging directory */
    VINE_NODE_OUTFILE_TYPE_TEMP,                    /* Node-output file will be stored in the temporary node-local storage */
    VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM,      /* Node-output file will be stored in the persistent shared file system */
} vine_task_node_outfile_type_t;

typedef enum {
    PRUNE_STATUS_NOT_PRUNED = 0,
    PRUNE_STATUS_SAFE, 
    PRUNE_STATUS_UNSAFE
} prune_status_t;

struct vine_task_node {
    char *node_key;

    struct vine_manager *manager;
    struct vine_task *task;
    struct vine_file *infile;
    struct vine_file *outfile;
    char *outfile_remote_name;
    char *staging_dir;
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

    vine_task_node_outfile_type_t outfile_type;
    prune_status_t prune_status;
};

struct vine_task_node *vine_task_node_create(
    struct vine_manager *manager,
    const char *node_key,
    const char *proxy_library_name,
    const char *proxy_function_name,
    const char *staging_dir,
    int prune_depth
);

void vine_task_node_delete(struct vine_task_node *node);
double compute_lex_priority(const char *key);
void vine_task_node_prune_ancestors(struct vine_task_node *node);
void vine_task_node_print_info(struct vine_task_node *node);
void vine_task_node_update_critical_time(struct vine_task_node *node, timestamp_t execution_time);
void vine_task_node_replicate_outfile(struct vine_task_node *node);
void vine_task_node_set_outfile(struct vine_task_node *node, vine_task_node_outfile_type_t outfile_type, const char *outfile_remote_name);

#endif