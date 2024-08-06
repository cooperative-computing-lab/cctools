/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_NODE_H
#define DAG_NODE_H

#include "batch_queue.h"
#include "batch_job.h"
#include "category.h"
#include "set.h"
#include "hash_table.h"
#include "itable.h"

typedef enum {
	DAG_NODE_STATE_WAITING = 0,
	DAG_NODE_STATE_RUNNING,
	DAG_NODE_STATE_COMPLETE,
	DAG_NODE_STATE_FAILED,
	DAG_NODE_STATE_ABORTED,
	DAG_NODE_STATE_MAX
} dag_node_state_t;

typedef enum {
	DAG_NODE_TYPE_COMMAND,
	DAG_NODE_TYPE_WORKFLOW,
} dag_node_type_t;

/* struct dag_node implements a linked list of nodes. A dag_node
 * represents a production rule from source files to target
 * files. The actual dag structure is given implicitly by the
 * source_files and target_files members (i.e., a dag_node has no
 * explicit knowledge of its logical dag_node ascendants or descendants).
 * In fact, dag_node acts more like the edge of the dag, with the
 * nodes being sets of source/target files (that is, a file may
 * be part of different nodes).
*/

struct dag_node {
	struct dag *d;           /* Dag this node belongs too. */
	dag_node_type_t type;	 /* Is the job a Unix command, a workflow, etc. */
	const char *command;     /* The command line to execute. */

	int nodeid;              /* The ordinal number as the rule appears in the makeflow file */
	int linenum;             /* Line number of the node's rule definition */
	int local_job;           /* Flag: does this node run locally? */

	struct set *descendants; /* The nodes of which this node is an immediate ancestor */
	struct set *ancestors;   /* The nodes of which this node is an immediate descendant */
	int ancestor_depth;      /* The depth of the ancestor tree for this node */

	const char *workflow_file;  /* Name of the sub-makeflow to run, if type is WORKFLOW */
	struct jx *workflow_args;   /* Arguments to pass to the workflow. */
	int workflow_is_jx;	    /* True is sub-workflow is jx, false otherwise. */

	struct itable *remote_names;        /* Mapping from struct *dag_files to remotenames (char *) */
	struct hash_table *remote_names_inv;/* Mapping from remote filenames to dag_file representing the local file. */
	struct list   *source_files;        /* list of dag_files of the node's requirements */
	struct list   *target_files;        /* list of dag_files of the node's productions */

	struct dag_node_footprint *footprint; /* Pointer to footprint structure created when using storage limits */

	struct category *category;          /* The set of task this node belongs too. Ideally, the makeflow
										   file labeled which tasks have comparable resource usage. */
	struct hash_table *variables;       /* This node settings for variables with @ syntax */

	category_allocation_t resource_request;  /* type of allocation for the node (user, unlabeled, max, etc.) */
    struct rmsummary *resources_requested;   /* resources required explicitely by this rule alone, not taking
                                                into account its category. Use dag_node_dynamic_label(n) for the
                                                resources this node requests, taking into account categories,
                                                dynamic resources, etc.  */
    struct rmsummary *resources_allocated;   /* resources allocated to this node when submitted */
	struct rmsummary *resources_measured;    /* resources measured on completion. */

	/* Variables used in dag_width, dag_width_uniform_task, and dag_depth
	* functions. Probably we should move them only to those functions, using
	* hashes.*/
	int level;                          /* The depth of a node in the dag */
	int children;                       /* The number of nodes this node is the immediate ancestor */
	int children_remaining;
	int only_my_children;               /* Number of nodes this node is the only parent. */

	/* dynamic properties of execution */
	batch_queue_id_t jobid;               /* The id this node get, either from the local or remote batch system. */
	dag_node_state_t state;             /* Enum: DAG_NODE_STATE_{WAITING,RUNNING,...} */
	int failure_count;                  /* How many times has this rule failed? (see -R and -r) */
	time_t previous_completion;

	const char *umbrella_spec;          /* the umbrella spec file for executing this job */
	
	char *archive_id;

	struct batch_job *task;            /* Batch task associated with job at batch_submit. */

	struct dag_node *next;              /* The next node in the list of nodes */
};

struct dag_node *dag_node_create(struct dag *d, int linenum);
struct jx * dag_node_to_jx( struct dag *d, struct dag_node *n , int send_all_local_env);
void dag_node_delete(struct dag_node *n);
struct dag_node_size *dag_node_size_create(struct dag_node *n, uint64_t size);

void dag_node_add_source_file(struct dag_node *n, const char *filename, const char *remotename);
void dag_node_add_target_file(struct dag_node *n, const char *filename, const char *remotename);

void dag_node_set_command(struct dag_node *n, const char *cmd);
const char *dag_node_nested_workflow_filename(struct dag_node *n, const char *which_file);
void dag_node_set_workflow(struct dag_node *n, const char *dag, struct jx *args, int is_jx );
void dag_node_insert(struct dag_node *n);

uint64_t dag_node_file_list_size(struct list *s);
uint64_t dag_node_file_set_size(struct set *s);

const char *dag_node_get_remote_name(struct dag_node *n, const char *filename );
const char *dag_node_get_local_name(struct dag_node *n, const char *filename );

char *dag_node_resources_wrap_options(struct dag_node *n, const char *default_options, batch_queue_type_t batch_type);
char *dag_node_resources_wrap_as_rmonitor_options(struct dag_node *n);

void dag_node_init_resources(struct dag_node *n);
int dag_node_update_resources(struct dag_node *n, int overflow);
void dag_node_print_debug_resources(struct dag_node *n);

const char *dag_node_state_name(dag_node_state_t state);
void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate);

struct jx * dag_node_env_create( struct dag *d, struct dag_node *n, int should_send_all_local_environment );

const struct rmsummary *dag_node_dynamic_label(const struct dag_node *n);

void dag_node_set_umbrella_spec(struct dag_node *n, const char *umbrella_spec);

#endif
