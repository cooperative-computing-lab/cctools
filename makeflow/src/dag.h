/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_DAG_H
#define MAKEFLOW_DAG_H

#include "dag_node.h"
#include "dag_file.h"
#include "dag_variable.h"

#include "itable.h"
#include "string_set.h"
#include "set.h"
#include "timestamp.h"
#include "batch_queue.h"
#include "category.h"

#include <stdio.h>

struct dag {
	/* Static properties of the DAG */
	char *filename;                    /* Source makeflow file path. */
	struct dag_node *nodes;            /* Linked list of all production rules, without ordering. */
	struct itable *node_table;         /* Mapping from unique integers dag_node->nodeid to nodes. */
	struct hash_table *files;          /* Maps every filename to a struct dag_file. */
	struct set *inputs;                /* Set of every struct dag_file specified as input. */
	struct set *outputs;               /* Set of every struct dag_file specified as output. */
	struct hash_table *categories;     /* Mapping from labels to category structures. */
	struct category *default_category; /* Default for all rules and variables without an explicit category. */
	struct string_set *export_vars;    /* List of variables with prefix export. (these are setenv'ed eventually). */
	struct string_set *special_vars;   /* List of special variables, such as category, cores, memory, etc. */
	category_mode_t allocation_mode;   /* One of CATEGORY_ALLOCATION_MODE_{FIXED,MAX_THROUGHTPUT,MIN_WASTE} */


	/* Dynamic states related to execution via Makeflow. */
	FILE *logfile;
	int node_states[DAG_NODE_STATE_MAX];/* node_states[STATE] keeps the count of nodes that have state STATE \in dag_node_state_t. */
	int nodeid_counter;                 /* Keeps a count of production rules read so far (used for the value of dag_node->nodeid). */

	struct itable *local_job_table;     /* Mapping from unique integers dag_node->jobid to nodes, rules with prefix LOCAL. */
	struct itable *remote_job_table;    /* Mapping from unique integers dag_node->jobid to nodes. */
	int completed_files;                /* Keeps a count of the rules in state recieved or beyond. */
	int deleted_files;                  /* Keeps a count of the files delete in GC. */

	char *cache_dir;                    /* The dirname of the cache storing all the deps specified in the mountfile */

	uint64_t total_file_size;           /* Keeps cumulative size of existing files. */
};

struct dag *dag_create();

struct list *dag_input_files( struct dag *d );

void dag_compile_ancestors(struct dag *d);
void dag_find_ancestor_depth(struct dag *d);
void dag_count_states(struct dag *d);

struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename);
struct dag_file *dag_file_from_name(struct dag *d, const char *filename);

int dag_width( struct dag *d );
int dag_depth( struct dag *d );
int dag_width_guaranteed_max( struct dag *d );
int dag_width_uniform_task( struct dag *d );

int dag_remote_jobs_running( struct dag *d );
int dag_local_jobs_running( struct dag *d );

/* dag_mount_clean cleans up the mem space allocated due to the usage of mountfile
 * return 0 on success, return non-zero on failure.
 */
int dag_mount_clean( struct dag *d );


uint64_t dag_absolute_filesize( struct dag *d );

#endif
