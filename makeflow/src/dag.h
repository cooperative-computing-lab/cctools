/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_DAG_H
#define MAKEFLOW_DAG_H

#include "dag_node.h"
#include "dag_file.h"
#include "dag_variable.h"
#include "dag_task_category.h"

#include "itable.h"
#include "set.h"
#include "timestamp.h"
#include "batch_job.h"

#include <stdio.h>

struct dag {
	/* Static properties of the DAG */
	char *filename;                     /* Source makeflow file path. */
	struct dag_node *nodes;             /* Linked list of all production rules, without ordering. */
	struct itable *node_table;          /* Mapping from unique integers dag_node->nodeid to nodes. */
	struct hash_table *files;           /* Maps every filename to a struct dag_file. */
	struct set *inputs;                 /* Set of every struct dag_file specified as input. */
	struct set *outputs;                /* Set of every struct dag_file specified as output. */
	struct hash_table *variables;       /* Mappings between variable names defined in the makeflow file and their values. */
	struct hash_table *task_categories; /* Mapping from labels to category structures. */
	struct set *export_vars;            /* List of variables with prefix export. (these are setenv'ed eventually). */
	struct set *special_vars;           /* List of special variables, such as category, cores, memory, etc. */

	/* Dynamic states related to execution via Makeflow. */
	FILE *logfile;
	int node_states[DAG_NODE_STATE_MAX];/* node_states[STATE] keeps the count of nodes that have state STATE \in dag_node_state_t. */
	int nodeid_counter;                 /* Keeps a count of production rules read so far (used for the value of dag_node->nodeid). */

	struct itable *local_job_table;     /* Mapping from unique integers dag_node->jobid to nodes, rules with prefix LOCAL. */
	struct itable *remote_job_table;    /* Mapping from unique integers dag_node->jobid to nodes. */
	int completed_files;				/* Keeps a count of the rules in state recieved or beyond. */
};

struct dag *dag_create();

struct list *dag_input_files( struct dag *d );

void dag_compile_ancestors(struct dag *d);
void dag_find_ancestor_depth(struct dag *d);
void dag_count_states(struct dag *d);

struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename);
struct dag_file *dag_input_lookup_or_create(struct dag *d, const char *filename);
struct dag_file *dag_output_lookup_or_create(struct dag *d, const char *filename);
struct dag_file *dag_file_from_name(struct dag *d, const char *filename);
struct dag_task_category *dag_task_category_lookup_or_create(struct dag *d, const char *label);

int dag_width( struct dag *d, int nested );
int dag_depth( struct dag *d );
int dag_width_guaranteed_max( struct dag *d );
int dag_width_uniform_task( struct dag *d );

int dag_remote_jobs_running( struct dag *d );
int dag_local_jobs_running( struct dag *d );

#endif
