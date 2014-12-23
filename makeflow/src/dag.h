/*
Copyright (C) 2008,2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_DAG_H
#define MAKEFLOW_DAG_H

#include "dag_node.h"
#include "dag_file.h"

#include "itable.h"
#include "set.h"
#include "timestamp.h"
#include "batch_job.h"

#include <stdio.h>

#define MAX_REMOTE_JOBS_DEFAULT 100

struct dag {
    char *filename;                          /* Source makeflow file path. */
    struct dag_node *nodes;                  /* Linked list of all production rules, without ordering. */
    struct itable *node_table;               /* Mapping from unique integers dag_node->nodeid
                                                to nodes. */
    struct itable *local_job_table;          /* Mapping from unique integers dag_node->jobid
                                                to nodes, rules with prefix LOCAL. */
    struct itable *remote_job_table;         /* Mapping from unique integers dag_node->jobid
                                                to nodes. */
    struct hash_table *file_table;           /* Maps every filename to a struct dag_file. */
    struct hash_table *completed_files;      /* Records which target files have been
                                                updated/generated. */
	struct hash_table *variables;            /* Mappings between variable names
												defined in the makeflow file
												and their values. */
    struct set *collect_table;               /* Keeps files that are garbage collectable. */
    struct set *export_vars;                /* List of variables with prefix export.
                                                (these are setenv'ed eventually). */
	struct set *special_vars;                /* List of special variables,
												such as, category, disk,
												memory, etc. */
 
    FILE *logfile;
    int node_states[DAG_NODE_STATE_MAX];     /* node_states[STATE] keeps the count of nodes that
                                                have state STATE \in dag_node_state_t. */
    int local_jobs_running;                  /* Count of jobs running locally. */
    int local_jobs_max;                      /* Maximum number of jobs that can run
                                                locally (default load_average_get_cpus)*/
    int remote_jobs_running;                 /* Count of jobs running remotelly. */
    int remote_jobs_max;                     /* Maximum number of jobs that can run remotelly
                                                (default at least max_remote_jobs_default)*/
    int nodeid_counter;                      /* Keeps a count of production rules read so far
                                                (used for the value of dag_node->nodeid). */
    struct hash_table *task_categories;      /* Mapping from labels to category structures. */
};

/* Information of task categories. Label, number of tasks in this
   category, and maximum resources allowed. */
struct dag_task_category
{
	char *label;
	struct list *nodes;
};

struct dag_lookup_set {
    struct dag *dag;
    struct dag_task_category *category;
    struct dag_node *node;
    struct hash_table *table;
};

/* A makeflow variable may have different bindings, depending on where it was
 * defined. struct dag_variable keeps track of these values. When a
 * substitution is required in a rule, we look for the value binded just before
 * the rule (using binary search on either n->nodeid or d->nodeid_counter).
 */
struct dag_variable {
	int    count;
	struct dag_variable_value **values;
};

struct dag_variable_value {
	int   nodeid;                           /* The nodeid of the rule to which
	                                           this value binding takes effect. */
	int   size;                             /* memory size allocated for value */
	int   len;                              /* records strlen(value) */
	char *value;
};

struct dag *dag_create();

struct list *dag_input_files(struct dag *d);

void dag_compile_ancestors(struct dag *d);
void dag_find_ancestor_depth(struct dag *d);

void dag_count_states(struct dag *d);

struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename);
struct dag_file *dag_file_from_name(struct dag *d, const char *filename);

void dag_variable_add_value(const char *name, struct hash_table *current_table, int nodeid, const char *value);
struct dag_variable_value *dag_get_variable_value(const char *name, struct hash_table *t, int node_id);
struct dag_variable_value *dag_lookup(const char *name, void *arg);

char *dag_lookup_set(const char *name, void *arg);
char *dag_lookup_str(const char *name, void *arg);

struct dag_task_category *dag_task_category_lookup_or_create(struct dag *d, const char *label);

struct dag_variable_value *dag_variable_value_create(const char *value);
void dag_variable_value_free(struct dag_variable_value *v);
struct dag_variable_value *dag_variable_value_append_or_create(struct dag_variable_value *v, const char *value);

#endif
