/*
Copyright (C) 2008,2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>

#include "itable.h"
#include "set.h"

#include "timestamp.h"
#include "batch_job.h"

#ifndef MAKEFLOW_DAG_H
#define MAKEFLOW_DAG_H

#define MAX_REMOTE_JOBS_DEFAULT 100;

#define RESOURCES_CATEGORY "CATEGORY"

typedef enum {
	DAG_NODE_STATE_WAITING = 0,
	DAG_NODE_STATE_RUNNING = 1,
	DAG_NODE_STATE_COMPLETE = 2,
	DAG_NODE_STATE_FAILED = 3,
	DAG_NODE_STATE_ABORTED = 4,
	DAG_NODE_STATE_MAX = 5
} dag_node_state_t;

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
    struct list *symlinks_created;           /* Remote filenames for which a symlink was 
                                                created (used now only for Condor, and only for 
                                                the final cleanup). */
    struct hash_table *variables;            /* Mappings between variables defined in the makeflow 
                                                file and their substitution. */
    struct set *collect_table;               /* Keeps files that are garbage collectable. */
    struct list *export_list;                /* List of variables with prefix export. 
                                                (these are setenv'ed eventually). */
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

struct lexer_book
{
	struct dag *d;                      /* The dag being built. */

	struct dag_task_category *category; /* Indicates the category to which the rules belong. The
					       idea is to have rules that perform similar tasks, or
					       use about the same resources, to belong to the
					       same category. task_category is updated every time the
					       value of the variable described in the macro
					       MAKEFLOW_TASK_CATEGORY is changed in the makeflow file.
					    */

	FILE  *stream;                  /* The file pointer the rules are been read. */
	char *lexeme_end;

	char *lexeme;
	uint64_t lexeme_max; 
	uint64_t lexeme_size; 

	int   chunk_last_loaded;
	char *buffer;

	int substitution_mode;
	int eof;

	long int   line_number;
	long int   column_number;
	struct list *column_numbers;

	struct list *token_queue; 

	char *linetext;   //This member will be removed once the new lexer is integrated.
};


/* Information of task categories. Label, number of tasks in this
   category, and maximum resources allowed. */
struct dag_task_category
{
	char *label;
	struct rmsummary *resources;
	struct list *nodes;
	struct hash_table *variables;
};

/* struct dag_node implements a linked list of nodes. A dag_node
 * represents a production rule from source files to target
 * files. The actual dag structure is given implicitly by the
 * source_files and target_files members (i.e., a dag_node has no
 * explicit knowledge of its logical dag_node ascendants or descendants).
 * In fact, dag_node acts more like the edge of the dag, with the
 * nodes being sets of source/target files (that is, a file may
 * be part of different nodes).*/
struct dag_node {
    struct dag *d;                      /* Dag this node belongs too. */
    int nodeid;                         /* The ordinal number as the rule appears in the makeflow file */
    batch_job_id_t jobid;               /* The id this node get, either from the local or remote 
                                           batch system. */
    dag_node_state_t state;             /* Enum: DAG_NODE_STATE_{WAITING,RUNNING,...} */

    const char *command;                /* The command line with files of the shell io redirection 
                                           with remote names. */
    const char *original_command;       /* The command line as in the makeflow file */

    int failure_count;                  /* How many times has this rule failed? (see -R and -r) */
    time_t previous_completion;

    int linenum;                        /* Line number of the node's rule definition */

    int local_job;                      /* Flag: does this node runs locally? */

    struct set *descendants;           /* The nodes this node is an immediate ancestor */
    struct set *ancestors;             /* The nodes this node is an immediate descendant */ 
    int ancestor_depth;                /* The depth of the ancestor tree for this node */

    /* Support for recursive calls to makeflow. If this node calls makeflow
     * recursively, makeflow_dag is the name of the makeflow file to run, and
     * makeflow_cwd is the working directory. See * dag_parse_node_makeflow_command 
     * (why is this here?) */
    int nested_job;                     /* Flag: Is this a recursive call to
                                           makeflow? */
    const char *makeflow_dag;
    const char *makeflow_cwd;           

    struct itable *remote_names;        /* Mapping from struct *dag_files to remotenames (char *) */
    struct hash_table *remote_names_inv;/* Mapping from remote filenames to dag_file representing 
                                           the local file. */

    struct list   *source_files;        /* list of dag_files of the node's requirements */
    struct list   *target_files;        /* list of dag_files of the node's productions */

    struct dag_task_category *category; /* The set of task this node belongs too. Ideally, the makeflow 
                                           file labeled which tasks have comparable resource usage. */ 


    struct hash_table *variables;       /* This node settings for environment variables (@ syntax) */

    /* Variables used in dag_width, dag_width_uniform_task, and dag_depth
     * functions. Probably we should move them only to those functions, using
     * hashes.*/
    int level;                          /* The depth of a node in the dag */
    int children;                       /* The number of nodes this node is the immediate ancestor */
    int children_remaining;
    int only_my_children;               /* Number of nodes this node is the only parent. */


    struct dag_node *next;              /* The next node in the list of nodes */
};

/* struct dag_file represents a file, inpur or output, of the
 * workflow. filename is the path given in the makeflow file,
 * that is the local name of the file. Additionaly, dag_file
 * keeps track which nodes use the file as a source, and the
 * unique node, if any, that produces the file.
 */

struct dag_file {
    const char *filename;

    struct list     *needed_by;              /* List of nodes that have this file as a source */
    struct dag_node *target_of;              /* The node (if any) that created the file */

	int    ref_count;                        /* How many nodes still to run need this file */
};

struct dag_lookup_set {
    struct dag *dag;
    struct dag_task_category *category;
    struct dag_node *node;
    struct hash_table *table;
};


struct dag_variable_value {
	int   size;                             /* memory size allocated for value */
	int   len;                              /* records strlen(value) */
	char *value;
};

struct dag *dag_create();
struct dag_node *dag_node_create(struct dag *d, int linenum);
struct dag_file *dag_file_create(struct dag_node *n, const char *filename, const char *remotename);
struct dag_file *dag_file_lookup_or_create(struct dag *d, const char *filename);

struct list *dag_input_files(struct dag *d);

void dag_node_add_source_file(struct dag_node *n, const char *filename, char *remotename);
void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename);

const char *dag_node_add_remote_name(struct dag_node *n, const char *filename, const char *remotename);

void dag_compile_ancestors(struct dag *d);
void dag_find_ancestor_depth(struct dag *d);

int dag_file_is_source(struct dag_file *f);
int dag_file_is_sink(struct dag_file *f);
int dag_node_is_source(struct dag_node *n);
int dag_node_is_sink(struct dag_node *n);

void dag_count_states(struct dag *d);
const char *dag_node_state_name(dag_node_state_t state);
void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate);
char *dag_node_translate_filename(struct dag_node *n, const char *filename);

char *dag_file_remote_name(struct dag_node *n, const char *filename);
int dag_file_isabsolute(const struct dag_file *f);

struct dag_variable_value *dag_lookup(const char *name, void *arg);
char *dag_lookup_set(const char *name, void *arg);
char *dag_lookup_str(const char *name, void *arg);

struct dag_variable_value *dag_variable_value_create(const char *value);
void dag_variable_value_free(struct dag_variable_value *v);
struct dag_variable_value *dag_variable_value_append_or_create(struct dag_variable_value *v, const char *value);

struct dag_task_category *dag_task_category_lookup_or_create(struct dag *d, const char *label);
char *dag_task_category_wrap_options(struct dag_task_category *category, const char *default_options, batch_queue_type_t batch_type);
char *dag_task_category_wrap_as_rmonitor_options(struct dag_task_category *category);

void dag_task_category_get_env_resources(struct dag_task_category *category);
void dag_task_category_print_debug_resources(struct dag_task_category *category);

#endif
