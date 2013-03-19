/*
Copyright (C) 2008,2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>

#include "itable.h"

#include "timestamp.h"
#include "batch_job.h"

#ifndef MAKEFLOW_DAG_H
#define MAKEFLOW_DAG_H

#define MAX_REMOTE_JOBS_DEFAULT 100;

typedef enum {
	DAG_NODE_STATE_WAITING = 0,
	DAG_NODE_STATE_RUNNING = 1,
	DAG_NODE_STATE_COMPLETE = 2,
	DAG_NODE_STATE_FAILED = 3,
	DAG_NODE_STATE_ABORTED = 4,
	DAG_NODE_STATE_MAX = 5
} dag_node_state_t;

struct dag {
	char *filename;					// Source makeflow file path.
	struct dag_node *nodes;				// Linked list of all production rules, without ordering.
	struct itable *node_table;			// Mapping from unique integers dag_node->nodeid to nodes.
	struct itable *local_job_table;			// Mapping from unique integers dag_node->jobid  to nodes, rules with prefix LOCAL.
	struct itable *remote_job_table;		// Mapping from unique integers dag_node->jobid  to nodes.
	struct hash_table *file_table;			// Maps every filename to a struct dag_file.
	struct hash_table *completed_files;		// Records which target files have been updated/generated.
	struct list *symlinks_created;			// Remote filenames for which a symlink was created (used now only for Condor, and only for the final cleanup).
	struct hash_table *variables;			// Mappings between variables defined in the makeflow file and their substitution.
	struct hash_table *collect_table;		// Keeps the reference counts of filenames of files that are garbage collectable.
	struct list *export_list;			// List of variables with prefix export. (these are setenv'ed eventually).
	FILE *logfile;
	int node_states[DAG_NODE_STATE_MAX];		// node_states[STATE] keeps the count of nodes that have state STATE \in dag_node_state_t.
	int local_jobs_running;				// Count of jobs running locally.
	int local_jobs_max;				// Maximum number of jobs that can run locally (default load_average_get_cpus)
	int remote_jobs_running;			// Count of jobs running remotelly.
	int remote_jobs_max;				// Maximum number of jobs that can run remotelly (default at least max_remote_jobs_default)
	int nodeid_counter;				// Keeps a count of production rules read so far (used for the value of dag_node->nodeid).
};

/* Bookkeeping for parsing a makeflow file */
struct dag_parse {
	struct dag *d;
	FILE *dag_stream;
	char *linetext;
	int monitor_mode;
	int colnum;
	int linenum;
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
	struct dag *d;
	int only_my_children;
	time_t previous_completion;
	int linenum;
	int nodeid;
	int local_job;
	int nested_job;
	int failure_count;
	dag_node_state_t state;

	const char *command;
	const char *original_command;

	const char *makeflow_cwd;
	const char *makeflow_dag;
	const char *symbol;

	struct itable *remote_names;                     // Mapping from struct *dag_files to remotenames (char *)
	struct hash_table *remote_names_inv;	         // Mapping from remote filenames to dag_file representing the local file.

	struct list   *source_files;
	struct list   *target_files;

	batch_job_id_t jobid;
	struct dag_node *next;
	int children;
	int children_remaining;
	int level;
	struct hash_table *variables;
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
};

struct dag_lookup_set {
	struct dag *dag;
	struct dag_node *node;
	struct hash_table *table;
};

struct dag *dag_create();
struct dag_node *dag_node_create(struct dag *d, int linenum);
struct dag_file *dag_file_create(struct dag_node *n, const char *filename, const char *remotename);

struct list *dag_input_files(struct dag *d);

void dag_node_add_source_file(struct dag_node *n, const char *filename, char *remotename);
void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename);

const char *dag_node_add_remote_name(struct dag_node *n, const char *filename, const char *remotename);

void dag_count_states(struct dag *d);
const char *dag_node_state_name(dag_node_state_t state);
void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate);
char *dag_node_translate_filename(struct dag_node *n, const char *filename);

char *dag_file_remote_name(struct dag_node *n, const char *filename);
int dag_file_isabsolute(const struct dag_file *f);

char *dag_lookup(const char *name, void *arg);
char *dag_lookup_set(const char *name, void *arg);
#endif
