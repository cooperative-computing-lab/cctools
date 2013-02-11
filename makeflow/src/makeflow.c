/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include "cctools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "batch_job.h"
#include "work_queue.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "timestamp.h"
#include "xxmalloc.h"
#include "getopt_aux.h"

#define SHOW_INPUT_FILES 2
#define SHOW_OUTPUT_FILES 3
#define SHOW_MAKEFLOW_ANALYSIS 4 
#define RANDOM_PORT_RETRY_TIME 300

#define MAKEFLOW_AUTO_WIDTH 1
#define MAKEFLOW_AUTO_GROUP 2

#define	MAKEFLOW_MIN_SPACE 10*1024*1024	/* 10 MB */
#define MAKEFLOW_GC_MIN_THRESHOLD 1

typedef enum {
	DAG_GC_NONE,
	DAG_GC_REF_COUNT,
	DAG_GC_INCR_FILE,
	DAG_GC_INCR_TIME,
	DAG_GC_ON_DEMAND,
} dag_gc_method_t;

static int dag_abort_flag = 0;
static int dag_failed_flag = 0;
static int dag_submit_timeout = 3600;
static int dag_retry_flag = 0;
static int dag_retry_max = 100;

static dag_gc_method_t dag_gc_method = DAG_GC_NONE;
static int dag_gc_param = -1;
static int dag_gc_collected = 0;
static int dag_gc_barrier = 1;
static double dag_gc_task_ratio = 0.05;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static char *project = NULL;
static int priority = 0;
static int port = 0;
static const char *port_file = NULL;
static int output_len_check = 0;

static char *makeflow_exe = NULL;

typedef enum {
	DAG_NODE_STATE_WAITING = 0,
	DAG_NODE_STATE_RUNNING = 1,
	DAG_NODE_STATE_COMPLETE = 2,
	DAG_NODE_STATE_FAILED = 3,
	DAG_NODE_STATE_ABORTED = 4,
	DAG_NODE_STATE_MAX = 5
} dag_node_state_t;

struct dag {
	char *filename;
	struct dag_node *nodes;
	struct itable *node_table;
	struct itable *local_job_table;
	struct itable *remote_job_table;
	struct hash_table *file_table;
	struct hash_table *completed_files;
	struct hash_table *filename_translation_rev;
	struct hash_table *filename_translation_fwd;
	struct list *symlinks_created;
	struct hash_table *variables;
	struct hash_table *collect_table;
	struct list *export_list;
	FILE *logfile;
	FILE *dagfile;
	char *linetext;
	int node_states[DAG_NODE_STATE_MAX];
	int colnum;
	int linenum;
	int local_jobs_running;
	int local_jobs_max;
	int remote_jobs_running;
	int remote_jobs_max;
	int nodeid_counter;
};

struct dag_file {
	const char *filename;
	char *remotename;
	struct dag_file *next;
};

struct dag_node {
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
	struct dag_file *source_files;
	struct dag_file *target_files;
	int source_file_names_size;
	int target_file_names_size;
	batch_job_id_t jobid;
	struct dag_node *next;
	int children;
	int children_remaining;
	int level;
	struct hash_table *variables;
};

struct dag_lookup_set {
	struct dag *dag;
	struct dag_node *node;
	struct hash_table *table;
};

int dag_depth(struct dag *d);
int dag_width_uniform_task(struct dag *d);
int dag_width_guaranteed_max(struct dag *d);
int dag_width(struct dag *d, int nested_jobs);
void dag_node_complete(struct dag *d, struct dag_node *n, struct batch_job_info *info);
char *dag_readline(struct dag *d, struct dag_node *n);
int dag_check (struct dag *d);
int dag_check_dependencies(struct dag *d);
int dag_parse_variable(struct dag *d, struct dag_node *n, char *line);
int dag_parse_node(struct dag *d, char *line, int clean_mode);
int dag_parse_node_filelist(struct dag *d, struct dag_node *n, char *filelist, int source, int clean_mode);
int dag_parse_node_command(struct dag *d, struct dag_node *n, char *line);
int dag_parse_node_makeflow_command(struct dag *d, struct dag_node *n, char *line);
int dag_parse_export(struct dag *d, char *line);
char *dag_lookup(const char *name, void *arg);
char *dag_lookup_set(const char *name, void *arg);
void dag_gc_ref_incr(struct dag *d, const char *file, int increment);
void dag_gc_ref_count(struct dag *d, const char *file);
void dag_export_variables(struct dag *d, struct dag_node *n);

/** 
 * If the return value is x, a positive integer, that means at least x tasks
 * can be run in parallel during a certain point of the execution of the
 * workflow. The following algorithm counts the number of direct child nodes of
 * each node (a node represents a task). Node A is a direct child of Node B
 * only when Node B is the only parent node of Node A. Then it returns the
 * maximum among the direct children counts.
 */
int dag_width_guaranteed_max(struct dag *d)
{
	struct dag_node *n, *m, *tmp;
	struct dag_file *f;
	int nodeid;
	int depends_on_single_node = 1;
	int max = 0;

	for(n = d->nodes; n; n = n->next) {
		depends_on_single_node = 1;
		nodeid = -1;
		m = 0;
		// for each source file, see if it is a target file of another node
		for(f = n->source_files; f; f = f->next) {
			// d->file_table contains all target files
			// get the node (tmp) that outputs current source file
			tmp = hash_table_lookup(d->file_table, f->filename);
			// if a source file is also a target file
			if(tmp) {
				debug(D_DEBUG, "%d depends on %d", n->nodeid, tmp->nodeid);
				if(nodeid == -1) {
					m = tmp;	// m holds the parent node
					nodeid = m->nodeid;
					continue;
				}
				// if current node depends on multiple nodes, continue to process next node
				if(nodeid != tmp->nodeid) {
					depends_on_single_node = 0;
					break;
				}
			}
		}
		// m != 0 : current node depends on at least one exsisting node
		if(m && depends_on_single_node && nodeid != -1) {
			m->only_my_children++;
		}
	}

	// find out the maximum number of direct children that a single parent node has
	for(n = d->nodes; n; n = n->next) {
		max = max < n->only_my_children ? n->only_my_children : max;
	}

	return max;
}

/** 
 * returns the depth of the given DAG.
 */
int dag_depth(struct dag *d)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	struct list *level_unsolved_nodes = list_create();
	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;
		for(f = n->source_files; f != NULL; f = f->next) {
			if((parent = (struct dag_node *) hash_table_lookup(d->file_table, f->filename)) != NULL) {
				n->level = -1;
				list_push_tail(level_unsolved_nodes, n);
				break;
			}
		}
	}

	int max_level = 0;
	while((n = (struct dag_node *) list_pop_head(level_unsolved_nodes)) != NULL) {
		for(f = n->source_files; f != NULL; f = f->next) {
			if((parent = (struct dag_node *) hash_table_lookup(d->file_table, f->filename)) != NULL) {
				if(parent->level == -1) {
					n->level = -1;
					list_push_tail(level_unsolved_nodes, n);
					break;
				} else {
					int tmp_level = parent->level + 1;
					n->level = n->level > tmp_level ? n->level : tmp_level;
					max_level = n->level > max_level ? n->level : max_level;
				}	
			}
		}
	}
	list_delete(level_unsolved_nodes);
	
	return max_level+1;
}

/** 
 * This algorithm assumes all the tasks take the same amount of time to execute
 * and each task would be executed as early as possible. If the return value is
 * x, a positive integer, that means at least x tasks can be run in parallel
 * during a certain point of the execution of the workflow. 
 *
 * The following algorithm first determines the level (depth) of each node by
 * calling the dag_depth() function and then count how many nodes are there at
 * each level. Then it returns the maximum of the numbers of nodes at each
 * level.
 */
int dag_width_uniform_task(struct dag *d)
{
	struct dag_node *n;

	int depth = dag_depth(d);

	size_t level_count_array_size = (depth) * sizeof(int);
	int *level_count = malloc(level_count_array_size);
	if(!level_count) {
		return -1;
	}
	memset(level_count, 0, level_count_array_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i < depth; i++) {
		if(max < level_count[i]) {
			max = level_count[i];
		}
	}

	free(level_count);
	return max;
}

void dag_show_analysis(struct dag *d) {
	printf("num_of_tasks\t%d\n", itable_size(d->node_table));
	printf("depth\t%d\n", dag_depth(d));
	printf("width_uniform_task\t%d\n", dag_width_uniform_task(d));
	printf("width_guaranteed_max\t%d\n", dag_width_guaranteed_max(d));
}

void dag_show_input_files(struct dag *d)
{
	struct dag_node *n, *tmp;
	struct dag_file *f;
	struct hash_table *ih;
	char *key;
	void *value;

	ih = hash_table_create(0, 0);
	for(n = d->nodes; n; n = n->next) {
		// for each source file, see if it is a target file of another node
		for(f = n->source_files; f; f = f->next) {
			// d->file_table contains all target files
			// get the node (tmp) that outputs current source file
			tmp = hash_table_lookup(d->file_table, f->filename);
			// if a source file is also a target file
			if(!tmp) {
				debug(D_DEBUG, "Found independent input file: %s", f->filename);
				hash_table_insert(ih, f->filename, (void *) NULL);
			}
		}
	}

	hash_table_firstkey(ih);
	while(hash_table_nextkey(ih, &key, &value)) {
		printf("%s\n", key);
	}

	hash_table_delete(ih);
}

void dag_show_output_files(struct dag *d)
{
	char *key;
	void *value;

	hash_table_firstkey(d->file_table);

	while(hash_table_nextkey(d->file_table, &key, &value)) {
		printf("%s\n", key);
	}
}

/** 
 * Code added by kparting to compute the width of the graph. Original algorithm
 * by pbui, with improvements by kparting.
 */
int dag_width(struct dag *d, int nested_jobs)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	/* 1. Find the number of immediate children for all nodes; also,
	   determine leaves by adding nodes with children==0 to list. */

	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0; // initialize 'level' value to 0 because other functions might have modified this value.
		for(f = n->source_files; f != NULL; f = f->next) {
			parent = (struct dag_node *) hash_table_lookup(d->file_table, f->filename);
			if(parent)
				parent->children++;
		}
	}

	struct list *leaves = list_create();

	for(n = d->nodes; n != NULL; n = n->next) {
		n->children_remaining = n->children;
		if(n->children == 0)
			list_push_tail(leaves, n);
	}

	/* 2. Assign every node a "reverse depth" level. Normally by depth,
	   I mean topologically sort and assign depth=0 to nodes with no
	   parents. However, I'm thinking I need to reverse this, with depth=0
	   corresponding to leaves. Also, we want to make sure that no node is
	   added to the queue without all its children "looking at it" first
	   (to determine its proper "depth level"). */

	int max_level = 0;

	while(list_size(leaves) > 0) {
		struct dag_node *n = (struct dag_node *) list_pop_head(leaves);

		for(f = n->source_files; f != NULL; f = f->next) {
			parent = (struct dag_node *) hash_table_lookup(d->file_table, f->filename);
			if(!parent)
				continue;

			if(parent->level < n->level + 1)
				parent->level = n->level + 1;

			if(parent->level > max_level)
				max_level = parent->level;

			parent->children_remaining--;
			if(parent->children_remaining == 0)
				list_push_tail(leaves, parent);
		}
	}
	list_delete(leaves);

	/* 3. Now that every node has a level, simply create an array and then
	   go through the list once more to count the number of nodes in each
	   level. */

	size_t level_count_size = (max_level + 1) * sizeof(int);
	int *level_count = malloc(level_count_size);

	memset(level_count, 0, level_count_size);

	for(n = d->nodes; n != NULL; n = n->next) {
		if (nested_jobs && !n->nested_job)
			continue;
		level_count[n->level]++;
	}

	int i, max = 0;
	for(i = 0; i <= max_level; i++) {
		if(max < level_count[i])
			max = level_count[i];
	}

	free(level_count);
	return max;
}

struct dot_node {
	int id;
	int count;
	int print;
};

struct file_node {
	int id;
	char *name;
	double size;
};

void dag_print(struct dag *d, int condense_display, int change_size)
{
	struct dag_node *n;
	struct dag_file *f;
	struct hash_table *h, *g;
	struct dot_node *t;

	struct file_node *e;

	struct stat st;
	const char *fn;

	char *name;
	char *label;

        double average = 0;
        double width = 0;

        fprintf(stdout, "digraph {\n");

	if (change_size){
		dag_check(d);
		hash_table_firstkey(d->completed_files);
		while(hash_table_nextkey(d->completed_files, &label, (void**)&name)) {
			stat(label, &st);
			average+=((double) st.st_size)/((double) hash_table_size(d->completed_files));
		}
	}
		
	
	h = hash_table_create(0,0);

	fprintf(stdout, "node [shape=ellipse,color = green,style = unfilled,fixedsize = false];\n");

	for(n = d->nodes; n; n = n->next){
		name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);
		if (!t) {
			t = malloc(sizeof(*t));
			t->id = n->nodeid;
			t->count = 1;
			t->print = 1;
			hash_table_insert(h, label, t);
		} else {
			t->count++;
		}
		
		free(name);
	}
	

	for(n = d->nodes; n; n = n->next) {
        	name = xxstrdup(n->command);
		label = strtok(name, " \t\n");
		t = hash_table_lookup(h, label);
		if(!condense_display || t->print){

			if((t->count == 1) || !condense_display) fprintf(stdout, "N%d [label=\"%s\"];\n", condense_display?t->id:n->nodeid, label);
			else fprintf(stdout, "N%d [label=\"%s x%d\"];\n", t->id, label, t->count);
			t->print = 0;
		}
		free(name);
	}

	fprintf(stdout, "node [shape=box,color=blue,style=unfilled,fixedsize=false];\n");

	g = hash_table_create(0,0);

	for(n = d->nodes; n; n = n->next){
		for (f = n->source_files; f; f = f->next){
			fn = f->filename;
			e = hash_table_lookup(g, fn);
			if (!e) {
				e = malloc(sizeof(*e));
				e->id = hash_table_size(g);
				e->name = xxstrdup(fn);
				if (stat(fn, &st) == 0) {
					e->size = (double)(st.st_size);	
				}
				else e->size = -1;
				hash_table_insert(g, fn, e);
			}
		}
		for (f = n->target_files; f; f = f->next){
			fn = f->filename;
                        e = hash_table_lookup(g, fn);
                        if (!e) {
                                e = malloc(sizeof(*e));
                                e->id = hash_table_size(g);
				e->name = xxstrdup(fn);
                                if (stat(fn, &st) == 0){
					e->size = (double)(st.st_size);
				}
                                else e->size = -1;
				hash_table_insert(g, fn, e);
                        }
                }
	}

	hash_table_firstkey(g);
        while(hash_table_nextkey(g, &label ,(void **)&e)) {
		fn = e->name;
		fprintf(stdout, "F%d [label = \"%s", e->id, fn);

		if (change_size) { 
			if (e->size >= 0){
				width = 5*(e->size/average);
				if (width <2.5) width = 2.5;
                                if (width >25) width = 25;
				fprintf(stdout, "\\nsize:%.0lfkb\", style=filled, fillcolor=skyblue1, fixedsize=true, width=%lf, height=0.75", e->size/1024, width);
			} else {
				fprintf(stdout, "\", fixedsize = false, style = unfilled, ");
			}
		} else fprintf(stdout, "\"");

		fprintf(stdout, "];\n");
				
        }

	fprintf(stdout, "\n");

	for(n =	d->nodes; n; n = n->next) {

		name = xxstrdup(n->command);
                label = strtok(name, " \t\n");
                t = hash_table_lookup(h, label);


		for(f = n->source_files; f; f = f->next) {
			e = hash_table_lookup(g, f->filename);
			fprintf(stdout, "F%d -> N%d;\n", e->id, condense_display?t->id:n->nodeid);
		}
		for(f = n->target_files; f; f = f->next) {
			e = hash_table_lookup(g, f->filename);
			fprintf(stdout, "N%d -> F%d;\n", condense_display?t->id:n->nodeid, e->id);
		}

		free(name);
	}

	fprintf(stdout, "}\n");

	hash_table_firstkey(h);
	while(hash_table_nextkey(h, &label ,(void **)&t)) {
		free(t);
		hash_table_remove(h, label);
	}

	hash_table_firstkey(g);
        while(hash_table_nextkey(g, &label ,(void **)&e)) {
                free(e);
                hash_table_remove(g, label);
        }

	hash_table_delete(g);
	hash_table_delete(h);
}

const char *dag_node_state_name(dag_node_state_t state)
{
	switch (state) {
	case DAG_NODE_STATE_WAITING:
		return "waiting";
	case DAG_NODE_STATE_RUNNING:
		return "running";
	case DAG_NODE_STATE_COMPLETE:
		return "complete";
	case DAG_NODE_STATE_FAILED:
		return "failed";
	case DAG_NODE_STATE_ABORTED:
		return "aborted";
	default:
		return "unknown";
	}
}

struct dag_file *dag_file_create(const char *filename, char *remotename, struct dag_file *next)
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = xxstrdup(filename);
	if(remotename) {
		f->remotename = xxstrdup(remotename);
	} else {
		f->remotename = NULL;
	}
	f->next = next;
	return f;
}

void dag_node_add_source_file(struct dag_node *n, const char *filename, char *remotename)
{
	n->source_files = dag_file_create(filename, remotename, n->source_files);
}

void dag_node_add_target_file(struct dag_node *n, const char *filename, char *remotename)
{
	n->target_files = dag_file_create(filename, remotename, n->target_files);
}

void dag_count_states(struct dag *d)
{
	struct dag_node *n;
	int i;

	for(i = 0; i < DAG_NODE_STATE_MAX; i++) {
		d->node_states[i] = 0;
	}

	for(n = d->nodes; n; n = n->next) {
		d->node_states[n->state]++;
	}
}

void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate)
{
	debug(D_DEBUG, "node %d %s -> %s\n", n->nodeid, dag_node_state_name(n->state), dag_node_state_name(newstate));

	if(d->node_states[n->state] > 0) {
		d->node_states[n->state]--;
	}
	n->state = newstate;
	d->node_states[n->state]++;

        /**
	 * Line format : timestamp node_id new_state job_id nodes_waiting nodes_running nodes_complete nodes_failed nodes_aborted node_id_counter
	 *
	 * timestamp - the unix time (in microseconds) when this line is written to the log file.
	 * node_id - the id of this node (task).
	 * new_state - a integer represents the new state this node (whose id is in the node_id column) has just entered. The value of the integer ranges from 0 to 4 and the states they are representing are:
	 *	0. waiting
	 *	1. running
	 *	2. complete
	 *	3. failed
	 *	4. aborted
	 * job_id - the job id of this node in the underline execution system (local or batch system). If the makeflow is executed locally, the job id would be the process id of the process that executes this node. If the underline execution system is a batch system, such as Condor or SGE, the job id would be the job id assigned by the batch system when the task was sent to the batch system for execution.
	 * nodes_waiting - the number of nodes are waiting to be executed.
	 * nodes_running - the number of nodes are being executed.
	 * nodes_complete - the number of nodes has been completed.
	 * nodes_failed - the number of nodes has failed.
	 * nodes_aborted - the number of nodes has been aborted.
	 * node_id_counter - total number of nodes in this makeflow.
	 *
	 */
	fprintf(d->logfile, "%llu %d %d %d %d %d %d %d %d %d\n", timestamp_get(), n->nodeid, newstate, n->jobid, d->node_states[0], d->node_states[1], d->node_states[2], d->node_states[3], d->node_states[4], d->nodeid_counter);
}

void dag_abort_all(struct dag *d)
{
	UINT64_T jobid;
	struct dag_node *n;

	printf("got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table, &jobid, (void **) &n)) {
		printf("aborting local job %llu\n", jobid);
		batch_job_remove(local_queue, jobid);
		dag_node_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		printf("aborting remote job %llu\n", jobid);
		batch_job_remove(remote_queue, jobid);
		dag_node_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}
}

void file_clean(const char *filename, int silent)
{
	if(!filename)
		return;

	if(unlink(filename) == 0) {
		if(!silent)
			printf("deleted file %s\n", filename);
	} else {
		if(errno == ENOENT) {
			// nothing
		} else if(errno == EISDIR) {
			if(delete_dir(filename) != 0) {
				if(!silent)
					fprintf(stderr,"couldn't delete directory %s: %s\n", filename, strerror(errno));
			} else {
				if(!silent)
					printf("deleted directory %s\n", filename);
			}
		} else {
			if(!silent)
				fprintf(stderr, "couldn't delete %s: %s\n", filename, strerror(errno));
		}
	}
}

void dag_node_clean(struct dag *d, struct dag_node *n)
{
	struct dag_file *f;
	for(f = n->target_files; f; f = f->next) {

		file_clean(f->filename, 0);

		/* Make sure to clobber the original file too if it exists */
		char *name = (char *) hash_table_lookup(d->filename_translation_rev, f->filename);

		if(name)
			file_clean(name, 0);

		hash_table_remove(d->completed_files, f->filename);
	}

	/* If the node is a Makeflow job, then we should recursively call the
	 * clean operation on it. */
	if(n->nested_job) {
		char *command = xxmalloc(sizeof(char) * (strlen(n->command) + 4));
		sprintf(command, "%s -c", n->command);
		/* Export environment variables in case nested Makeflow
		 * requires them. */
		dag_export_variables(d, n);
		system(command);
		free(command);
	}
}

void clean_symlinks(struct dag *d, int silent)
{
	char *filename;

	if(batch_queue_type != BATCH_QUEUE_TYPE_CONDOR)
		return;

	while((filename = list_pop_head(d->symlinks_created))) {
		file_clean(filename, silent);
	}
}

void dag_clean(struct dag *d)
{
	struct dag_node *n;
	for(n = d->nodes; n; n = n->next)
		dag_node_clean(d, n);

	/* Since we are in clean mode, remove symlinks verbosely */
	clean_symlinks(d, 0);
}

void dag_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n);

/** 
 * Decide whether to rerun a node based on file system status
 */
void dag_node_decide_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n)
{
	struct stat filestat;
	struct dag_file *f;

	if(itable_lookup(rerun_table, n->nodeid))
		return;

	// Below are a bunch of situations when a node has to be rerun.

	// If a job was submitted to Condor, then just reconnect to it.
	if(n->state == DAG_NODE_STATE_RUNNING && !n->local_job && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		// Reconnect the Condor jobs
		printf("rule still running: %s\n", n->command);
		itable_insert(d->remote_job_table, n->jobid, n);
		d->remote_jobs_running++;

		// Otherwise, we cannot reconnect to the job, so rerun it
	} else if(n->state == DAG_NODE_STATE_RUNNING || n->state == DAG_NODE_STATE_FAILED || n->state == DAG_NODE_STATE_ABORTED) {
		printf("will retry failed rule: %s\n", n->command);
		goto rerun;
	}
	// Rerun if an input file has been updated since the last execution.
	for(f = n->source_files; f; f = f->next) {
		if(stat(f->filename, &filestat) >= 0) {
			if(S_ISDIR(filestat.st_mode))
				continue;
			if(difftime(filestat.st_mtime, n->previous_completion) > 0) {
				goto rerun;	// rerun this node
			}
		} else {
			if(!hash_table_lookup(d->file_table, f->filename)) {
				fprintf(stderr, "makeflow: input file %s does not exist and is not created by any rule.\n", f->filename);
				exit(1);
			} else {
				/* If input file is missing, but node completed and file was garbage, then avoid rerunning. */
				if(n->state == DAG_NODE_STATE_COMPLETE && hash_table_lookup(d->collect_table, f->filename)) {
					dag_gc_ref_incr(d, f->filename, -1);
					continue;
				}
				goto rerun;
			}
		}
	}

	// Rerun if an output file is missing.
	for(f = n->target_files; f; f = f->next) {
		if(stat(f->filename, &filestat) < 0) {
			/* If output file is missing, but node completed and file was garbage, then avoid rerunning. */
			if(n->state == DAG_NODE_STATE_COMPLETE && hash_table_lookup(d->collect_table, f->filename)) {
				continue;
			}
			goto rerun;
		}
	}

	// Do not rerun this node
	return;

      rerun:
	dag_node_force_rerun(rerun_table, d, n);
}

void dag_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n)
{
	struct dag_node *p;
	struct dag_file *f1;
	struct dag_file *f2;
	int child_node_found;

	if(itable_lookup(rerun_table, n->nodeid))
		return;

	// Mark this node as having been rerun already
	itable_insert(rerun_table, n->nodeid, n);

	// Remove running batch jobs
	if(n->state == DAG_NODE_STATE_RUNNING) {
		if(n->local_job) {
			batch_job_remove(local_queue, n->jobid);
			if(itable_remove(d->local_job_table, n->jobid)) {
				d->local_jobs_running--;
			}
		} else {
			batch_job_remove(remote_queue, n->jobid);
			if(itable_remove(d->remote_job_table, n->jobid)) {
				d->remote_jobs_running--;
			}
		}
	}
	// Clean up things associated with this node
	dag_node_clean(d, n);
	dag_node_state_change(d, n, DAG_NODE_STATE_WAITING);

	// For each parent node, rerun it if input file was garbage collected
	for(f1 = n->source_files; f1; f1 = f1->next) {
		if(hash_table_lookup(d->collect_table, f1->filename) == NULL)
			continue;

		p = hash_table_lookup(d->file_table, f1->filename);
		if (p) {
			dag_node_force_rerun(rerun_table, d, p);
			dag_gc_ref_incr(d, f1->filename, 1);
		}
	}

	// For each child node, rerun it
	for(f1 = n->target_files; f1; f1 = f1->next) {
		for(p = d->nodes; p; p = p->next) {
			child_node_found = 0;
			for(f2 = p->source_files; f2; f2 = f2->next) {
				if(!strcmp(f1->filename, f2->filename)) {
					child_node_found = 1;
					break;
				}
			}
			if(child_node_found) {
				dag_node_force_rerun(rerun_table, d, p);
			}
		}
	}
}


void dag_log_recover(struct dag *d, const char *filename)
{
	char *line;
	int nodeid, state, jobid;
	int first_run = 1;
	struct dag_node *n;
	timestamp_t previous_completion_time;

	d->logfile = fopen(filename, "r");
	if(d->logfile) {
		int linenum = 0;
		first_run = 0;

		while((line = get_line(d->logfile))) {
			linenum++;

			if(line[0] == '#')
				continue;
			if(sscanf(line, "%llu %d %d %d", &previous_completion_time, &nodeid, &state, &jobid) == 4) {
				n = itable_lookup(d->node_table, nodeid);
				if(n) {
					n->state = state;
					n->jobid = jobid;
					/* Log timestamp is in microseconds, we need seconds for diff. */
					n->previous_completion = (time_t) (previous_completion_time / 1000000);
					continue;
				}
			}

			fprintf(stderr, "makeflow: %s appears to be corrupted on line %d\n", filename, linenum);
			clean_symlinks(d, 1);
			exit(1);
		}
		fclose(d->logfile);
	}

	d->logfile = fopen(filename, "a");
	if(!d->logfile) {
		fprintf(stderr, "makeflow: couldn't open logfile %s: %s\n", filename, strerror(errno));
		clean_symlinks(d, 1);
		exit(1);
	}
	if(setvbuf(d->logfile, NULL, _IOLBF, BUFSIZ) != 0) {
		fprintf(stderr, "makeflow: couldn't set line buffer on logfile %s: %s\n", filename, strerror(errno));
		clean_symlinks(d, 1);
		exit(1);
	}

	if(first_run) {
		struct dag_file *f;
		struct dag_node *p;
		for(n = d->nodes; n; n = n->next) {
			/* Record node information to log */
			fprintf(d->logfile, "# NODE\t%d\t%s\n", n->nodeid, n->original_command);

			/* Record node symbol to log */
			if(n->symbol) {
				fprintf(d->logfile, "# SYMBOL\t%d\t%s\n", n->nodeid, n->symbol);
			}

			/* Record node parents to log */
			fprintf(d->logfile, "# PARENTS\t%d", n->nodeid);
			for(f = n->source_files; f; f = f->next) {
				p = hash_table_lookup(d->file_table, f->filename);
				if(p)
					fprintf(d->logfile, "\t%d", p->nodeid);
			}
			fputc('\n', d->logfile);

			/* Record node inputs to log */
			fprintf(d->logfile, "# SOURCES\t%d", n->nodeid);
			for(f = n->source_files; f; f = f->next) {
				fprintf(d->logfile, "\t%s", f->filename);
			}
			fputc('\n', d->logfile);

			/* Record node outputs to log */
			fprintf(d->logfile, "# TARGETS\t%d", n->nodeid);
			for(f = n->target_files; f; f = f->next) {
				fprintf(d->logfile, "\t%s", f->filename);
			}
			fputc('\n', d->logfile);

			/* Record translated command to log */
			fprintf(d->logfile, "# COMMAND\t%d\t%s\n", n->nodeid, n->command);
		}
	}

	dag_count_states(d);

	// Decide rerun tasks
	if(!first_run) {
		struct itable *rerun_table = itable_create(0);
		for(n = d->nodes; n; n = n->next) {
			dag_node_decide_rerun(rerun_table, d, n);
		}
		itable_delete(rerun_table);
	}
}

static int translate_filename(struct dag *d, const char *filename, char **newname_ptr)
{
	/* The purpose of this function is to translate an absolute path
	   filename into a unique slash-less name to allow for the sending
	   of any file to remote systems. Function returns 1 on success, 0 if
	   filename has already been translated. */

	if(!newname_ptr)
		return 0;

	/* If there are no slashes in path, then we don't need to translate. */
	if(!strchr(filename, '/')) {
		*newname_ptr = NULL;
		return 0;
	}

	/* If the filename is in the current directory and doesn't contain any
	 * additional slashes, then we can also skip translation.
	 *
	 * Note: this doesn't handle redundant ./'s such as ./././././foo/bar */
	if(!strncmp(filename, "./", 2) && !strchr(filename + 2, '/')) {
		*newname_ptr = NULL;
		return 0;
	}

	/* First check for if the filename has already been translated-- if so,
	   use that translation */

	char *newname;
	newname = (char *) hash_table_lookup(d->filename_translation_fwd, filename);

	if(newname) {		/* Filename has been translated before */
		char *temp = newname;
		newname = xxstrdup(temp);
		*newname_ptr = newname;
		return 0;
	}

	newname = xxstrdup(filename);
	char *c;

	for(c = newname; *c; ++c) {
		if(*c == '/')
			*c = '_';
	}

	for(c = newname; *c == '.'; ++c) {
		*c = '_';
	}

	while(!hash_table_insert(d->filename_translation_rev, newname, xxstrdup(filename))) {
		/* It's not 100% collision-proof, technically, but the odds of
		   an unresolvable collision are unbelievably slim. */

		c = strchr(newname, '_');
		if(c) {
			*c = '~';
		} else {
			c = strchr(newname, '~');
			if(c) {
				*c = '-';
			} else {
				*newname_ptr = NULL;
				return 0;
			}
		}
	}

	hash_table_insert(d->filename_translation_fwd, filename, xxstrdup(newname));

	*newname_ptr = newname;
	return 1;
}

static char *translate_command(struct dag *d, char *old_command, int is_local)
{
	char *new_command;
	char *sp;
	char *token;
	int first = 1;
	int wait = 0;		/* Wait for next token before prepending "./"? */
	int padding = 3;
	char prefix;

	UPTRINT_T current_length = (UPTRINT_T) 0;

	for(sp = old_command; *sp; sp++)
		if(isspace((int) *sp))
			padding += 2;

	new_command = malloc((strlen(old_command) + padding) * sizeof(char));
	new_command[0] = '\0';

	token = strtok(old_command, " \t\n");

	while(token) {
		/* Remove (and store) the shell metacharacter prefix, if
		   there is one. */
		switch (token[0]) {
		case '<':
		case '>':
			prefix = token[0];
			++token;
			break;
		default:
			prefix = '\0';
		}

		if(prefix && !token) {
			/* Indicates "< input" or "> output", i.e., with
			   space after the shell metacharacter */
			wait = 1;
		}

		char *val = NULL;
		int len;

		if(!is_local)
			val = (char *) hash_table_lookup(d->filename_translation_fwd, token);

		if(!first) {
			strncat(new_command + current_length, " ", 1);
			++current_length;
		} else {
			first = 0;
		}

		/* Append the shell metacharacter prefix, if there is one. */
		if(prefix) {
			strncat(new_command + current_length, &prefix, 1);
			++current_length;
		}

		if(val) {
			/* If the executable has a hashtable entry, then we
			   need to prepend "./" to the symlink name */
			if(wait) {
				wait = 0;
			} else {
				strncat(new_command + current_length, "./", 2);
				current_length += 2;
			}

			len = strlen(val);
			strncat(new_command + current_length, val, len);
			current_length += len;
		} else {
			len = strlen(token);
			strncat(new_command + current_length, token, len);
			current_length += len;
		}

		token = strtok(NULL, " \t\n");
	}

	return new_command;
}

struct dag *dag_create()
{
	struct dag *d = malloc(sizeof(*d));
	memset(d, 0, sizeof(*d));
	d->nodes = 0;
	d->colnum = 0;
	d->linenum = 0;
	d->linetext = NULL;
	d->filename = NULL;
	d->dagfile = NULL;
	d->node_table = itable_create(0);
	d->local_job_table = itable_create(0);
	d->remote_job_table = itable_create(0);
	d->file_table = hash_table_create(0, 0);
	d->completed_files = hash_table_create(0, 0);
	d->symlinks_created = list_create();
	d->variables = hash_table_create(0, 0);
	d->local_jobs_running = 0;
	d->local_jobs_max = 1;
	d->remote_jobs_running = 0;
	d->remote_jobs_max = 100;
	d->nodeid_counter = 0;
	d->filename_translation_rev = hash_table_create(0, 0);
	d->filename_translation_fwd = hash_table_create(0, 0);
	d->collect_table = hash_table_create(0, 0);
	d->export_list = list_create();

	/* Add _MAKEFLOW_COLLECT_LIST to variables table to ensure it is in
	 * global DAG scope. */
	hash_table_insert(d->variables, "_MAKEFLOW_COLLECT_LIST", xxstrdup(""));

	memset(d->node_states, 0, sizeof(int) * DAG_NODE_STATE_MAX);
	return d;
}

struct dag_node *dag_node_create(struct dag *d)
{
	struct dag_node *n;

	n = malloc(sizeof(struct dag_node));
	memset(n, 0, sizeof(struct dag_node));
	n->linenum = d->linenum;
	n->state = DAG_NODE_STATE_WAITING;
	n->nodeid = d->nodeid_counter++;
	n->variables = hash_table_create(0, 0);

	return n;
}

#define dag_parse_error(dag, type) \
	fprintf(stderr, "makeflow: invalid " type " in file %s at line %d, column %d:\n%s\n", (dag)->filename, (dag)->linenum, (dag)->colnum, (dag)->linetext ? (dag)->linetext : "");

int dag_parse(struct dag *d, const char *filename, int clean_mode)
{
	char *line = NULL;

	d->dagfile = fopen(filename, "r");
	if(d->dagfile == NULL) {
		fprintf(stderr, "makeflow: unable to open file %s: %s\n", filename, strerror(errno));
		goto failure;
	}

	d->linenum = 0;
	free(d->filename);
	d->filename = xxstrdup(filename);

	while((line = dag_readline(d, NULL)) != NULL) {
		
		if (strlen(line) == 0  || line[0] == '#' ) {
			/* Skip blank lines and comments */
			free(line);
			continue;
		}
		if(strncmp(line, "export ", 7) == 0) {
			if(!dag_parse_export(d, line)) {
				dag_parse_error(d, "export");
				goto failure;
			}
		} else if(strchr(line, '=')) {
			if(!dag_parse_variable(d, NULL, line)) {
				dag_parse_error(d, "variable");
				goto failure;
			}
		} else if(strstr(line, ":")) {
			if(!dag_parse_node(d, line, clean_mode)) {
				dag_parse_error(d, "node");
				goto failure;
			}
		} else {
			dag_parse_error(d, "syntax");
			goto failure;
		}

		free(line);
	}

	free(line);
	fclose(d->dagfile);
	return dag_check_dependencies(d);

failure:
	free(line);
	if(d->dagfile) fclose(d->dagfile);
	return 0;
}

int dag_check_dependencies(struct dag *d)
{
	struct dag_node *n, *m;
	struct dag_file *f;

	/* Walk list of nodes and associate target files with the nodes that
	 * generate them. */
	for(n = d->nodes; n; n = n->next) {
		for(f = n->target_files; f; f = f->next) {
			m = hash_table_lookup(d->file_table, f->filename);
			if(m) {
				fprintf(stderr, "makeflow: %s is defined multiple times at %s:%d and %s:%d\n", f->filename, d->filename, n->linenum, d->filename, m->linenum);
				errno = EINVAL;
				return 0;
			} else {
				hash_table_insert(d->file_table, f->filename, n);
			}
		}
	}

	return 1;
}

void dag_prepare_gc(struct dag *d)
{
	/* Parse _MAKEFLOW_COLLECT_LIST and record which target files should be
	 * garbage collected. */
	char *collect_list = dag_lookup("_MAKEFLOW_COLLECT_LIST", d);
	if(collect_list == NULL)
		return;

	int i, argc;
	char **argv;
	string_split_quotes(collect_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		/* Must initialize to non-zero for hash_table functions to work properly. */
		hash_table_insert(d->collect_table, argv[i], (void *)MAKEFLOW_GC_MIN_THRESHOLD);
		debug(D_DEBUG, "Added %s to garbage collection list", argv[i]);
	}
	free(argv);

	/* Mark garbage files with reference counts. This will be used to
	 * detect when it is safe to remove a garbage file. */
	struct dag_node *n;
	struct dag_file *f;
	for(n = d->nodes; n; n = n->next)
		for(f = n->source_files; f; f = f->next)
			dag_gc_ref_incr(d, f->filename, 1);
}

void dag_prepare_nested_jobs(struct dag *d)
{
	/* Update nested jobs with appropriate number of local jobs (total
	 * local jobs max / maximum number of concurrent nests). */
	int dag_nested_width = dag_width(d, 1);
	int update_dag_nests = 1;
	char *s = getenv("MAKEFLOW_UPDATE_NESTED_JOBS");
	if(s)
		update_dag_nests = atoi(s);

	if(dag_nested_width > 0 && update_dag_nests) {
		dag_nested_width = MIN(dag_nested_width, d->local_jobs_max);
		struct dag_node *n;
		for(n = d->nodes; n; n = n->next) {
			if(n->nested_job && (n->local_job || batch_queue_type == BATCH_QUEUE_TYPE_LOCAL)) {
				char *command = xxmalloc(strlen(n->command) + 20);
				sprintf(command, "%s -j %d", n->command, d->local_jobs_max / dag_nested_width);
				free((char *)n->command);
				n->command = command;
			}
		}
	}
}

char *dag_readline(struct dag *d, struct dag_node *n)
{
	struct dag_lookup_set s = {d, n, NULL};
	char *raw_line = get_line(d->dagfile);

	if(raw_line) {
		d->colnum = 1;
		d->linenum++;

		if(d->linenum % 1000 == 0) {
			debug(D_DEBUG, "read line %d\n", d->linenum);
		}

		/* Strip whitespace */
		string_chomp(raw_line);
		while(isspace(*raw_line)) {
			raw_line++;
			d->colnum++;
		}

		/* Chop off comments
		 * TODO: this will break if we use # in a string. */
		char *hash = strrchr(raw_line, '#');
		if (hash && hash != raw_line) {
			*hash = 0;
		}

		char *subst_line = xxstrdup(raw_line);
		subst_line = string_subst(subst_line, dag_lookup_set, &s);

		free(d->linetext);
		d->linetext = xxstrdup(subst_line);

		/* Expand backslash-escaped characters. */
		/* NOTE: This function call is responsible for translating escape
		character sequences such as \n, \t, etc. which are found in the
		makeflow file into their ASCII character equivalents. Such escape
		sequences are necessary for assigning values to variables which
		contain multiple lines of text, since the entire assignment
		statement must be contained on one line. */
		string_replace_backslash_codes(subst_line, subst_line);

		return subst_line;
	}

	return NULL;
}

int dag_parse_variable(struct dag *d, struct dag_node *n, char *line)
{
	char *name  = line + (n ? 1 : 0); /* Node variables require offset of 1 */
	char *value = NULL;
	char *equal = NULL;
	int append = 0;

	equal = strchr(line, '=');
	if((value = strstr(line, "+=")) && value < equal) {
		*value = 0;
		value  = value + 2;
		append = 1;
	} else {
		value  = equal;
		*value = 0;
		value  = value + 1;
	}

	name  = string_trim_spaces(name);
	value = string_trim_spaces(value);
	value = string_trim_quotes(value);

	if(strlen(name) == 0) {
		dag_parse_error(d, "variable name");
		return 0;
	}

	struct dag_lookup_set s = {d, n, NULL};
	char *old_value = (char *)dag_lookup_set(name, &s);
	if(append && old_value) {
		char *new_value = NULL;
		if(s.table) {
			free(hash_table_remove(s.table, name));
		} else {
			s.table = d->variables;
		}
		new_value = calloc(strlen(old_value) + strlen(value) + 2, sizeof(char));
		new_value = strcpy(new_value, old_value);
		new_value = strcat(new_value, " ");
		free(old_value);
		value = strcat(new_value, value);
		hash_table_insert(s.table, name, value);
	} else {
		hash_table_insert((n ? n->variables : d->variables), name, xxstrdup(value));
	}

	debug(D_DEBUG, "%s variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
	return 1;
}

int dag_parse_node(struct dag *d, char *line, int clean_mode)
{
	char *outputs = line;
	char *inputs  = NULL;
	struct dag_node *n;

	n = dag_node_create(d);

	inputs  = strchr(line, ':');
	*inputs = 0;
	inputs  = inputs + 1;

	outputs = string_trim_spaces(outputs);
	inputs  = string_trim_spaces(inputs);

	dag_parse_node_filelist(d, n, outputs, 0, clean_mode);
	dag_parse_node_filelist(d, n, inputs, 1, clean_mode);

	while((line = dag_readline(d, n)) != NULL) {
		if(line[0] == '#') {
			if(strncmp(line, "# SYMBOL", 8) == 0) {
				char *symbol = strchr(line, '\t');
				if(symbol) {
					n->symbol = xxstrdup(symbol + 1);
					debug(D_DEBUG, "node symbol=%s", n->symbol);
				}
			}
		} else if(line[0] == '@' && strchr(line, '=')) {
			if(!dag_parse_variable(d, n, line)) {
				dag_parse_error(d, "node variable");
				free(line);
				return 0;
			}
		} else {
			if(dag_parse_node_command(d, n, line)) {
				n->next = d->nodes;
				d->nodes = n;
				itable_insert(d->node_table, n->nodeid, n);
				free(line);
				return 1;
			} else {
				dag_parse_error(d, "node command");
				free(line);
				return 0;
			}
		}
		free(line);
	}

	free(line);
	return 1;
}

/** Parse a node's input or output filelist.
Parse through a list of input or output files, adding each as a source or target file to the provided node.
@param d The DAG being constructed
@param n The node that the files are being added to
@param filelist The list of files, separated by whitespace
@param source a flag for whether the files are source or target files.  1 indicates source files, 0 indicates targets
@param clean_mode a flag for whether the DAG is being constructed for cleaning or running.
*/
int dag_parse_node_filelist(struct dag *d, struct dag_node *n, char *filelist, int source, int clean_mode)
{
	char *filename;
	char *newname;
	char **argv;
	int i, argc;

	string_split_quotes(filelist, &argc, &argv);
	for (i = 0; i < argc; i++) {
		filename = argv[i];
		newname  = NULL;
		debug(D_DEBUG, "node %s file=%s", (source ? "input" : "output"), filename);

		// remote renaming
		if((newname = strstr(filename, "->"))) {
			*newname = '\0';
			newname+=2;
		}
		
		if(source) {
			dag_node_add_source_file(n, filename, newname);
			n->source_file_names_size += strlen(filename) + (newname?strlen(newname):0) + 2;
		} else {
			dag_node_add_target_file(n, filename, newname);
			n->target_file_names_size += strlen(filename) + (newname?strlen(newname):0) + 2;
		}
		
	}
	free(argv);
	return 1;

}

int dag_prepare_for_batch_system(struct dag *d) {

	struct dag_node *n;
	struct dag_file *f;
	int source;
	int rv;
	
	for(n = d->nodes; n; n = n->next) {
		source = 1;
		for(f = n->source_files; f; ) {
			switch(batch_queue_type) {
			
			case BATCH_QUEUE_TYPE_CONDOR:
				
				rv = 0;
				if(strchr(f->filename, '/') && !f->remotename) {
					rv = translate_filename(d, f->filename, &(f->remotename));
					if(source) {
						n->source_file_names_size += strlen(f->remotename);
					} else {
						n->target_file_names_size += strlen(f->remotename);
					}
				} else if(f->remotename) {
					rv = 1;
				}
				
				if(rv) {
					debug(D_DEBUG,"creating symlink \"./%s\" for file \"%s\"\n", f->remotename, f->filename);
					rv = symlink(f->filename, f->remotename);
					if(rv < 0) {
						if(errno != EEXIST) {
							fprintf(stderr, "makeflow: could not create symbolic link (%s)\n", strerror(errno));
							goto failure;
						} else {
							int link_size = strlen(f->filename)+2;
							char *link_contents = malloc(link_size);
							
							link_size = readlink(f->remotename, link_contents, link_size);
							if(!link_size || strncmp(f->filename, link_contents, link_size)) {
								fprintf(stderr, "makeflow: symbolic link %s points to wrong file (\"%s\" instead of \"%s\")\n", f->remotename, link_contents, f->filename);
								free(link_contents);
								goto failure;
							}
							free(link_contents);
						}
					} else {
						list_push_tail(d->symlinks_created, f->remotename);
					}
	
					/* Create symlink target stub for output files, otherwise Condor will fail on write-back */
					if(!source && access(f->filename, R_OK) < 0) {
						int fd = open(f->filename, O_WRONLY | O_CREAT | O_TRUNC, 0700);
						if(fd < 0) {
							fprintf(stderr, "makeflow: could not create symbolic link target (%s): %s\n", f->filename, strerror(errno));
							goto failure;
						}
						close(fd);
					}
				}
				 
				break;
	
			case BATCH_QUEUE_TYPE_WORK_QUEUE:
				
				if(f->filename[0] == '/' && !f->remotename) {
					/* Translate only explicit absolute paths for Work Queue tasks.*/
					translate_filename(d, f->filename, &(f->remotename));
					debug(D_DEBUG, "translating work queue absolute path (%s) -> (%s)", f->filename, f->remotename);
					
					if(source) {
						n->source_file_names_size += strlen(f->remotename);
					} else {
						n->target_file_names_size += strlen(f->remotename);
					}
				}
				break;
				
			default:

				if(f->remotename) {
					fprintf(stderr, "makeflow: automatic file renaming (%s=%s) only works with Condor or Work Queue drivers\n", f->filename, f->remotename);
					goto failure;
				}
				break;
				
			}

			f = f->next;
			
			/* If we're done with the source files, switch to checking target files */
			if(!f && source) {
				f = n->target_files;
				source = 0;
			}
		}
	}
failure:
	return 0;
}

void dag_parse_node_set_command(struct dag *d, struct dag_node *n, char *command)
{
	struct dag_lookup_set s = {d, n};
	char *local = dag_lookup_set("BATCH_LOCAL", &s);

	if (local) {
		if(string_istrue(local))
			n->local_job = 1;
		free(local);
	}

	n->original_command = xxstrdup(command);
	n->command = translate_command(d, command, n->local_job);
	debug(D_DEBUG, "node command=%s", n->command);
}

int dag_parse_node_command(struct dag *d, struct dag_node *n, char *line)
{
	char *command = line;

	while(*command && isspace(*command))
		command++;

	if(strncmp(command, "LOCAL ", 6) == 0) {
		n->local_job = 1;
		command += 6;
	}
	if(strncmp(command, "MAKEFLOW ", 9) == 0) {
		return dag_parse_node_makeflow_command(d, n, command + 9);
	}

	dag_parse_node_set_command(d, n, command);
	return 1;
}

int dag_parse_node_makeflow_command(struct dag *d, struct dag_node *n, char *line)
{
	int argc;
	char **argv;
	char *wrapper = NULL;
	char *command = NULL;

	n->nested_job = 1;
	string_split_quotes(line, &argc, &argv);
	switch(argc) {
		case 1:
			n->makeflow_dag = xxstrdup(argv[0]);
			n->makeflow_cwd = xxstrdup(".");
			break;
		case 2:
			n->makeflow_dag = xxstrdup(argv[0]);
			n->makeflow_cwd = xxstrdup(argv[1]);
			break;
		case 3:
			n->makeflow_dag = xxstrdup(argv[0]);
			n->makeflow_cwd = xxstrdup(argv[1]);
			wrapper = argv[2];
			break;
		default:
			dag_parse_error(d, "node makeflow command");
			goto failure;
	}

	wrapper = wrapper ? wrapper : "";
	command = xxmalloc(sizeof(char) * (strlen(n->makeflow_cwd) + strlen(wrapper) + strlen(makeflow_exe) + strlen(n->makeflow_dag) + 20));
	sprintf(command, "cd %s && %s %s %s", n->makeflow_cwd, wrapper, makeflow_exe, n->makeflow_dag);

	dag_parse_node_filelist(d, n, argv[0], 1, 0);
	dag_parse_node_set_command(d, n, command);

	free(argv);
	free(command);
	return 1;
failure:
	free(argv);
	return 0;
}

int dag_parse_export(struct dag *d, char *line)
{
	int i, argc;
	char *end_export, *equal;
	char **argv;

	end_export = strstr(line, "export ");

	if(!end_export)
		return 0;
	else
		end_export += strlen("export ");

	while(isblank(*end_export))
		end_export++;

	if(end_export == '\0')
		return 0;

	string_split_quotes(end_export, &argc, &argv);
	for(i = 0; i < argc; i++) {
		equal = strchr(argv[i], '=');
		if(equal)
		{
			if(!dag_parse_variable(d, NULL, argv[i]))
			{
				return 0;
			}
			else
			{
				*equal = '\0';
				setenv(argv[i], equal + 1, 1); //this shouldn't be here...
			}
		}
		list_push_tail(d->export_list, xxstrdup(argv[i]));
		debug(D_DEBUG, "export variable=%s", argv[i]);
	}
	free(argv);
	return 1;
}

char *dag_lookup(const char *name, void *arg)
{
	struct dag_lookup_set s = {(struct dag *)arg, NULL, NULL};
	return dag_lookup_set(name, &s);
}

char *dag_lookup_set(const char *name, void *arg)
{
	struct dag_lookup_set *s = (struct dag_lookup_set *)arg;
	const char *value;

	/* Try node variables table */
	if(s->node) {
		value = (const char *)hash_table_lookup(s->node->variables, name);
		if(value) {
			s->table = s->node->variables;
			return xxstrdup(value);
		}
	}

	/* Try dag variables table */
	if(s->dag) {
		value = (const char *)hash_table_lookup(s->dag->variables, name);
		if(value) {
			s->table = s->dag->variables;
			return xxstrdup(value);
		}
	}

	/* Try environment */
	value = getenv(name);
	if(value) {
		return xxstrdup(value);
	}

	return NULL;
}

void dag_export_variables(struct dag *d, struct dag_node *n)
{
	struct dag_lookup_set s = {d, n, NULL};
	char *key;

	list_first_item(d->export_list);
	while((key = list_next_item(d->export_list))) {
		char *value = dag_lookup_set(key, &s);
		if(value) {
			setenv(key, value, 1);
			debug(D_DEBUG, "export %s=%s", key, value);
		}
	}
}

void dag_node_submit(struct dag *d, struct dag_node *n)
{
	char *input_files = NULL;
	char *output_files = NULL;
	struct dag_file *f;

	struct batch_queue *thequeue;

	if(n->local_job) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}

	printf("%s\n", n->command);

	input_files = malloc((n->source_file_names_size + 1) * sizeof(char));
	memset(input_files, 0, n->source_file_names_size + 1);
	for(f = n->source_files; f; f = f->next) {
		switch(batch_queue_get_type(thequeue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			strcat(input_files, f->filename);
			if(f->remotename) {
				strcat(input_files, "=");
				strcat(input_files, f->remotename);
			}
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			if(f->remotename) {
				strcat(input_files, f->remotename);
			} else {
				strcat(input_files, f->filename);
			}
			break;
		default:
			strcat(input_files, f->filename);
		}
		
		strcat(input_files, ",");
	}

	output_files = malloc((n->target_file_names_size + 1) * sizeof(char));
	memset(output_files, 0, n->target_file_names_size + 1);
	for(f = n->target_files; f; f = f->next) {
		switch(batch_queue_get_type(thequeue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			if(f->remotename) {
				strcat(output_files, f->remotename);
				strcat(output_files, "=");
			}
			strcat(output_files, f->filename);
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			if(f->remotename) {
				strcat(output_files, f->remotename);
			} else {
				strcat(output_files, f->filename);
			}
			break;
		default:
			strcat(output_files, f->filename);
		}
		strcat(output_files, ",");
	}

	/* Before setting the batch job options (stored in the "BATCH_OPTIONS"
	 * variable), we must save the previous global queue value, and then
	 * restore it after we submit. */
	struct dag_lookup_set s = {d, n, NULL};
	char *batch_submit_options = dag_lookup_set("BATCH_OPTIONS", &s);
	char *old_batch_submit_options = NULL;

	if(batch_submit_options) {
		old_batch_submit_options = batch_queue_options(thequeue);
		batch_queue_set_options(thequeue, batch_submit_options);
		free(batch_submit_options);
	}

	time_t stoptime = time(0) + dag_submit_timeout;
	int waittime = 1;

	/* Export variables before each submit. We have to do this before each
	 * node submission because each node may have local variables
	 * definitions. */
	dag_export_variables(d, n);

	while(1) {
		n->jobid = batch_job_submit_simple(thequeue, n->command, input_files, output_files);
		if(n->jobid >= 0)
			break;

		fprintf(stderr,"couldn't submit batch job, still trying...\n");

		if(time(0) > stoptime) {
			fprintf(stderr,"unable to submit job after %d seconds!\n", dag_submit_timeout);
			break;
		}

		sleep(waittime);
		waittime *= 2;
		if(waittime > 60)
			waittime = 60;
	}

	/* Restore old batch job options. */
	if (old_batch_submit_options) {
		batch_queue_set_options(thequeue, old_batch_submit_options);
		free(old_batch_submit_options);
	}

	if(n->jobid >= 0) {
		dag_node_state_change(d, n, DAG_NODE_STATE_RUNNING);
		if(n->local_job) {
			itable_insert(d->local_job_table, n->jobid, n);
			d->local_jobs_running++;
		} else {
			itable_insert(d->remote_job_table, n->jobid, n);
			d->remote_jobs_running++;
		}
	} else {
		dag_node_state_change(d, n, DAG_NODE_STATE_FAILED);
		dag_failed_flag = 1;
	}

	free(input_files);
	free(output_files);
}

int dag_node_ready(struct dag *d, struct dag_node *n)
{
	struct dag_file *f;

	if(n->state != DAG_NODE_STATE_WAITING)
		return 0;

	if(n->local_job) {
		if(d->local_jobs_running >= d->local_jobs_max)
			return 0;
	} else {
		if(d->remote_jobs_running >= d->remote_jobs_max)
			return 0;
	}

	for(f = n->source_files; f; f = f->next) {
		if(hash_table_lookup(d->completed_files, f->filename)) {
			continue;
		} else {
			return 0;
		}
	}

	return 1;
}

void dag_dispatch_ready_jobs(struct dag *d)
{
	struct dag_node *n;

	for(n = d->nodes; n; n = n->next) {

		if(d->remote_jobs_running >= d->remote_jobs_max && d->local_jobs_running >= d->local_jobs_max)
			break;

		if(dag_node_ready(d, n)) {
			dag_node_submit(d, n);
		}
	}
}

void dag_node_complete(struct dag *d, struct dag_node *n, struct batch_job_info *info)
{
	struct dag_file *f;
	int job_failed = 0;

	struct stat stat_info;

	if(n->state != DAG_NODE_STATE_RUNNING)
		return;

	if(n->local_job) {
		d->local_jobs_running--;
	} else {
		d->remote_jobs_running--;
	}

	if(info->exited_normally && info->exit_code == 0) {
		for(f = n->target_files; f; f = f->next) {
			if(access(f->filename, R_OK) != 0) {
				fprintf(stderr,"%s did not create file %s\n", n->command, f->filename);
				job_failed = 1;
			} else {
				if(output_len_check) {
					if(stat(f->filename, &stat_info) == 0) {
						if(stat_info.st_size <= 0) {
							debug(D_DEBUG, "%s created a file of length %ld\n", n->command, (long) stat_info.st_size);
							job_failed = 1;
						}
					}
				}
			}
		}
	} else {
		if(info->exited_normally) {
			fprintf(stderr,"%s failed with exit code %d\n", n->command, info->exit_code);
		} else {
			fprintf(stderr,"%s crashed with signal %d (%s)\n", n->command, info->exit_signal, strsignal(info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		dag_node_state_change(d, n, DAG_NODE_STATE_FAILED);
		if(dag_retry_flag || info->exit_code == 101) {
			n->failure_count++;
			if(n->failure_count > dag_retry_max) {
				fprintf(stderr,"job %s failed too many times.\n", n->command);
				dag_failed_flag = 1;
			} else {
				fprintf(stderr,"will retry failed job %s\n", n->command);
				dag_node_state_change(d, n, DAG_NODE_STATE_WAITING);
			}
		} else {
			dag_failed_flag = 1;
		}
	} else {
		/* Record which target files have been generated by this node. */
		for(f = n->target_files; f; f = f->next) {
			hash_table_insert(d->completed_files, f->filename, f->filename);
		}

		/* Mark source files that have been used by this node and
		 * perform collection if we are doing reference counting. */
		for(f = n->source_files; f; f = f->next) {
			dag_gc_ref_incr(d, f->filename, -1);
			if (dag_gc_method == DAG_GC_REF_COUNT) {
				dag_gc_ref_count(d, f->filename);
			}
		}
		dag_node_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
}

int dag_check(struct dag *d)
{
	struct dag_node *n;
	struct dag_file *f;
	int error = 0;

	debug(D_DEBUG, "checking rules for consistency...\n");

	for(n = d->nodes; n; n = n->next) {
		for(f = n->source_files; f; f = f->next) {
			if(hash_table_lookup(d->completed_files, f->filename)) {
				continue;
			}

			if(access(f->filename, R_OK) == 0) {
				hash_table_insert(d->completed_files, f->filename, f->filename);
				continue;
			}

			if(hash_table_lookup(d->file_table, f->filename)) {
				continue;
			}

			if (!error){
				fprintf(stderr, "makeflow: %s does not exist, and is not created by any rule.\n", f->filename);
			}
			error = 1;
		}
	}

	if(error) {
		clean_symlinks(d, 1);
		return 0;
	}
	return 1;
}

int dag_gc_file(struct dag *d, const char *file, int count)
{
	if(access(file, R_OK) == 0 && unlink(file) < 0) {
		debug(D_NOTICE, "makeflow: unable to collect %s: %s", file, strerror(errno));
		return 0;
	} else {
		debug(D_DEBUG, "Garbage collected %s (%d)", file, count - 1);
		hash_table_remove(d->collect_table, file);
		return 1;
	}
}

void dag_gc_all(struct dag *d, int threshold, int maxfiles, time_t stoptime)
{
	int collected = 0;
	char *key;
	PTRINT_T value;
	timestamp_t start_time, stop_time;

	/* This will walk the table of files to collect and will remove any
	 * that are below or equal to the threshold. */
	start_time = timestamp_get();
	hash_table_firstkey(d->collect_table);
	while(hash_table_nextkey(d->collect_table, &key, (void **)&value) && time(0) < stoptime && collected < maxfiles) {
		if(value <= threshold && dag_gc_file(d, key, (int)value))
			collected++;
	}
	stop_time = timestamp_get();

	/* Record total amount of files collected to Makeflowlog. */
	if(collected > 0) {
		dag_gc_collected += collected;
		/** Line format: # GC timestamp collected time_spent dag_gc_collected
		 *
		 * timestamp - the unix time (in microseconds) when this line is written to the log file.
		 * collected - the number of files were collected in this garbage collection cycle.
		 * time_spent - the length of time this cycle took.
		 * dag_gc_collected - the total number of files has been collected so far since the start this makeflow execution.
		 *
		 */
		fprintf(d->logfile, "# GC\t%llu\t%d\t%llu\t%d\n", timestamp_get(), collected, stop_time - start_time, dag_gc_collected);
	}
}

void dag_gc_ref_incr(struct dag *d, const char *file, int increment)
{
	/* Increment the garbage file by specified amount */
	PTRINT_T ref_count = (PTRINT_T)hash_table_remove(d->collect_table, file);
	if(ref_count) {
		ref_count = ref_count + increment;
		hash_table_insert(d->collect_table, file, (void *)ref_count);
		debug(D_DEBUG, "Marked file %s references (%d)", file, ref_count - 1);
	}
}

void dag_gc_ref_count(struct dag *d, const char *file)
{
	PTRINT_T ref_count = (PTRINT_T)hash_table_remove(d->collect_table, file);
	if(ref_count && ref_count <= MAKEFLOW_GC_MIN_THRESHOLD) {
		timestamp_t start_time, stop_time;
		start_time = timestamp_get();
		dag_gc_file(d, file, ref_count);
		stop_time = timestamp_get();
		/** Line format: # GC timestamp collected time_spent dag_gc_collected
		 *
		 * timestamp - the unix time (in microseconds) when this line is written to the log file.
		 * collected - the number of files were collected in this garbage collection cycle.
		 * time_spent - the length of time this cycle took.
		 * dag_gc_collected - the total number of files has been collected so far since the start this makeflow execution.
		 *
		 */
		fprintf(d->logfile, "# GC\t%llu\t%d\t%llu\t%d\n", timestamp_get(), 1, stop_time - start_time, ++dag_gc_collected);
	}
}

/* TODO: move this to a more appropriate location? */
int directory_inode_count(const char *dirname)
{
	DIR *dir;
	struct dirent *d;
	int inode_count = 0;

	dir = opendir(dirname);
	if(dir == NULL)
		return INT_MAX;

	while((d = readdir(dir)))
		inode_count++;
	closedir(dir);

	return inode_count;
}

int directory_low_disk(const char *path)
{
	UINT64_T avail, total;

	if(disk_info_get(path, &avail, &total) >= 0)
		return avail <= MAKEFLOW_MIN_SPACE;

	return 0;
}

void dag_gc(struct dag *d)
{
	char cwd[PATH_MAX];

	switch(dag_gc_method) {
		case DAG_GC_INCR_FILE:
			debug(D_DEBUG, "Performing incremental file (%d) garbage collection", dag_gc_param);
			dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, dag_gc_param, INT_MAX);
			break;
		case DAG_GC_INCR_TIME:
			debug(D_DEBUG, "Performing incremental time (%d) garbage collection", dag_gc_param);
			dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, INT_MAX, time(0) + dag_gc_param);
			break;
		case DAG_GC_ON_DEMAND:
			getcwd(cwd, PATH_MAX);
			if(directory_inode_count(cwd) >= dag_gc_param || directory_low_disk(cwd)) {
				debug(D_DEBUG, "Performing on demand (%d) garbage collection", dag_gc_param);
				dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, INT_MAX, INT_MAX);
			}
			break;
		default:
			break;
	}
}

void dag_run(struct dag *d)
{
	struct dag_node *n;
	batch_job_id_t jobid;
	struct batch_job_info info;

	while(!dag_abort_flag) {

		dag_dispatch_ready_jobs(d);

		if(d->local_jobs_running == 0 && d->remote_jobs_running == 0)
			break;

		if(d->remote_jobs_running) {
			int tmp_timeout = 5;
			jobid = batch_job_wait_timeout(remote_queue, &info, time(0) + tmp_timeout);
			if(jobid > 0) {
				debug(D_DEBUG, "Job %d has returned.\n", jobid);
				n = itable_remove(d->remote_job_table, jobid);
				if(n)
					dag_node_complete(d, n, &info);
			}
		}

		if(d->local_jobs_running) {
			time_t stoptime;
			int tmp_timeout = 5;

			if(d->remote_jobs_running) {
				stoptime = time(0);
			} else {
				stoptime = time(0) + tmp_timeout;
			}

			jobid = batch_job_wait_timeout(local_queue, &info, stoptime);
			if(jobid > 0) {
				debug(D_DEBUG, "Job %d has returned.\n", jobid);
				n = itable_remove(d->local_job_table, jobid);
				if(n)
					dag_node_complete(d, n, &info);
			}
		}

		/* Rather than try to garbage collect after each time in this
		 * wait loop, perform garbage collection after a proportional
		 * amount of tasks have passed. */
		dag_gc_barrier--;
		if(dag_gc_barrier == 0) {
			dag_gc(d);
			dag_gc_barrier = MAX(d->nodeid_counter*dag_gc_task_ratio, 1);
		}
	}

	if(dag_abort_flag) {
		dag_abort_all(d);
	} else {
		if(!dag_failed_flag && dag_gc_method != DAG_GC_NONE) {
			dag_gc_all(d, INT_MAX, INT_MAX, INT_MAX);
		}
	}
}

static void handle_abort(int sig)
{
	dag_abort_flag = 1;
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, "Frequently used options:\n");
	fprintf(stdout, "-c             Clean up: remove logfile and all targets.\n");
	fprintf(stdout, "-T <type>      Batch system type: %s. (default is local)\n", batch_queue_type_string());
	fprintf(stdout, "Where options are:\n");
	fprintf(stdout, " -a             Advertise the master information to a catalog server.\n");
	fprintf(stdout, " -A             Disable the check for AFS.                  (experts only.)\n");;
	fprintf(stdout, " -B <options>   Add these options to all batch submit files.\n");
	fprintf(stdout, " -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	fprintf(stdout, " -d <subsystem> Enable debugging for this subsystem\n");
	fprintf(stdout, " -D             Display the Makefile as a Dot graph. Additional options 'c' to condense similar boxes, 's' to change the size of the boxes proportional to file size, else type 'none' for full expansion\n");
	fprintf(stdout, " -E             Enable master capacity estimation in Work Queue. Estimated master capacity may be viewed in the Work Queue log file.\n");
	fprintf(stdout, " -F <#>         Work Queue fast abort multiplier.           (default is deactivated)\n");
	fprintf(stdout, " -h             Show this help screen.\n");
	fprintf(stdout, " -i             Show the pre-execution analysis of the Makeflow script - <dagfile>.\n");
	fprintf(stdout, " -I             Show input files.\n");
	fprintf(stdout, " -j <#>         Max number of local jobs to run at once.    (default is # of cores)\n");
	fprintf(stdout, " -J <#>         Max number of remote jobs to run at once.   (default is 100)\n");
	fprintf(stdout, " -k             Syntax check.\n");
	fprintf(stdout, " -K             Preserve (i.e., do not clean) intermediate symbolic links\n");
	fprintf(stdout, " -l <logfile>   Use this file for the makeflow log.         (default is X.makeflowlog)\n");
	fprintf(stdout, " -L <logfile>   Use this file for the batch system log.     (default is X.condorlog)\n");
	fprintf(stdout, " -N <project>   Set the project name to <project>\n");
	fprintf(stdout, " -o <file>      Send debugging to this file.\n");
	fprintf(stdout, " -O             Show output files.\n");
	fprintf(stdout, " -p <port>      Port number to use with Work Queue.         (default is %d, 0=arbitrary)\n", WORK_QUEUE_DEFAULT_PORT);
	fprintf(stdout, " -P <integer>   Priority. Higher the value, higher the priority.\n");
	fprintf(stdout, " -R             Automatically retry failed batch jobs up to %d times.\n", dag_retry_max);
	fprintf(stdout, " -r <n>         Automatically retry failed batch jobs up to n times.\n");
	fprintf(stdout, " -S <timeout>   Time to retry failed batch job submission.  (default is %ds)\n", dag_submit_timeout);
	fprintf(stdout, " -t <timeout>   Work Queue keepalive timeout.           (default is %ds)\n", WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	fprintf(stdout, " -u <interval>  Work Queue keepalive interval.           (default is %ds)\n", WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	fprintf(stdout, " -v             Show version string\n");
	fprintf(stdout, " -W <mode>      Work Queue scheduling algorithm.            (time|files|fcfs)\n");
	fprintf(stdout, " -z             Force failure on zero-length output files \n");
	fprintf(stdout, " -Z <file>      Select port at random and write it to this file.\n");
}

int main(int argc, char *argv[])
{
	char c;
	char *logfilename = NULL;
	char *batchlogfilename = NULL;
	int clean_mode = 0;
	int display_mode = 0;
	int condense_display = 0;
	int change_size = 0;
	int syntax_check = 0;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int skip_afs_check = 0;
	int preserve_symlinks = 0;
	const char *batch_submit_options = getenv("BATCH_OPTIONS");
	int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	int work_queue_estimate_capacity_on = 0;
	int work_queue_keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	int work_queue_keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT;
	char *catalog_host;
	int catalog_port;
	int port_set = 0;
	char *s;

	debug_config(argv[0]);

	makeflow_exe = argv[0];

	s = getenv("MAKEFLOW_BATCH_QUEUE_TYPE");
	if(s) {
		batch_queue_type = batch_queue_type_from_string(s);
		if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
			fprintf(stderr, "makeflow: unknown batch queue type: %s (from $MAKEFLOW_BATCH_QUEUE_TYPE)\n", s);
			return 1;
		}
	}
	s = getenv("WORK_QUEUE_MASTER_MODE");
	if(s) {
		work_queue_master_mode = atoi(s);
	}
	s = getenv("WORK_QUEUE_NAME");
	if(s) {
		project = xxstrdup(s);
	}
	s = getenv("WORK_QUEUE_FAST_ABORT_MULTIPLIER");
	if(s) {
		wq_option_fast_abort_multiplier = atof(s);
	}

	while((c = getopt(argc, argv, "aAB:cC:d:D:E:f:F:g:G:hiIj:J:kKl:L:m:N:o:Op:P:r:RS:t:T:u:vW:zZ:")) != (char) -1) {
		switch (c) {
			case 'a':
				work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
				break;
			case 'A':
				skip_afs_check = 1;
				break;
			case 'B':
				batch_submit_options = optarg;
				break;
			case 'c':
				clean_mode = 1;
				break;
			case 'C':
				if(!parse_catalog_server_description(optarg, &catalog_host, &catalog_port)) {
					fprintf(stderr, "makeflow: catalog server should be given as HOSTNAME:PORT'.\n");
					exit(1);
				}
				setenv("CATALOG_HOST", catalog_host, 1);
	
				char *value = string_format("%d", catalog_port);
				setenv("CATALOG_PORT", value, 1);
				free(value);
	
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'D':
				if (strcasecmp(optarg, "c") == 0) condense_display = 1;
				if (strcasecmp(optarg, "s") == 0) change_size = 1;
				display_mode = 1;
				break;
			case 'E':
				work_queue_estimate_capacity_on = 1;
				break;
			case 'F':
				wq_option_fast_abort_multiplier = atof(optarg);
				break;
			case 'g':
				if (strcasecmp(optarg, "none") == 0) {
					dag_gc_method = DAG_GC_NONE;
				} else if (strcasecmp(optarg, "ref_count") == 0) {
					dag_gc_method = DAG_GC_REF_COUNT;
				} else if (strcasecmp(optarg, "incr_file") == 0) {
					dag_gc_method = DAG_GC_INCR_FILE;
					if (dag_gc_param < 0)
						dag_gc_param = 16;	/* Try to collect at most 16 files. */
				} else if (strcasecmp(optarg, "incr_time") == 0) {
					dag_gc_method = DAG_GC_INCR_TIME;
					if (dag_gc_param < 0)
						dag_gc_param = 5;	/* Timeout of 5. */
				} else if (strcasecmp(optarg, "on_demand") == 0) {
					dag_gc_method = DAG_GC_ON_DEMAND;
					if (dag_gc_param < 0)
						dag_gc_param = 1 << 14; /* Inode threshold of 2^14. */
				} else {
					fprintf(stderr, "makeflow: invalid garbage collection method: %s\n", optarg);
					exit(1);
				}
				break;
			case 'G':
				dag_gc_param = atoi(optarg);
				break;
			case 'h':
				show_help(argv[0]);
				return 0;
			case 'i':
				display_mode = SHOW_MAKEFLOW_ANALYSIS;	
				break;
			case 'I':
				display_mode = SHOW_INPUT_FILES;
				break;
			case 'j':
				explicit_local_jobs_max = atoi(optarg);
				break;
			case 'J':
				explicit_remote_jobs_max = atoi(optarg);
				break;
			case 'k':
				syntax_check = 1;
				break;
			case 'K':
				preserve_symlinks = 1;
				break;
			case 'l':
				logfilename = xxstrdup(optarg);
				break;
			case 'L':
				batchlogfilename = xxstrdup(optarg);
				break;
			case 'N':
				free(project);
				project = xxstrdup(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'O':
				display_mode = SHOW_OUTPUT_FILES;
				break;
			case 'p':
				port_set = 1;
				port = atoi(optarg);
				break;
			case 'P':
				priority = atoi(optarg);
				break;
			case 'r':
				dag_retry_flag = 1;
				dag_retry_max = atoi(optarg);
				break;
			case 'R':
				dag_retry_flag = 1;
				break;
			case 'S':
				dag_submit_timeout = atoi(optarg);
				break;
			case 't':
				work_queue_keepalive_timeout = atoi(optarg);
				break;
			case 'T':
				batch_queue_type = batch_queue_type_from_string(optarg);
				if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr, "makeflow: unknown batch queue type: %s\n", optarg);
					return 1;
				}
				break;
			case 'u':
				work_queue_keepalive_interval = atoi(optarg);
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case 'W':
				if(!strcmp(optarg, "files")) {
					wq_option_scheduler = WORK_QUEUE_SCHEDULE_FILES;
				} else if(!strcmp(optarg, "time")) {
					wq_option_scheduler = WORK_QUEUE_SCHEDULE_TIME;
				} else if(!strcmp(optarg, "fcfs")) {
					wq_option_scheduler = WORK_QUEUE_SCHEDULE_FCFS;
				} else {
					fprintf(stderr, "makeflow: unknown scheduling mode %s\n", optarg);
					return 1;
				}
				break;
			case 'z':
				output_len_check = 1;
				break;
			case 'Z':
				port_file = optarg;
				port = 0;
				port_set = 1; //WQ is going to set the port, so we continue as if already set.
				break;
			default:
				show_help(argv[0]);
				return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	const char *dagfile;

	if((argc - optind) != 1) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}

		dagfile = "./Makeflow";
	} else {
		dagfile = argv[optind];
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		if(work_queue_master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && !project) {
			fprintf(stderr, "makeflow: Makeflow running in catalog mode. Please use '-N' option to specify the name of this project.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}

			// Use Work Queue default port in standalone mode when port is not
			// specified with -p option. In Work Queue catalog mode, Work Queue
			// would choose an arbitrary port when port is not explicitly specified.
		if(!port_set && work_queue_master_mode == WORK_QUEUE_MASTER_MODE_STANDALONE) {
			port_set = 1;
			port = WORK_QUEUE_DEFAULT_PORT;
		}

		if(port_set) {
			char *value;
			value = string_format("%d", port);
			setenv("WORK_QUEUE_PORT", value, 1);
			free(value);
		}
	}

	if(!logfilename)
		logfilename = string_format("%s.makeflowlog", dagfile);
	if(!batchlogfilename) {
		switch (batch_queue_type) {
		case BATCH_QUEUE_TYPE_CONDOR:
			batchlogfilename = string_format("%s.condorlog", dagfile);
			break;
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			batchlogfilename = string_format("%s.wqlog", dagfile);
			break;
		default:
			batchlogfilename = string_format("%s.batchlog", dagfile);
			break;
		}

		// In clean mode, delete all exsiting log files
		if(clean_mode) {
			char *cleanlog = string_format("%s.condorlog", dagfile);
			file_clean(cleanlog, 0);
			free(cleanlog);
			cleanlog = string_format("%s.wqlog", dagfile);
			file_clean(cleanlog, 0);
			free(cleanlog);
			cleanlog = string_format("%s.batchlog", dagfile);
			file_clean(cleanlog, 0);
			free(cleanlog);
		}
	}

	int no_symlinks = (clean_mode || syntax_check || display_mode);
	struct dag *d = dag_create();
	if(!d || !dag_parse(d, dagfile, no_symlinks)) {
		fprintf(stderr, "makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
		free(logfilename);
		free(batchlogfilename);
		return 1;
	}

	if(syntax_check) {
		fprintf(stdout, "%s: Syntax OK.\n", dagfile);
		return 0;
	}

	if(display_mode == SHOW_INPUT_FILES) {
		dag_show_input_files(d);
		exit(0);
	}
	if(display_mode == SHOW_OUTPUT_FILES) {
		dag_show_output_files(d);
		exit(0);
	}
	if(display_mode == SHOW_MAKEFLOW_ANALYSIS) {
		dag_show_analysis(d);
		exit(0);
	}

	if(explicit_local_jobs_max) {
		d->local_jobs_max = explicit_local_jobs_max;
	} else {
		d->local_jobs_max = load_average_get_cpus();
	}

	if(explicit_remote_jobs_max) {
		d->remote_jobs_max = explicit_remote_jobs_max;
	} else {
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			d->remote_jobs_max = load_average_get_cpus();
		} else if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			d->remote_jobs_max = 1000;
		} else {
			d->remote_jobs_max = 100;
		}
	}

	s = getenv("MAKEFLOW_MAX_REMOTE_JOBS");
	if(s) {
		d->remote_jobs_max = MIN(d->remote_jobs_max, atoi(s));
	}

	s = getenv("MAKEFLOW_MAX_LOCAL_JOBS");
	if(s) {
		int n = atoi(s);
		d->local_jobs_max = MIN(d->local_jobs_max, n);
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			d->remote_jobs_max = MIN(d->local_jobs_max, n);
		}
	}

	if(dag_prepare_for_batch_system(d)) {
		return 1;
	}

	dag_prepare_gc(d);
	dag_prepare_nested_jobs(d);

	if(display_mode) {
		free(logfilename);
		free(batchlogfilename);
		dag_print(d, condense_display, change_size);
		return 0;
	}

	if(clean_mode) {
		dag_clean(d);
		file_clean(logfilename, 0);
		file_clean(batchlogfilename, 0);
		free(logfilename);
		free(batchlogfilename);
		return 0;
	}

	if(!dag_check(d)) {
		free(logfilename);
		free(batchlogfilename);
		return 1;
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR && !skip_afs_check) {
		char *cwd = string_getcwd();
		if(!strncmp(cwd, "/afs", 4)) {
			fprintf(stderr, "makeflow: This won't work because Condor is not able to write to files in AFS.\n");
			fprintf(stderr, "makeflow: Instead, run makeflow from a local disk like /tmp.\n");
			fprintf(stderr, "makeflow: Or, use the Work Queue with -T wq and condor_submit_workers.\n");

			free(logfilename);
			free(batchlogfilename);
			free(cwd);

			exit(1);
		}
		free(cwd);
	}

	setlinebuf(stdout);
	setlinebuf(stderr);

	local_queue = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL);
	if(!local_queue) {
		fprintf(stderr, "makeflow: couldn't create local job queue.\n");
		exit(1);
	}

	remote_queue = batch_queue_create(batch_queue_type);
	if(!remote_queue) {
		fprintf(stderr, "makeflow: couldn't create batch queue.\n");
		if(port != 0)
			fprintf(stderr, "makeflow: perhaps port %d is already in use?\n", port);
		exit(1);
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
		struct work_queue *q = batch_queue_get_work_queue(remote_queue);
		if(!q) {
			fprintf(stderr, "makeflow: cannot get work queue object.\n");
			exit(1);
		}

		work_queue_specify_master_mode(q, work_queue_master_mode);
		work_queue_specify_name(q, project);
		work_queue_specify_priority(q, priority);
		work_queue_specify_estimate_capacity_on(q, work_queue_estimate_capacity_on);
		work_queue_specify_keepalive_interval(q, work_queue_keepalive_interval);
		work_queue_specify_keepalive_timeout(q, work_queue_keepalive_timeout);
		port = work_queue_port(q);
		if(port_file)
			opts_write_port_file(port_file, port);
	}

	if(batch_submit_options) {
		debug(D_DEBUG, "setting batch options to %s\n", batch_submit_options);
		batch_queue_set_options(remote_queue, batch_submit_options);
	}

	if(batchlogfilename) {
		batch_queue_set_logfile(remote_queue, batchlogfilename);
	}

	port = batch_queue_port(remote_queue);
	if(port > 0)
		printf("listening for workers on port %d.\n", port);

	dag_log_recover(d, logfilename);

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);

	fprintf(d->logfile, "# STARTED\t%llu\n", timestamp_get());
	dag_run(d);

	batch_queue_delete(local_queue);
	batch_queue_delete(remote_queue);

	if(!preserve_symlinks && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		clean_symlinks(d, 0);
	}

	free(logfilename);
	free(batchlogfilename);

	if(dag_abort_flag) {
		fprintf(d->logfile, "# ABORTED\t%llu\n", timestamp_get());
		fprintf(stderr, "workflow was aborted.\n");
		return 1;
	} else if(dag_failed_flag) {
		fprintf(d->logfile, "# FAILED\t%llu\n", timestamp_get());
		fprintf(stderr, "workflow failed.\n");
		return 1;
	} else {
		fprintf(d->logfile, "# COMPLETED\t%llu\n", timestamp_get());
		fprintf(stderr, "nothing left to do.\n");
		return 0;
	}
}

/* vim: set sw=8 sts=8 ts=8 ft=c: */
