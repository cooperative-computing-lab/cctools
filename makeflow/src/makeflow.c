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
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include "cctools.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "work_queue.h"
#include "work_queue_internal.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "xxmalloc.h"
#include "getopt_aux.h"
#include "rmonitor.h"
#include "random_init.h"

#include "dag.h"
#include "visitors.h"

/* Display options */
enum { SHOW_INPUT_FILES = 2,
       SHOW_OUTPUT_FILES,
       SHOW_MAKEFLOW_ANALYSIS,
       SHOW_DAG_DOT,
       SHOW_DAG_PPM };

#define RANDOM_PORT_RETRY_TIME 300

#define MAKEFLOW_AUTO_WIDTH 1
#define MAKEFLOW_AUTO_GROUP 2

#define	MAKEFLOW_MIN_SPACE 10*1024*1024	/* 10 MB */

#define MONITOR_ENV_VAR "CCTOOLS_RESOURCE_MONITOR"

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%06.6d"
#define DEFAULT_MONITOR_INTERVAL   1

/* Unique integers for long options. */

enum { LONG_OPT_MONITOR_INTERVAL = 1,
       LONG_OPT_MONITOR_LOG_NAME,
       LONG_OPT_MONITOR_LIMITS,
       LONG_OPT_MONITOR_TIME_SERIES,
       LONG_OPT_MONITOR_OPENED_FILES,
       LONG_OPT_PASSWORD,
       LONG_OPT_PPM_ROW,
       LONG_OPT_PPM_FILE,
       LONG_OPT_PPM_EXE,
       LONG_OPT_PPM_LEVELS,
       LONG_OPT_DOT_PROPORTIONAL,
       LONG_OPT_DOT_CONDENSE };

typedef enum {
	DAG_GC_NONE,
	DAG_GC_REF_COUNT,
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
static char *monitor_exe = NULL;

static int monitor_mode = 0; /* & 1 enabled, & 2 with time-series, & 4 whith opened-files */
static char *monitor_limits_name = NULL;
static int monitor_interval = 1;	// in seconds  
static char *monitor_log_format = NULL;
static char *monitor_log_dir = NULL;

static char *wq_password = 0;

int dag_depth(struct dag *d);
int dag_width_uniform_task(struct dag *d);
int dag_width_guaranteed_max(struct dag *d);
int dag_width(struct dag *d, int nested_jobs);
void dag_node_complete(struct dag *d, struct dag_node *n, struct batch_job_info *info);
int dag_check(struct dag *d);
int dag_check_dependencies(struct dag *d);

void dag_export_variables(struct dag *d, struct dag_node *n);

char *dag_parse_readline(struct lexer_book *bk, struct dag_node *n);
int dag_parse(struct dag *d, FILE * dag_stream);
int dag_parse_variable(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_node(struct lexer_book *bk, char *line);
int dag_parse_node_filelist(struct lexer_book *bk, struct dag_node *n, char *filelist, int source);
int dag_parse_node_command(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_node_makeflow_command(struct lexer_book *bk, struct dag_node *n, char *line);
int dag_parse_export(struct lexer_book *bk, char *line);

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
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			// get the node (tmp) that outputs current source file
			tmp = f->target_of;
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
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->target_of) != NULL) {
				n->level = -1;
				list_push_tail(level_unsolved_nodes, n);
				break;
			}
		}
	}

	int max_level = 0;
	while((n = (struct dag_node *) list_pop_head(level_unsolved_nodes)) != NULL) {
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if((parent = f->target_of) != NULL) {
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

	return max_level + 1;
}

/** 
 * This algorithm assumes all the tasks take the same amount of time to execute
 * and each task would be executed as early as possible. If the return value is
 * x, a positive integer, that means at least x tasks can be run in parallel
 * during a certain point of the execution of the workflow. 
 *
 * The following algorithm first determines the level (depth) of each node by
 * calling the dag_depth() function and then counts how many nodes are there at
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

void dag_show_analysis(struct dag *d)
{
	printf("num_of_tasks\t%d\n", itable_size(d->node_table));
	printf("depth\t%d\n", dag_depth(d));
	printf("width_uniform_task\t%d\n", dag_width_uniform_task(d));
	printf("width_guaranteed_max\t%d\n", dag_width_guaranteed_max(d));
}

void dag_show_input_files(struct dag *d)
{
	struct dag_file *f;
	struct list *il;

	il = dag_input_files(d);
	list_first_item(il);
	while((f = list_next_item(il)))
		printf("%s\n", f->filename);

	list_delete(il);
}


void collect_input_files(struct dag *d, char *bundle_dir, char *(*rename) (struct dag_node * d, const char *filename))
{
	char file_destination[PATH_MAX];
	char *new_name;

	struct list *il;
	il = dag_input_files(d);

	struct dag_file *f;

	if(!rename)
		rename = dag_node_translate_filename;

	list_first_item(il);
	while((f = list_next_item(il))) {
		new_name = rename(NULL, f->filename);
		char *dir = NULL;
		dir = xxstrdup(new_name);
		string_dirname(new_name, dir);
		if(dir){
			sprintf(file_destination, "%s/%s", bundle_dir, dir);
			if(!create_dir(file_destination, 0755)) {
				fprintf(stderr,  "Could not create %s. Check the permissions and try again.\n", file_destination);
				free(dir);
				exit(1);
			}
			free(dir);
		}

		sprintf(file_destination, "%s/%s", bundle_dir, new_name);

		copy_file_to_file(f->filename, file_destination);
		free(new_name);
	}

	list_delete(il);
}

/* When a collision is detected (a file with an absolute path has the same base name as a relative file) a
 * counter is appended to the filename and the name translation is retried */
char *bundler_translate_name(const char *input_filename, int collision_counter)
{
	static struct hash_table *previous_names = NULL;
	static struct hash_table *reverse_names = NULL;
	if(!previous_names)
		previous_names = hash_table_create(0, NULL);
	if(!reverse_names)
		reverse_names = hash_table_create(0, NULL);

	char *filename = NULL;

	if(collision_counter){
		filename = string_format("%s%d", input_filename, collision_counter);
	}else{
		filename = xxstrdup(input_filename);
	}

	const char *new_filename;
	new_filename = hash_table_lookup(previous_names, filename);
	if(new_filename)
		return xxstrdup(new_filename);

	new_filename = hash_table_lookup(reverse_names, filename);
	if(new_filename) {
		collision_counter++;
		char *tmp = bundler_translate_name(filename, collision_counter);
		free(filename);
		return tmp;
	}
	if(filename[0] == '/') {
		new_filename = string_basename(filename);
		if(hash_table_lookup(previous_names, new_filename)) {
			collision_counter++;
			char *tmp = bundler_translate_name(filename, collision_counter);
			free(filename);
			return tmp;
		} else if(hash_table_lookup(reverse_names, new_filename)) {
			collision_counter++;
			char *tmp = bundler_translate_name(filename, collision_counter);
			free(filename);
			return tmp;
		} else {
			hash_table_insert(reverse_names, new_filename, filename);
			hash_table_insert(previous_names, filename, new_filename);
			return xxstrdup(new_filename);
		}
	} else {
		hash_table_insert(previous_names, filename, filename);
		hash_table_insert(reverse_names, filename, filename);
		return filename;
	}
}

char *bundler_rename(struct dag_node *n, const char *filename)
{

	if(n) {
		struct list *input_files = dag_input_files(n->d);
		if(list_find(input_files, (int (*)(void *, const void *)) string_equal, (void *) filename)) {
			list_free(input_files);
			return xxstrdup(filename);
		}
	}
	return bundler_translate_name(filename, 0);	/* no collisions yet -> 0 */
}

void dag_show_output_files(struct dag *d)
{
	struct dag_file *f;
	char *filename;

	hash_table_firstkey(d->file_table);
	while(hash_table_nextkey(d->file_table, &filename, (void **) &f)) {
		if(f->target_of)
			fprintf(stdout, "%s\n", filename);
	}
}

/** 
 * Computes the width of the graph
 */
int dag_width(struct dag *d, int nested_jobs)
{
	struct dag_node *n, *parent;
	struct dag_file *f;

	/* 1. Find the number of immediate children for all nodes; also,
	   determine leaves by adding nodes with children==0 to list. */

	for(n = d->nodes; n != NULL; n = n->next) {
		n->level = 0;	// initialize 'level' value to 0 because other functions might have modified this value.
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->target_of;
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

		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			parent = f->target_of;
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
		if(nested_jobs && !n->nested_job)
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


void dag_abort_all(struct dag *d)
{
	UINT64_T jobid;
	struct dag_node *n;

	printf("got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table, &jobid, (void **) &n)) {
		printf("aborting local job %" PRIu64 "\n", jobid);
		batch_job_remove(local_queue, jobid);
		dag_node_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		printf("aborting remote job %" PRIu64 "\n", jobid);
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
					fprintf(stderr, "couldn't delete directory %s: %s\n", filename, strerror(errno));
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
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		file_clean(f->filename, 0);

		/* Make sure to clobber the original file too if it exists */
		char *name = (char *) hash_table_lookup(n->remote_names_inv, f->filename);

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
		fprintf(stderr, "rule still running: %s\n", n->command);
		itable_insert(d->remote_job_table, n->jobid, n);
		d->remote_jobs_running++;

		// Otherwise, we cannot reconnect to the job, so rerun it
	} else if(n->state == DAG_NODE_STATE_RUNNING || n->state == DAG_NODE_STATE_FAILED || n->state == DAG_NODE_STATE_ABORTED) {
		fprintf(stderr, "will retry failed rule: %s\n", n->command);
		goto rerun;
	}
	// Rerun if an input file has been updated since the last execution.
	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
		if(stat(f->filename, &filestat) >= 0) {
			if(S_ISDIR(filestat.st_mode))
				continue;
			if(difftime(filestat.st_mtime, n->previous_completion) > 0) {
				goto rerun;	// rerun this node
			}
		} else {
			if(!f->target_of) {
				fprintf(stderr, "makeflow: input file %s does not exist and is not created by any rule.\n", f->filename);
				exit(1);
			} else {
				/* If input file is missing, but node completed and file was garbage, then avoid rerunning. */
				if(n->state == DAG_NODE_STATE_COMPLETE && set_lookup(d->collect_table, f)) {
					continue;
				}
				goto rerun;
			}
		}
	}

	// Rerun if an output file is missing.
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		if(stat(f->filename, &filestat) < 0) {
			/* If output file is missing, but node completed and file was garbage, then avoid rerunning. */
			if(n->state == DAG_NODE_STATE_COMPLETE && set_lookup(d->collect_table, f)) {
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
	list_first_item(n->source_files);
	while((f1 = list_next_item(n->source_files))) {
		if(!set_lookup(d->collect_table, f1))
			continue;

		p = f1->target_of;
		if(p) {
			dag_node_force_rerun(rerun_table, d, p);
			f1->ref_count += 1;
		}
	}

	// For each child node, rerun it
	list_first_item(n->target_files);
	while((f1 = list_next_item(n->target_files))) {
		for(p = d->nodes; p; p = p->next) {
			child_node_found = 0;

			list_first_item(p->source_files);
			while((f2 = list_next_item(n->source_files))) {
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
			if(sscanf(line, "%" SCNu64 " %d %d %d", &previous_completion_time, &nodeid, &state, &jobid) == 4) {
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
			/* record node information to log */
			fprintf(d->logfile, "# NODE\t%d\t%s\n", n->nodeid, n->original_command);

			/* Record the node category to the log */
			fprintf(d->logfile, "# CATEGORY\t%d\t%s\n", n->nodeid, n->category->label);

			/* Record node parents to log */
			fprintf(d->logfile, "# PARENTS\t%d", n->nodeid);
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files))) {
				p = f->target_of;
				if(p)
					fprintf(d->logfile, "\t%d", p->nodeid);
			}
			fputc('\n', d->logfile);

			/* Record node inputs to log */
			fprintf(d->logfile, "# SOURCES\t%d", n->nodeid);
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files))) {
				fprintf(d->logfile, "\t%s", f->filename);
			}
			fputc('\n', d->logfile);

			/* Record node outputs to log */
			fprintf(d->logfile, "# TARGETS\t%d", n->nodeid);
			list_first_item(n->target_files);
			while((f = list_next_item(n->target_files))) {
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

	//Update file reference counts from nodes in log
	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_COMPLETE)
		{
			struct dag_file *f;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)))
				f->ref_count += -1;
		}
	}
}

static char *translate_command(struct dag_node *n, char *old_command, int is_local)
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
			val = dag_file_remote_name(n, token);

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


#define dag_parse_error(bk, type) \
	fprintf(stderr, "makeflow: invalid " type " in file %s at line %ld, column %ld\n", (bk)->d->filename, (bk)->line_number, (bk)->column_number);

/* Returns a pointer to a new struct dag described by filename. Return NULL on
 * failure. */
struct dag *dag_from_file(const char *filename)
{
	FILE *dagfile;
	struct dag *d = NULL;

	dagfile = fopen(filename, "r");
	if(dagfile == NULL)
		debug(D_DEBUG, "makeflow: unable to open file %s: %s\n", filename, strerror(errno));
	else {
		d = dag_create();
		d->filename = xxstrdup(filename);
		if(!dag_parse(d, dagfile)) {
			free(d);
			d = NULL;
		}

		fclose(dagfile);
	}

	return d;
}

int dag_parse(struct dag *d, FILE * dag_stream)
{
	char *line = NULL;
	struct lexer_book *bk = calloc(1, sizeof(struct lexer_book));	//Taking advantage that calloc zeroes memory

	bk->d = d;
	bk->stream = dag_stream;

	bk->category = dag_task_category_lookup_or_create(d, "default");

	dag_task_category_get_env_resources(bk->category);

	while((line = dag_parse_readline(bk, NULL)) != NULL) {

		if(strlen(line) == 0 || line[0] == '#') {
			/* Skip blank lines and comments */
			free(line);
			continue;
		}
		if(strncmp(line, "export ", 7) == 0) {
			if(!dag_parse_export(bk, line)) {
				dag_parse_error(bk, "export");
				goto failure;
			}
		} else if(strchr(line, '=')) {
			if(!dag_parse_variable(bk, NULL, line)) {
				dag_parse_error(bk, "variable");
				goto failure;
			}
		} else if(strstr(line, ":")) {
			if(!dag_parse_node(bk, line)) {
				dag_parse_error(bk, "node");
				goto failure;
			}
		} else {
			dag_parse_error(bk, "syntax");
			goto failure;
		}

		free(line);
	}
//ok:
	dag_compile_ancestors(d);
	free(bk);
	return 1;

      failure:
	free(line);
	free(bk);
	return 0;
}

void dag_prepare_gc(struct dag *d)
{
	/* Files to be collected:
	 * ((all_files \minus sink_files)) \union collect_list) \minus preserve_list) \minus source_files 
	 */

	/* Parse GC_*_LIST and record which target files should be
	 * garbage collected. */
	char *collect_list  = dag_lookup_set("GC_COLLECT_LIST", d);
	char *preserve_list = dag_lookup_set("GC_PRESERVE_LIST", d);

	struct dag_file *f;
	char *filename;

	/* add all files, but sink_files */
	hash_table_firstkey(d->file_table);
	while((hash_table_nextkey(d->file_table, &filename, (void **) &f)))
		if(!dag_file_is_sink(f)) {
			set_insert(d->collect_table, f);
		}

	int i, argc;
	char **argv;

	/* add collect_list, for sink_files that should be removed */
	string_split_quotes(collect_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		f = dag_file_lookup_or_create(d, argv[i]);
		set_insert(d->collect_table, f);
		debug(D_DEBUG, "Added %s to garbage collection list", f->filename);
	}
	free(argv);

	/* remove files from preserve_list */
	string_split_quotes(preserve_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		/* Must initialize to non-zero for hash_table functions to work properly. */
		f = dag_file_lookup_or_create(d, argv[i]);
		set_remove(d->collect_table, f);
		debug(D_DEBUG, "Removed %s from garbage collection list", f->filename);
	}
	free(argv);

	/* remove source_files from collect_table */
	hash_table_firstkey(d->file_table);
	while((hash_table_nextkey(d->file_table, &filename, (void **) &f)))
		if(dag_file_is_source(f)) {
			set_remove(d->collect_table, f);
			debug(D_DEBUG, "Removed %s from garbage collection list", f->filename);
		}

	/* Print reference counts of files to be collected */
	set_first_element(d->collect_table);
	while((f = set_next_element(d->collect_table)))
		debug(D_DEBUG, "Added %s to garbage collection list (%d)", f->filename, f->ref_count);
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
				free((char *) n->command);
				n->command = command;
			}
		}
	}
}

char *dag_parse_readline(struct lexer_book *bk, struct dag_node *n)
{
	struct dag *d = bk->d;
	struct dag_lookup_set s = { d, bk->category, n, NULL };
	char *raw_line = get_line(bk->stream);

	if(raw_line) {
		bk->column_number = 1;
		bk->line_number++;

		if(bk->line_number % 1000 == 0) {
			debug(D_DEBUG, "read line %d\n", bk->line_number);
		}

		/* Strip whitespace */
		string_chomp(raw_line);
		while(isspace(*raw_line)) {
			raw_line++;
			bk->column_number++;
		}

		/* Chop off comments
		 * TODO: this will break if we use # in a string. */
		char *hash = strrchr(raw_line, '#');
		if(hash && hash != raw_line) {
			*hash = 0;
		}

		char *subst_line = xxstrdup(raw_line);
		subst_line = string_subst(subst_line, dag_lookup_str, &s);

		free(bk->linetext);
		bk->linetext = xxstrdup(subst_line);

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

//return 1 if name is special variable, 0 otherwise
int dag_parse_process_variable(struct lexer_book *bk, struct dag_node *n, struct hash_table *current_table, char *name, struct dag_variable_value *v)
{
	struct dag *d = bk->d;
	int   special = 0;

	if(strcmp(RESOURCES_CATEGORY, name) == 0) {
		special = 1;
		/* If we have never seen this label, then create
		 * a new category, otherwise retrieve the category. */
		struct dag_task_category *category = dag_task_category_lookup_or_create(d, v->value);

		/* If we are parsing inside a node, make category
		 * the category of the node, but do not update
		 * the global task_category. Else, update the
		 * global task category. */
		if(n) {
			/* Remove node from previous category...*/
			list_pop_tail(n->category->nodes);
			n->category = category;
			/* and add it to the new one */
			list_push_tail(n->category->nodes, n);
			debug(D_DEBUG, "Updating category '%s' for rule %d.\n", v->value, n->nodeid);
		}
		else
			bk->category = category;
	}
	else if(strcmp(RESOURCES_CORES,  name) == 0) {
		special = 1;
		current_table = bk->category->variables;
		bk->category->resources->cores             = atoi(v->value);
	}
	else if(strcmp(RESOURCES_MEMORY, name) == 0) {
		special = 1;
		current_table = bk->category->variables;
		bk->category->resources->resident_memory   = atoi(v->value);
	}
	else if(strcmp(RESOURCES_DISK,   name) == 0) {
		special = 1;
		current_table = bk->category->variables;
		bk->category->resources->workdir_footprint = atoi(v->value);
	}
	/* else if some other special variable .... */
	/* ... */

	hash_table_remove(current_table, name); //memory leak...
	hash_table_insert(current_table, name, v);

	return special;
}

int dag_parse_variable(struct lexer_book *bk, struct dag_node *n, char *line)
{
	struct dag *d = bk->d;
	char *name = line + (n ? 1 : 0);	/* Node variables require offset of 1 */
	char *value = NULL;
	char *equal = NULL;
	int append = 0;

	equal = strchr(line, '=');
	if((value = strstr(line, "+=")) && value < equal) {
		*value = 0;
		value = value + 2;
		append = 1;
	} else {
		value = equal;
		*value = 0;
		value = value + 1;
	}

	name = string_trim_spaces(name);
	value = string_trim_spaces(value);
	value = string_trim_quotes(value);

	if(strlen(name) == 0) {
		dag_parse_error(bk, "variable name");
		return 0;
	}

	struct dag_lookup_set s = { d, bk->category, n, NULL };
	struct dag_variable_value *v = dag_lookup(name, &s);
	struct hash_table *current_table = d->variables;

	if(append && v) {
		if(s.table)
			current_table = s.table;
		v = dag_variable_value_append_or_create(v, value);
	} else {
		if(n)
			current_table = n->variables;
		v = dag_variable_value_create(value);
	}

	dag_parse_process_variable(bk, n, current_table, name, v);

	if(append)
		debug(D_DEBUG, "%s appending to variable name=%s, value=%s", (n ? "node" : "dag"), name, value);
	else
		debug(D_DEBUG, "%s variable name=%s, value=%s", (n ? "node" : "dag"), name, value);

	return 1;
}

char *monitor_log_name(char *dirname, int nodeid)
{
	char *name = string_format(monitor_log_format, nodeid);
	char *path = string_format("%s/%s", dirname, name);
	free(name);

	return path;
}

int dag_parse_node(struct lexer_book *bk, char *line_org)
{
	struct dag *d = bk->d;
	char *line;
	char *outputs = NULL;
	char *inputs = NULL;
	struct dag_node *n;

	n = dag_node_create(bk->d, bk->line_number);

	n->category = bk->category;
	list_push_tail(n->category->nodes, n);

	line = xxstrdup(line_org);

	outputs = line;

	inputs = strchr(line, ':');
	*inputs = 0;
	inputs = inputs + 1;

	inputs = string_trim_spaces(inputs);
	outputs = string_trim_spaces(outputs);

	dag_parse_node_filelist(bk, n, outputs, 0);
	dag_parse_node_filelist(bk, n, inputs, 1);

	while((line = dag_parse_readline(bk, n)) != NULL) {
		if(line[0] == '@' && strchr(line, '=')) {
			if(!dag_parse_variable(bk, n, line)) {
				dag_parse_error(bk, "node variable");
				free(line);
				return 0;
			}
		} else {
			if(dag_parse_node_command(bk, n, line)) {
				n->next = d->nodes;
				d->nodes = n;
				itable_insert(d->node_table, n->nodeid, n);
				break;
			} else {
				dag_parse_error(bk, "node command");
				free(line);
				return 0;
			}
		}
		free(line);
	}

	debug(D_DEBUG, "Setting resource category '%s' for rule %d.\n", n->category->label, n->nodeid);
	dag_task_category_print_debug_resources(n->category);

	return 1;
}

/** Parse a node's input or output filelist.
Parse through a list of input or output files, adding each as a source or target file to the provided node.
@param d The DAG being constructed
@param n The node that the files are being added to
@param filelist The list of files, separated by whitespace
@param source a flag for whether the files are source or target files.  1 indicates source files, 0 indicates targets
*/
int dag_parse_node_filelist(struct lexer_book *bk, struct dag_node *n, char *filelist, int source)
{
	char *filename;
	char *newname;
	char **argv;
	int i, argc;

	string_split_quotes(filelist, &argc, &argv);
	for(i = 0; i < argc; i++) {
		filename = argv[i];
		newname = NULL;
		debug(D_DEBUG, "node %s file=%s", (source ? "input" : "output"), filename);

		// remote renaming
		if((newname = strstr(filename, "->"))) {
			*newname = '\0';
			newname += 2;
		}

		if(source)
			dag_node_add_source_file(n, filename, newname);
		else
			dag_node_add_target_file(n, filename, newname);
	}
	free(argv);
	return 1;

}

int dag_prepare_for_batch_system_files(struct dag_node *n, struct list *files, int source_flag)
{
	struct dag_file *f;
	list_first_item(files);

	while((f = list_next_item(files))) {
		const char *remotename = dag_file_remote_name(n, f->filename);

		switch (batch_queue_type) {
		case BATCH_QUEUE_TYPE_CONDOR:
			if(strchr(f->filename, '/') && !remotename)
				remotename = dag_node_add_remote_name(n, f->filename, NULL);

			if(remotename) {
				debug(D_DEBUG, "creating symlink \"./%s\" for file \"%s\"\n", remotename, f->filename);
				if(symlink(f->filename, remotename) < 0) {
					if(errno != EEXIST) {
						fatal("makeflow: could not create symbolic link (%s)\n", strerror(errno));
					} else {
						int link_size = strlen(f->filename) + 2;
						char *link_contents = malloc(link_size);

						link_size = readlink(remotename, link_contents, link_size);
						if(!link_size || strncmp(f->filename, link_contents, link_size)) {
							free(link_contents);
							fatal("makeflow: symbolic link %s points to wrong file (\"%s\" instead of \"%s\")\n", remotename, link_contents, f->filename);
						}
						free(link_contents);
					}
				} else {
					list_push_tail(n->d->symlinks_created, (void *) remotename);
				}

				/* Create symlink target  stub for output files, otherwise Condor will fail on write-back */
				if(!source_flag && access(f->filename, R_OK) < 0) {
					int fd = open(f->filename, O_WRONLY | O_CREAT | O_TRUNC, 0700);
					if(fd < 0) {
						fatal("makeflow: could not create symbolic link target (%s): %s\n", f->filename, strerror(errno));
					}
					close(fd);
				}
			}
			break;
                case BATCH_QUEUE_TYPE_WORK_QUEUE:
                        /* Note we do not fall with
                         * BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS here, since we
                         * do not want to rename absolute paths in such case.
                         * */
			if(f->filename[0] == '/' && !remotename) {
				/* Translate only explicit absolute paths for Work Queue tasks. */
				remotename = dag_node_add_remote_name(n, f->filename, NULL);
				debug(D_DEBUG, "translating work queue absolute path (%s) -> (%s)", f->filename, remotename);
			}
			break;
		default:
			if(remotename)
				fprintf(stderr, "makeflow: automatic file renaming (%s->%s) only works with Condor or Work Queue drivers\n", f->filename, remotename);
			break;

		}
	}
	return 1;
}

int dag_prepare_for_batch_system(struct dag *d)
{

	struct dag_node *n;

	for(n = d->nodes; n; n = n->next) {
		if(!dag_prepare_for_batch_system_files(n, n->source_files, 1 /* source_flag */ ))
			return 0;
		if(!dag_prepare_for_batch_system_files(n, n->target_files, 0))
			return 0;
	}

	return 1;
}

int dag_prepare_for_monitoring(struct dag *d)
{	
	struct dag_node *n;

	for(n = d->nodes; n; n = n->next)
	{
		if(monitor_mode)
		{
			char *log_name_prefix;
			char *log_name;
			log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
			n->command = resource_monitor_rewrite_command((char *) n->command, log_name_prefix,
					monitor_limits_name,
					dag_task_category_wrap_as_rmonitor_options(n->category),
					monitor_mode & 1,
					monitor_mode & 2,
					monitor_mode & 4);

			dag_node_add_source_file(n, monitor_exe, NULL);

			log_name = string_format("%s.summary", log_name_prefix);
			dag_node_add_target_file(n, log_name, NULL);

			free(log_name);
			if(monitor_mode & 02)
			{
				log_name = string_format("%s.series", log_name_prefix);
				dag_node_add_target_file(n, log_name, NULL);
				free(log_name);
			}

			if(monitor_mode & 04)
			{
				log_name = string_format("%s.files", log_name_prefix);
				dag_node_add_target_file(n, log_name, NULL);
				free(log_name);
			}

			free(log_name_prefix);
		}
	}

	return 1;
}

void dag_parse_node_set_command(struct lexer_book *bk, struct dag_node *n, char *command)
{
	struct dag_lookup_set s = { bk->d, bk->category, n, NULL };
	char *local = dag_lookup_str("BATCH_LOCAL", &s);

	if(local) {
		if(string_istrue(local))
			n->local_job = 1;
		free(local);
	}

	n->original_command = xxstrdup(command);
	n->command = translate_command(n, command, n->local_job);
	debug(D_DEBUG, "node command=%s", n->command);
}

int dag_parse_node_command(struct lexer_book *bk, struct dag_node *n, char *line)
{
	char *command = line;

	while(*command && isspace(*command))
		command++;

	if(strncmp(command, "LOCAL ", 6) == 0) {
		n->local_job = 1;
		command += 6;
	}

	/* Is this node a recursive call to makeflow? */
	if(strncmp(command, "MAKEFLOW ", 9) == 0) {
		return dag_parse_node_makeflow_command(bk, n, command + 9);
	}

	dag_parse_node_set_command(bk, n, command);

	return 1;
}

/* Support for recursive calls to makeflow. A recursive call is indicated in
 * the makeflow file with the following syntax:
 * \tMAKEFLOW some-makeflow-file [working-directory [wrapper]]
 *
 * If wrapper is not given, it defaults to an empty string.
 * If working-directory is not given, it defaults to ".".
 *
 * The call is then as:
 *
 * cd working-directory && wrapper makeflow_exe some-makeflow-file
 *
 * */

int dag_parse_node_makeflow_command(struct lexer_book *bk, struct dag_node *n, char *line)
{
	int argc;
	char **argv;
	char *wrapper = NULL;
	char *command = NULL;

	n->nested_job = 1;
	string_split_quotes(line, &argc, &argv);
	switch (argc) {
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
		dag_parse_error(bk, "node makeflow command");
		goto failure;
	}

	wrapper = wrapper ? wrapper : "";
	command = xxmalloc(sizeof(char) * (strlen(n->makeflow_cwd) + strlen(wrapper) + strlen(makeflow_exe) + strlen(n->makeflow_dag) + 20));
	sprintf(command, "cd %s && %s %s %s", n->makeflow_cwd, wrapper, makeflow_exe, n->makeflow_dag);

	dag_parse_node_filelist(bk, n, argv[0], 1);
	dag_parse_node_set_command(bk, n, command);

	free(argv);
	free(command);
	return 1;
      failure:
	free(argv);
	return 0;
}

int dag_parse_export(struct lexer_book *bk, char *line)
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
		if(equal) {
			if(!dag_parse_variable(bk, NULL, argv[i])) {
				return 0;
			} else {
				*equal = '\0';
				setenv(argv[i], equal + 1, 1);	//this shouldn't be here...
			}
		}
		list_push_tail(bk->d->export_list, xxstrdup(argv[i]));
		debug(D_DEBUG, "export variable=%s", argv[i]);
	}
	free(argv);
	return 1;
}


void dag_export_variables(struct dag *d, struct dag_node *n)
{
	struct dag_lookup_set s = { d, n->category, n, NULL };
	char *key;

	list_first_item(d->export_list);
	while((key = list_next_item(d->export_list))) {
		char *value = dag_lookup_str(key, &s);
		if(value) {
			setenv(key, value, 1);
			debug(D_DEBUG, "export %s=%s", key, value);
		}
	}
}

void dag_node_submit(struct dag *d, struct dag_node *n)
{
	char *input_files  = NULL;
	char *output_files = NULL;
	struct dag_file *f;
	const char *remotename;

	char current_dir[PATH_MAX];
	char abs_name[PATH_MAX];
	getcwd(current_dir, PATH_MAX);

	struct batch_queue *thequeue;

	if(n->local_job) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}


	int len = 0, len_temp;
	char *tmp;

	printf("%s\n", n->command);

	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
		remotename = dag_file_remote_name(n, f->filename);
		if(!remotename)
			remotename = f->filename;

		switch (batch_queue_get_type(thequeue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			tmp = string_format("%s=%s,", f->filename, remotename);
			break;
		case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
			if(f->filename[0] == '/')
			{
				tmp = string_format("%s=%s,", f->filename, remotename);
			}
			else
			{
				char *tmp_name = string_format("%s/%s", current_dir, f->filename); 
				string_collapse_path(tmp_name, abs_name, 1);
				free(tmp_name);
				tmp = string_format("%s=%s,", abs_name, remotename);
			}
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			tmp = string_format("%s,", remotename);
			break;
		default:
			tmp = string_format("%s,", f->filename);
		}
		len_temp = strlen(tmp);
		input_files = realloc(input_files, (len + len_temp + 1) * sizeof(char));
		memcpy(input_files + len, tmp, len_temp + 1);
		len += len_temp;
	}


	len = 0;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		remotename = dag_file_remote_name(n, f->filename);
		if(!remotename)
			remotename = f->filename;

		switch (batch_queue_get_type(thequeue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			tmp = string_format("%s=%s,", f->filename, remotename);
			break;
		case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
			if(f->filename[0] == '/')
			{
				tmp = string_format("%s=%s,", f->filename, remotename);
			}
			else
			{
				char *tmp_name = string_format("%s/%s", current_dir, f->filename); 
				string_collapse_path(tmp_name, abs_name, 1);
				free(tmp_name);
				tmp = string_format("%s=%s,", abs_name, remotename);
			}
			break;
		case BATCH_QUEUE_TYPE_CONDOR:
			tmp = string_format("%s,", remotename);
			break;
		default:
			tmp = string_format("%s,", f->filename);
		}
		len_temp = strlen(tmp);
		output_files = realloc(output_files, (len + len_temp + 1) * sizeof(char));
		memcpy(output_files + len, tmp, len_temp + 1);
		len += len_temp;
	}

	/* Before setting the batch job options (stored in the "BATCH_OPTIONS"
	 * variable), we must save the previous global queue value, and then
	 * restore it after we submit. */
	struct dag_lookup_set s = { d, n->category, n, NULL };
	char *batch_options_env    = dag_lookup_str("BATCH_OPTIONS", &s);
	char *batch_submit_options = dag_task_category_wrap_options(n->category, batch_options_env, batch_queue_get_type(thequeue));
	char *old_batch_submit_options = NULL;

	free(batch_options_env);
	if(batch_submit_options) {
		debug(D_DEBUG, "Batch options: %s\n", batch_submit_options);
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

		fprintf(stderr, "couldn't submit batch job, still trying...\n");

		if(time(0) > stoptime) {
			fprintf(stderr, "unable to submit job after %d seconds!\n", dag_submit_timeout);
			break;
		}

		sleep(waittime);
		waittime *= 2;
		if(waittime > 60)
			waittime = 60;
	}

	/* Restore old batch job options. */
	if(old_batch_submit_options) {
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

	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
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
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			if(access(f->filename, R_OK) != 0) {
				fprintf(stderr, "%s did not create file %s\n", n->command, f->filename);
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
			fprintf(stderr, "%s failed with exit code %d\n", n->command, info->exit_code);
		} else {
			fprintf(stderr, "%s crashed with signal %d (%s)\n", n->command, info->exit_signal, strsignal(info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		dag_node_state_change(d, n, DAG_NODE_STATE_FAILED);
		if(monitor_mode && info->exit_code == 147)
		{
			fprintf(stderr, "\nrule %d failed because it exceeded the resources limits.\n", n->nodeid);
			char *log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
			char *summary_name = string_format("%s.summary", log_name_prefix);
			struct rmsummary *s = rmsummary_parse_limits_exceeded(summary_name);

			if(s)
			{
				rmsummary_print(stderr, s);
				free(s);
				fprintf(stderr, "\n");
			}

			free(log_name_prefix);
			free(summary_name);

			dag_failed_flag = 1;
		}
		else if(dag_retry_flag || info->exit_code == 101) {
			n->failure_count++;
			if(n->failure_count > dag_retry_max) {
				fprintf(stderr, "job %s failed too many times.\n", n->command);
				dag_failed_flag = 1;
			} else {
				fprintf(stderr, "will retry failed job %s\n", n->command);
				dag_node_state_change(d, n, DAG_NODE_STATE_WAITING);
			}
		} 
		else
		{
			dag_failed_flag = 1;
		}
	} else {
		/* Record which target files have been generated by this node. */
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			hash_table_insert(d->completed_files, f->filename, f->filename);
		}

		/* Mark source files that have been used by this node */
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files)))
			f->ref_count+= -1;

		set_first_element(d->collect_table);
		while((f = set_next_element(d->collect_table))) {
			debug(D_DEBUG, "%s: %d\n", f->filename, f->ref_count);
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
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if(hash_table_lookup(d->completed_files, f->filename)) {
				continue;
			}

			if(access(f->filename, R_OK) == 0) {
				hash_table_insert(d->completed_files, f->filename, f->filename);
				continue;
			}

			if(f->target_of) {
				continue;
			}

			if(!error) {
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

int dag_gc_file(struct dag *d, const struct dag_file *f)
{
	if(access(f->filename, R_OK) == 0 && unlink(f->filename) < 0) {
		debug(D_NOTICE, "makeflow: unable to collect %s: %s", f->filename, strerror(errno));
		return 0;
	} else {
		debug(D_DEBUG, "Garbage collected %s\n", f->filename);
		set_remove(d->collect_table, f);
		return 1;
	}
}

void dag_gc_all(struct dag *d, int maxfiles)
{
	int collected = 0;
	struct dag_file *f;
	timestamp_t start_time, stop_time;

	/* This will walk the table of files to collect and will remove any
	 * that are below or equal to the threshold. */
	start_time = timestamp_get();
	set_first_element(d->collect_table);
	while((f = set_next_element(d->collect_table)) && collected < maxfiles) {
		if(f->ref_count < 1 && dag_gc_file(d, f))
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
		fprintf(d->logfile, "# GC\t%" PRIu64 "\t%d\t%" PRIu64 "\t%d\n", timestamp_get(), collected, stop_time - start_time, dag_gc_collected);
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

	switch (dag_gc_method) {
	case DAG_GC_REF_COUNT:
		debug(D_DEBUG, "Performing incremental file (%d) garbage collection", dag_gc_param);
		dag_gc_all(d, dag_gc_param);
		break;
	case DAG_GC_ON_DEMAND:
		getcwd(cwd, PATH_MAX);
		if(directory_inode_count(cwd) >= dag_gc_param || directory_low_disk(cwd)) {
			debug(D_DEBUG, "Performing on demand (%d) garbage collection", dag_gc_param);
			dag_gc_all(d, INT_MAX);
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
		if(dag_gc_method != DAG_GC_NONE && dag_gc_barrier == 0) {
			dag_gc(d);
			dag_gc_barrier = MAX(d->nodeid_counter * dag_gc_task_ratio, 1);
		}
	}

	if(dag_abort_flag) {
		dag_abort_all(d);
	} else {
		if(!dag_failed_flag && dag_gc_method != DAG_GC_NONE) {
			dag_gc_all(d, INT_MAX);
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
	fprintf(stdout, "Frequently used options:\n\n");
	fprintf(stdout, " %-30s Clean up: remove logfile and all targets.\n", "-c,--clean");
	fprintf(stdout, " %-30s Batch system type: (default is local)\n", "-T,--batch-type=<type>");
	fprintf(stdout, " %-30s %s\n\n", "", batch_queue_type_string());
	fprintf(stdout, "Other options are:\n");
	fprintf(stdout, " %-30s Advertise the master information to a catalog server.\n", "-a,--advertise");
	fprintf(stdout, " %-30s Disable the check for AFS. (experts only.)\n", "-A,--disable-afs-check");
	fprintf(stdout, " %-30s Create portable bundle of workflow in <directory>\n", "-b,--bundle-dir=<directory>");
	fprintf(stdout, " %-30s Add these options to all batch submit files.\n", "-B,--batch-options=<options>");
	fprintf(stdout, " %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT \n", "-C,--catalog-server=<catalog>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem\n", "-d,--debug=<subsystem>");
	fprintf(stdout, " %-30s Write summary of workflow to this file upon success or failure.\n", "-f,--summary-log=<file>");
	fprintf(stdout, " %-30s Work Queue fast abort multiplier.           (default is deactivated)\n", "-F,--wq-fast-abort=<#>");
	fprintf(stdout, " %-30s Show this help screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Show the pre-execution analysis of the Makeflow script - <dagfile>.\n", "-i,--analyze-exec");
	fprintf(stdout, " %-30s Show input files.\n", "-I,--show-input");
	fprintf(stdout, " %-30s Max number of local jobs to run at once.    (default is # of cores)\n", "-j,--max-local=<#>");
	fprintf(stdout, " %-30s Max number of remote jobs to run at once.   (default is 100)\n", "-J,--max-remote=<#>");
	fprintf(stdout, " %-30s Syntax check.\n", "-k,--syntax-check");
	fprintf(stdout, " %-30s Preserve (i.e., do not clean intermediate symbolic links)\n", "-K,--preserve-links");
	fprintf(stdout, " %-30s Use this file for the makeflow log.         (default is X.makeflowlog)\n", "-l,--makeflow-log=<logfile>");
	fprintf(stdout, " %-30s Use this file for the batch system log.     (default is X.<type>log)\n", "-L,--batch-log=<logfile>");
	fprintf(stdout, " %-30s Send summary of workflow to this email address upon success or failure.\n", "-m,--email=<email>");
	fprintf(stdout, " %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	fprintf(stdout, " %-30s Send debugging to this file.\n", "-o,--debug-output=<file>");
	fprintf(stdout, " %-30s Show output files.\n", "-O,--show-output");
	fprintf(stdout, " %-30s Password file for authenticating workers.\n", "   --password");
	fprintf(stdout, " %-30s Port number to use with Work Queue.       (default is %d, 0=arbitrary)\n", "-p,--port=<port>", WORK_QUEUE_DEFAULT_PORT);
	fprintf(stdout, " %-30s Priority. Higher the value, higher the priority.\n", "-P,--priority=<integer>");
	fprintf(stdout, " %-30s Automatically retry failed batch jobs up to %d times.\n", "-R,--retry", dag_retry_max);
	fprintf(stdout, " %-30s Automatically retry failed batch jobs up to n times.\n", "-r,--retry-count=<n>");
	fprintf(stdout, " %-30s Time to retry failed batch job submission.  (default is %ds)\n", "-S,--submission-timeout=<#>", dag_submit_timeout);
	fprintf(stdout, " %-30s Work Queue keepalive timeout.               (default is %ds)\n", "-t,--wq-keepalive-timeout=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	fprintf(stdout, " %-30s Work Queue keepalive interval.              (default is %ds)\n", "-u,--wq-keepalive-interval=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	fprintf(stdout, " %-30s Show version string\n", "-v,--version");
	fprintf(stdout, " %-30s Work Queue scheduling algorithm.            (time|files|fcfs)\n", "-W,--wq-schedule=<mode>");
	fprintf(stdout, " %-30s Force failure on zero-length output files \n", "-z,--zero-length-error");
	fprintf(stdout, " %-30s Select port at random and write it to this file.\n", "-Z,--port-file=<file>");

	fprintf(stdout, "\n*Monitor Options:\n\n");
	fprintf(stdout, " %-30s Enable the resource monitor, and write the monitor logs to <dir>.\n", "-M,--monitor=<dir>");
	fprintf(stdout, " %-30s Use <file> as value-pairs for resource limits.\n", "--monitor-limits=<file>");
	fprintf(stdout, " %-30s Set monitor interval to <#> seconds.        (default is 1 second)\n", "--monitor-interval=<#>");
	fprintf(stdout, " %-30s Enable monitor time series.                 (default is disabled)\n", "--monitor-with-time-series");
	fprintf(stdout, " %-30s Enable monitoring of openened files.        (default is disabled)\n", "--monitor-with-opened-files");
	fprintf(stdout, " %-30s Format for monitor logs.                    (default %s)\n", "--monitor-log-fmt=<fmt>", DEFAULT_MONITOR_LOG_FORMAT);

	fprintf(stdout, "\n*Display Options:\n\n");
	fprintf(stdout, " %-30s Display the Makefile as a Dot graph or a PPM completion graph.\n", "-D,--display=<opt>");
	fprintf(stdout, " %-30s Where <opt> is:\n", "");
	fprintf(stdout, " %-35s dot      Standard Dot graph\n", "");
	fprintf(stdout, " %-35s ppm      Display a completion graph in PPM format\n", "");

	fprintf(stdout, " %-30s Condense similar boxes.\n", "--dot-merge-similar");
	fprintf(stdout, " %-30s Change the size of the boxes proportional to file size.\n", "--dot-proportional");
	fprintf(stdout, "\nThe following options for ppm generation are mutually exclusive:\n\n");
	fprintf(stdout, " %-30s Highlight row <row> in completion grap\n", "--ppm-highlight-row=<row>");
	fprintf(stdout, " %-30s Highlight node that creates file <file> in completion graph\n", "--ppm-highlight-file=<file>");
	fprintf(stdout, " %-30s Highlight executable <exe> in completion grap\n", "--ppm-highlight-exe=<exe>");
	fprintf(stdout, " %-30s Display different levels of depth in completion graph\n", "--ppm-show-levels");
}




static void summarize(FILE * file, FILE * email, const char *format, ...)
{
	va_list args;
	if(file) {
		va_start(args, format);
		vfprintf(file, format, args);
		va_end(args);
	}
	if(email) {
		va_start(args, format);
		vfprintf(email, format, args);
		va_end(args);
	}
}

static void create_summary(struct dag *d, const char *write_summary_to, const char *email_summary_to, timestamp_t runtime, timestamp_t time_completed, int argc, char *argv[], const char *dagfile)
{
	char buffer[50];
	FILE *summary_file = NULL;
	FILE *summary_email = NULL;
	if(write_summary_to)
		summary_file = fopen(write_summary_to, "w");
	if(email_summary_to) {
		summary_email = popen("sendmail -t", "w");
		fprintf(summary_email, "To: %s\n", email_summary_to);
		timestamp_fmt(buffer, 50, "%c", time_completed);
		fprintf(summary_email, "Subject: Makeflow Run Summary - %s \n", buffer);
	}

	int i;
	for(i = 0; i < argc; i++)
		summarize(summary_file, summary_email, "%s ", argv[i]);
	summarize(summary_file, summary_email, "\n");

	if(dag_abort_flag)
		summarize(summary_file, summary_email, "Workflow aborted:\t ");
	else if(dag_failed_flag)
		summarize(summary_file, summary_email, "Workflow failed:\t ");
	else
		summarize(summary_file, summary_email, "Workflow completed:\t ");
	timestamp_fmt(buffer, 50, "%c\n", time_completed);
	summarize(summary_file, summary_email, "%s", buffer);

	int seconds = runtime / 1000000;
	int hours = seconds / 3600;
	int minutes = (seconds - hours * 3600) / 60;
	seconds = seconds - hours * 3600 - minutes * 60;
	summarize(summary_file, summary_email, "Total runtime:\t\t %d:%02d:%02d\n", hours, minutes, seconds);

	summarize(summary_file, summary_email, "Workflow file:\t\t %s\n", dagfile);

	struct dag_node *n;
	struct dag_file *f;
	const char *fn;
	struct stat st;
	const char *size;
	dag_node_state_t state;
	struct list *output_files;
	output_files = list_create();
	struct list *failed_tasks;
	failed_tasks = list_create();
	int total_tasks = itable_size(d->node_table);
	int tasks_completed = 0;
	int tasks_aborted = 0;
	int tasks_unrun = 0;

	for(n = d->nodes; n; n = n->next) {
		state = n->state;
		if(state == DAG_NODE_STATE_FAILED && !list_find(failed_tasks, (int (*)(void *, const void *)) string_equal, (void *) fn))
			list_push_tail(failed_tasks, (void *) n->command);
		else if(state == DAG_NODE_STATE_ABORTED)
			tasks_aborted++;
		else if(state == DAG_NODE_STATE_COMPLETE) {
			tasks_completed++;
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files))) {
				fn = f->filename;
				if(!list_find(output_files, (int (*)(void *, const void *)) string_equal, (void *) fn))
					list_push_tail(output_files, (void *) fn);
			}
		} else
			tasks_unrun++;
	}

	summarize(summary_file, summary_email, "Number of tasks:\t %d\n", total_tasks);
	summarize(summary_file, summary_email, "Completed tasks:\t %d/%d\n", tasks_completed, total_tasks);
	if(tasks_aborted != 0)
		summarize(summary_file, summary_email, "Aborted tasks:\t %d/%d\n", tasks_aborted, total_tasks);
	if(tasks_unrun != 0)
		summarize(summary_file, summary_email, "Tasks not run:\t\t %d/%d\n", tasks_unrun, total_tasks);
	if(list_size(failed_tasks) > 0)
		summarize(summary_file, summary_email, "Failed tasks:\t\t %d/%d\n", list_size(failed_tasks), total_tasks);
	for(list_first_item(failed_tasks); (fn = list_next_item(failed_tasks)) != NULL;)
		summarize(summary_file, summary_email, "\t%s\n", fn);

	if(list_size(output_files) > 0) {
		summarize(summary_file, summary_email, "Output files:\n");
		for(list_first_item(output_files); (fn = list_next_item(output_files)) != NULL;) {
			stat(fn, &st);
			size = string_metric(st.st_size, -1, NULL);
			summarize(summary_file, summary_email, "\t%s\t%s\n", fn, size);
		}
	}

	list_free(output_files);
	list_delete(output_files);
	list_free(failed_tasks);
	list_delete(failed_tasks);

	if(write_summary_to) {
		fprintf(stderr, "writing summary to %s.\n", write_summary_to);
		fclose(summary_file);
	}
	if(email_summary_to) {
		fprintf(stderr, "emailing summary to %s.\n", email_summary_to);
		fclose(summary_email);
	}
}

int main(int argc, char *argv[])
{
	int c;
	char *logfilename = NULL;
	char *batchlogfilename = NULL;
	char *bundle_directory = NULL;
	int clean_mode = 0;
	int display_mode = 0;
	int condense_display = 0;
	int change_size = 0;
	int syntax_check = 0;
	char *email_summary_to = NULL;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int skip_afs_check = 0;
	int preserve_symlinks = 0;
	int ppm_mode = 0;
	char *ppm_option = NULL;
	const char *batch_submit_options = getenv("BATCH_OPTIONS");
	int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	int work_queue_estimate_capacity_on = 1; // capacity estimation is on by default
	int work_queue_keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	int work_queue_keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT;
	char *write_summary_to = NULL;
	char *catalog_host;
	int catalog_port;
	int port_set = 0;
	timestamp_t runtime = 0;
	timestamp_t time_completed = 0;
	char *s;

	random_init();

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

	struct option long_options[] = {
		{"debug", required_argument, 0, 'd'},
		{"help", no_argument, 0, 'h'},
		{"clean", no_argument, 0, 'c'},
		{"batch-type", required_argument, 0, 'T'},
		{"advertise", no_argument, 0, 'a'},
		{"disable-afs-check", no_argument, 0, 'A'},
		{"bundle-dir", required_argument, 0, 'b'},
		{"batch-options", required_argument, 0, 'B'},
		{"catalog-server", required_argument, 0, 'C'},
		{"display-mode", required_argument, 0, 'D'},
		{"dot-merge-similar", no_argument, 0,  LONG_OPT_DOT_CONDENSE},
		{"dot-proportional",  no_argument, 0,  LONG_OPT_DOT_PROPORTIONAL},
		{"ppm-highlight-row", required_argument, 0, LONG_OPT_PPM_ROW},
		{"ppm-highlight-exe", required_argument, 0, LONG_OPT_PPM_EXE},
		{"ppm-highlight-file", required_argument, 0, LONG_OPT_PPM_FILE},
		{"ppm-show-levels", no_argument, 0, LONG_OPT_PPM_LEVELS},
		{"wq-estimate-capacity", no_argument, 0, 'E'},
		{"summary-log", no_argument, 0, 'f'},
		{"wq-fast-abort", required_argument, 0, 'F'},
		{"analyze-exec", no_argument, 0, 'i'},
		{"show-input", no_argument, 0, 'I'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
		{"syntax-check", no_argument, 0, 'k'},
		{"preserve-links", no_argument, 0, 'K'},
		{"makeflow-log", required_argument, 0, 'l'},
		{"batch-log", required_argument, 0, 'L'},
		{"email", required_argument, 0, 'm'},
		{"monitor", required_argument, 0, 'M'},
		{"monitor-interval", required_argument, 0, LONG_OPT_MONITOR_INTERVAL},
		{"monitor-log-name", required_argument, 0, LONG_OPT_MONITOR_LOG_NAME},
		{"monitor-limits", required_argument,   0, LONG_OPT_MONITOR_LIMITS},
		{"monitor-with-time-series",  no_argument, 0, LONG_OPT_MONITOR_TIME_SERIES},
		{"monitor-with-opened-files", no_argument, 0, LONG_OPT_MONITOR_OPENED_FILES},
		{"password", required_argument, 0, LONG_OPT_PASSWORD},
		{"project-name", required_argument, 0, 'N'},
		{"debug-output", required_argument, 0, 'o'},
		{"show-output", no_argument, 0, 'O'},
		{"port", required_argument, 0, 'p'},
		{"priority", required_argument, 0, 'P'},
		{"retry", no_argument, 0, 'R'},
		{"retry-count", required_argument, 0, 'r'},
		{"submission-timeout", required_argument, 0, 'S'},
		{"wq-keepalive-timeout", required_argument, 0, 't'},
		{"wq-keepalive-interval", required_argument, 0, 'u'},
		{"version", no_argument, 0, 'v'},
		{"wq-schedule", required_argument, 0, 'W'},
		{"zero-length-error", no_argument, 0, 'z'},
		{"port-file", required_argument, 0, 'Z'},
		{0, 0, 0, 0}
	};


	while((c = getopt_long(argc, argv, "aAb:B:cC:d:D:Ef:F:g:G:hiIj:J:kKl:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:zZ:", long_options, NULL)) >= 0) {
		switch (c) {
		case 'a':
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'A':
			skip_afs_check = 1;
			break;
		case 'b':
			bundle_directory = xxstrdup(optarg);
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
			if(strcasecmp(optarg, "dot") == 0)
				display_mode = SHOW_DAG_DOT;
			else if (strcasecmp(optarg, "ppm") == 0)
				display_mode = SHOW_DAG_PPM;
			else
				fatal("Unknown display option: %s\n", optarg);
			break;
		case LONG_OPT_DOT_CONDENSE:
			display_mode = SHOW_DAG_DOT;
			condense_display = 1;
			break;
		case LONG_OPT_DOT_PROPORTIONAL:
			display_mode = SHOW_DAG_DOT;
			change_size = 1;
			break;
		case LONG_OPT_PPM_EXE:
			display_mode = SHOW_DAG_PPM;
			ppm_option = optarg;
			ppm_mode = 2;
			break;
		case LONG_OPT_PPM_FILE:
			display_mode = SHOW_DAG_PPM;
			ppm_option = optarg;
			ppm_mode = 3;
			break;
		case LONG_OPT_PPM_ROW:
			display_mode = SHOW_DAG_PPM;
			ppm_option = optarg;
			ppm_mode = 4;
			break;
		case LONG_OPT_PPM_LEVELS:
			display_mode = SHOW_DAG_PPM;
			ppm_mode = 5;
			break;
		case 'E':
			// This option is deprecated. Capacity estimation is now on by default.
			work_queue_estimate_capacity_on = 1;
			break;
		case 'f':
			write_summary_to = xxstrdup(optarg);
			break;
		case 'F':
			wq_option_fast_abort_multiplier = atof(optarg);
			break;
		case 'g':
			if(strcasecmp(optarg, "none") == 0) {
				dag_gc_method = DAG_GC_NONE;
			} else if(strcasecmp(optarg, "ref_count") == 0) {
				dag_gc_method = DAG_GC_REF_COUNT;
				if(dag_gc_param < 0)
					dag_gc_param = 16;	/* Try to collect at most 16 files. */
			} else if(strcasecmp(optarg, "on_demand") == 0) {
				dag_gc_method = DAG_GC_ON_DEMAND;
				if(dag_gc_param < 0)
					dag_gc_param = 1 << 14;	/* Inode threshold of 2^14. */
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
		case 'm':
			email_summary_to = xxstrdup(optarg);
			break;
		case 'N':
			free(project);
			project = xxstrdup(optarg);
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
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
			port_set = 1;	//WQ is going to set the port, so we continue as if already set.
			break;
		case 'M':
			monitor_mode |= 01;
			if(monitor_log_dir)
				free(monitor_log_dir);
			monitor_log_dir = xxstrdup(optarg);
			break;
		case LONG_OPT_MONITOR_LIMITS:
			monitor_mode |= 01;
			if(monitor_limits_name)
				free(monitor_limits_name);
			monitor_limits_name = xxstrdup(optarg);
			break;
		case LONG_OPT_MONITOR_INTERVAL:
			monitor_mode |= 01;
			monitor_interval = atoi(optarg);
			break;
		case LONG_OPT_MONITOR_TIME_SERIES:
			monitor_mode |= 02;
			break;
		case LONG_OPT_MONITOR_OPENED_FILES:
			monitor_mode |= 04;
			break;
		case LONG_OPT_MONITOR_LOG_NAME:
			monitor_mode |= 01;
			if(monitor_log_format)
				free(monitor_log_format);
			monitor_log_format = xxstrdup(optarg);
			break;
		case LONG_OPT_PASSWORD:
			if(copy_file_to_buffer(optarg, &wq_password) < 0) {
				fprintf(stderr, "makeflow: couldn't open %s: %s\n", optarg, strerror(errno));
				return 1;
			}
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

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE || batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
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
		case BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS:
			batchlogfilename = string_format("%s.wqlog", dagfile);
			break;
		default:
			batchlogfilename = string_format("%s.batchlog", dagfile);
			break;
		}

		// In clean mode, delete all existing log files
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

	if(monitor_mode) {

		if(!monitor_log_dir)
		{
			fatal("Monitor mode was enabled, but a log output directory was not specified (use -M<dir>)");
		}

		monitor_exe = resource_monitor_copy_to_wd(NULL);

		if(monitor_interval < 1)
			fatal("Monitoring interval should be non-negative.");

		if(!monitor_log_format)
			monitor_log_format = DEFAULT_MONITOR_LOG_FORMAT;
	}

	struct dag *d = dag_from_file(dagfile);
	if(!d) {
		free(logfilename);
		free(batchlogfilename);
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

	if(syntax_check) {
		fprintf(stdout, "%s: Syntax OK.\n", dagfile);
		return 0;
	}

	if(bundle_directory) {
		//Create Bundle!
		fprintf(stderr, "Creating workflow bundle...\n");

		struct stat s;
		if(!stat(bundle_directory, &s)) {
			fprintf(stderr, "Target directory, %s, already exists.\n", bundle_directory);
			exit(1);
		}
		fprintf(stderr, "Creating new directory, %s ..........", bundle_directory);
		if(!create_dir(bundle_directory, 0755)) {
			fprintf(stderr, "FAILED\n");
			exit(1);
		}
		fprintf(stderr, "COMPLETE\n");

		dag_show_input_files(d);
		collect_input_files(d, bundle_directory, bundler_rename);

		char output_makeflow[PATH_MAX];
		sprintf(output_makeflow, "%s/%s", bundle_directory, dagfile);
		fprintf(stderr, "Writing workflow, %s, to %s\n", dagfile, output_makeflow);
		dag_to_file(d, output_makeflow, bundler_rename);
		free(bundle_directory);
		exit(0);
	}

	if(display_mode)
	{
		switch(display_mode)
		{
		case SHOW_INPUT_FILES:
			dag_show_input_files(d);
			break;

		case SHOW_OUTPUT_FILES:
			dag_show_output_files(d);
			break;

		case SHOW_MAKEFLOW_ANALYSIS:
			dag_show_analysis(d);
			break;

		case SHOW_DAG_DOT:
			dag_to_dot(d, condense_display, change_size);
			break;

		case SHOW_DAG_PPM:
			dag_to_ppm(d, ppm_mode, ppm_option);
			break;

		default:
			fatal("Unknown display option.");
			break;
		}

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
			d->remote_jobs_max = 10 * MAX_REMOTE_JOBS_DEFAULT;
		} else if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
			d->remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
		} else {
			d->remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
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

	if(!dag_prepare_for_monitoring(d)) {
		fatal("Could not prepare for monitoring.\n");
	}

	if(!dag_prepare_for_batch_system(d)) {
		fatal("Could not prepare for submission to batch system.\n");
	}

	if(dag_gc_method != DAG_GC_NONE)
		dag_prepare_gc(d);

	dag_prepare_nested_jobs(d);

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

	dag_log_recover(d, logfilename);

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE || batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		struct work_queue *q = batch_queue_get_work_queue(remote_queue);
		if(!q) {
			fprintf(stderr, "makeflow: cannot get work queue object.\n");
			exit(1);
		}

		if(wq_password)
			work_queue_specify_password(q, wq_password);
		work_queue_specify_master_mode(q, work_queue_master_mode);
		work_queue_specify_name(q, project);
		work_queue_specify_priority(q, priority);
		work_queue_specify_estimate_capacity_on(q, work_queue_estimate_capacity_on);
		work_queue_specify_keepalive_interval(q, work_queue_keepalive_interval);
		work_queue_specify_keepalive_timeout(q, work_queue_keepalive_timeout);
		work_queue_enable_process_module(q);
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


	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);

	fprintf(d->logfile, "# STARTED\t%" PRIu64 "\n", timestamp_get());
	runtime = timestamp_get();
	dag_run(d);
	time_completed = timestamp_get();
	runtime = time_completed - runtime;

	batch_queue_delete(local_queue);
	batch_queue_delete(remote_queue);

	if(!preserve_symlinks && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		clean_symlinks(d, 0);
	}

	if(write_summary_to || email_summary_to)
		create_summary(d, write_summary_to, email_summary_to, runtime, time_completed, argc, argv, dagfile);
	free(logfilename);
	free(batchlogfilename);
	free(write_summary_to);
	free(email_summary_to);


	if(dag_abort_flag) {
		fprintf(d->logfile, "# ABORTED\t%" PRIu64 "\n", timestamp_get());
		fprintf(stderr, "workflow was aborted.\n");
		return 1;
	} else if(dag_failed_flag) {
		fprintf(d->logfile, "# FAILED\t%" PRIu64 "\n", timestamp_get());
		fprintf(stderr, "workflow failed.\n");
		return 1;
	} else {
		fprintf(d->logfile, "# COMPLETED\t%" PRIu64 "\n", timestamp_get());
		fprintf(stderr, "nothing left to do.\n");
		return 0;
	}
}

/* vim: set sw=8 sts=8 ts=8 ft=c: */
