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

#define SHOW_INPUT_FILES 2
#define SHOW_OUTPUT_FILES 3
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

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static char *project = NULL;
static int priority = 0;
static int port = 0;
static int output_len_check = 0;

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
	struct dag_file *next;
};

struct dag_node {
	int only_my_children;
	time_t previous_completion;
	int linenum;
	int nodeid;
	int local_job;
	int failure_count;
	dag_node_state_t state;
	const char *command;
	const char *original_command;
	struct dag_file *source_files;
	struct dag_file *target_files;
	int source_file_names_size;
	int target_file_names_size;
	batch_job_id_t jobid;
	struct dag_node *next;
	int children;
	int children_left;
	int level;
	struct hash_table *variables;
};

struct dag_lookup_set {
	struct dag *dag;
	struct dag_node *node;
	struct hash_table *table;
};

int dag_width(struct dag *d);
void dag_node_complete(struct dag *d, struct dag_node *n, struct batch_job_info *info);
char *dag_readline(struct dag *d, struct dag_node *n);
int dag_check_dependencies(struct dag *d);
int dag_parse_variable(struct dag *d, struct dag_node *n, char *line);
int dag_parse_node(struct dag *d, char *line, int clean_mode);
int dag_parse_node_filelist(struct dag *d, struct dag_node *n, char *filelist, int source, int clean_mode);
int dag_parse_node_command(struct dag *d, struct dag_node *n, char *line);
int dag_parse_export(struct dag *d, char *line);
char *dag_lookup(const char *name, void *arg);
char *dag_lookup_set(const char *name, void *arg);
void dag_gc_ref_incr(struct dag *d, const char *file, int increment);


int dag_estimate_nodes_needed(struct dag *d, int actual_max)
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
			// return if maximum number of possible nodes is reached
			if(m->only_my_children == actual_max)
				return actual_max;
		}
	}

	// find out the maximum number of direct children that a single parent node has
	for(n = d->nodes; n; n = n->next) {
		max = max < n->only_my_children ? n->only_my_children : max;
	}

	return max;
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
		debug(D_DEBUG, "%s", key);
	}

	hash_table_delete(ih);
}

void dag_show_output_files(struct dag *d)
{
	char *key;
	void *value;

	hash_table_firstkey(d->file_table);

	while(hash_table_nextkey(d->file_table, &key, &value)) {
		debug(D_DEBUG, "%s", key);
	}
}

static int handle_auto_workers(struct dag *d, int auto_workers)
{
	char hostname[DOMAIN_NAME_MAX];
	int num_of_workers;

	domain_name_cache_guess(hostname);

	if(auto_workers == MAKEFLOW_AUTO_GROUP) {
		num_of_workers = dag_estimate_nodes_needed(d, d->remote_jobs_max);
	} else {		/* if (auto_workers == MAKEFLOW_AUTO_WIDTH) ALWAYS TRUE */

		num_of_workers = dag_width(d);
		if(num_of_workers > d->remote_jobs_max)
			num_of_workers = d->remote_jobs_max;
	}

	char *start_worker_command = string_format("condor_submit_workers %s %d %d", hostname, port, num_of_workers);
	debug(D_DEBUG, "starting %d workers: `%s`", num_of_workers, start_worker_command);
	int status = system(start_worker_command);
	free(start_worker_command);
	if(status) {
		debug(D_DEBUG, "unable to start workers.");
		return 0;
	}

	return 1;
}

/* Code added by kparting to compute the width of the graph.
   Original algorithm by pbui, with improvements by kparting */

int dag_width(struct dag *d)
{
	struct dag_node *n, *tmp;
	struct dag_file *f;

	/* 1. Find the number of immediate children for all nodes; also,
	   determine leaves by adding nodes with children==0 to list. */

	for(n = d->nodes; n != NULL; n = n->next) {
		for(f = n->source_files; f != NULL; f = f->next) {
			tmp = (struct dag_node *) hash_table_lookup(d->file_table, f->filename);
			if(!tmp)
				continue;
			++tmp->children;
		}
	}

	struct list *list = list_create();

	for(n = d->nodes; n != NULL; n = n->next) {
		n->children_left = n->children;
		if(n->children == 0)
			list_push_tail(list, n);
	}

	/* 2. Assign every node a "reverse depth" level. Normally by depth,
	   I mean topologically sort and assign depth=0 to nodes with no
	   parents. However, I'm thinking I need to reverse this, with depth=0
	   corresponding to leaves. Also, we want to make sure that no node is
	   added to the queue without all its children "looking at it" first
	   (to determine its proper "depth level"). */

	int max_level = 0;

	while(list_size(list) > 0) {
		struct dag_node *n = (struct dag_node *) list_pop_head(list);

		for(f = n->source_files; f != NULL; f = f->next) {
			tmp = (struct dag_node *) hash_table_lookup(d->file_table, f->filename);
			if(!tmp)
				continue;

			if(tmp->level < n->level + 1)
				tmp->level = n->level + 1;

			if(tmp->level > max_level)
				max_level = tmp->level;

			--tmp->children_left;
			if(tmp->children_left == 0)
				list_push_tail(list, tmp);
		}
	}

	/* 3. Now that every node has a level, simply create an array and then
	   go through the list once more to count the number of nodes in each
	   level. */

	int *level_count = malloc((max_level + 1) * sizeof(*level_count));

	int i;
	for(i = 0; i <= max_level; ++i) {	/* yes, should be <=, no joke */
		level_count[i] = 0;
	}

	for(n = d->nodes; n != NULL; n = n->next) {
		++level_count[n->level];
	}

	int max = 0;
	for(i = 0; i <= max_level; ++i) {	/* yes, should still be <=, srsly */
		if(max < level_count[i])
			max = level_count[i];
	}

	free(level_count);
	return max;
}

void dag_print(struct dag *d)
{
	struct dag_node *n;
	struct dag_file *f;

	fprintf(stdout, "digraph {\n");

	fprintf(stdout, "node [shape=ellipse];\n");

	for(n = d->nodes; n; n = n->next) {
		char *name = xxstrdup(n->command);
		char *label = strtok(name, " \t\n");
		fprintf(stdout, "N%d [label=\"%s\"];\n", n->nodeid, label);
		free(name);
	}

	fprintf(stdout, "node [shape=box];\n");

	for(n = d->nodes; n; n = n->next) {
		for(f = n->source_files; f; f = f->next) {
			fprintf(stdout, "\"%s\" -> N%d;\n", f->filename, n->nodeid);
		}
		for(f = n->target_files; f; f = f->next) {
			fprintf(stdout, "N%d -> \"%s\";\n", n->nodeid, f->filename);
		}
	}

	fprintf(stdout, "}\n");
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

struct dag_file *dag_file_create(const char *filename, struct dag_file *next)
{
	struct dag_file *f = malloc(sizeof(*f));
	f->filename = xxstrdup(filename);
	f->next = next;
	return f;
}

void dag_node_add_source_file(struct dag_node *n, const char *filename)
{
	n->source_files = dag_file_create(filename, n->source_files);
}

void dag_node_add_target_file(struct dag_node *n, const char *filename)
{
	n->target_files = dag_file_create(filename, n->target_files);
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

	fprintf(d->logfile, "%llu %d %d %d %d %d %d %d %d %d\n", timestamp_get(), n->nodeid, newstate, n->jobid, d->node_states[0], d->node_states[1], d->node_states[2], d->node_states[3], d->node_states[4], d->nodeid_counter);
}

void dag_abort_all(struct dag *d)
{
	UINT64_T jobid;
	struct dag_node *n;

	debug(D_DEBUG, "got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table, &jobid, (void **) &n)) {
		debug(D_DEBUG, "aborting local job %llu\n", jobid);
		batch_job_remove(local_queue, jobid);
		dag_node_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		debug(D_DEBUG, "aborting remote job %llu\n", jobid);
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
			debug(D_DEBUG, "deleted file %s\n", filename);
	} else {
		if(errno == ENOENT) {
			// nothing
		} else if(errno == EISDIR) {
			if(!delete_dir(filename)) {
				if(!silent)
					debug(D_DEBUG, "couldn't delete directory %s: %s\n", filename, strerror(errno));
			} else {
				if(!silent)
					debug(D_DEBUG, "deleted directory %s\n", filename);
			}
		} else {
			if(!silent)
				debug(D_DEBUG, "couldn't delete %s: %s\n", filename, strerror(errno));
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
}

void clean_symlinks(struct dag *d, int silent)
{
	char *key;
	void *value;

	if(batch_queue_type != BATCH_QUEUE_TYPE_CONDOR)
		return;

	hash_table_firstkey(d->filename_translation_rev);
	while(hash_table_nextkey(d->filename_translation_rev, &key, &value)) {
		file_clean(key, silent);
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

// Decide whether to rerun a node based on file system status
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
		debug(D_DEBUG, "rule still running: %s\n", n->command);
		itable_insert(d->remote_job_table, n->jobid, n);
		d->remote_jobs_running++;

		// Otherwise, we cannot reconnect to the job, so rerun it
	} else if(n->state == DAG_NODE_STATE_RUNNING || n->state == DAG_NODE_STATE_FAILED || n->state == DAG_NODE_STATE_ABORTED) {
		debug(D_DEBUG, "will retry failed rule: %s\n", n->command);
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
				goto rerun;
			}
		}
	}

	// Rerun if an output file is missing.
	for(f = n->target_files; f; f = f->next) {
		if(stat(f->filename, &filestat) < 0) {
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

	// Mark this node as having been rerun already
	itable_insert(rerun_table, n->nodeid, n);
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
	if(!strncmp(filename, "./", 2)) {
		/* Assume this is a current working directory path */
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
		if (strlen(line) == 0) {
			free(line);
			continue;
		}

		if(line[0] == '#') {
			continue;
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
		} else if(strncmp(line, "export ", 7) == 0) {
			if(!dag_parse_export(d, line)) {
				dag_parse_error(d, "export");
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
				return 0;
			} else {
				hash_table_insert(d->file_table, f->filename, n);
			}
		}
	}

	/* Parse _MAKEFLOW_COLLECT_LIST and record which target files should be
	 * garbage collected. */
	char *collect_list = dag_lookup("_MAKEFLOW_COLLECT_LIST", d);
	int i, argc;
	char **argv;

	if(collect_list == NULL)
		return 1;

	string_split_quotes(collect_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		/* Must initialize to non-zero for hash_table functions to work properly. */
		hash_table_insert(d->collect_table, argv[i], (void *)MAKEFLOW_GC_MIN_THRESHOLD);
		debug(D_DEBUG, "Added %s to garbage collection list", argv[i]);
	}
	free(argv);

	/* Mark garbage files with reference counts. This will be used to
	 * detect when it is safe to remove a garbage file. */
	for(n = d->nodes; n; n = n->next)
		for(f = n->source_files; f; f = f->next)
			dag_gc_ref_incr(d, f->filename, 1);

	return 1;
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

		/* Chop off comments
		 * TODO: this will break if we use # in a string. */
		char *hash = strrchr(raw_line, '#');
		if (hash) {
			*hash = 0;
		}

		string_chomp(raw_line);
		while(isspace(*raw_line)) {
			raw_line++;
			d->colnum++;
		}

		char *subst_line = xxstrdup(raw_line);
		subst_line = string_subst(subst_line, dag_lookup_set, &s);
		free(d->linetext);
		d->linetext = xxstrdup(subst_line);
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
		if(line[0] == '@' && strchr(line, '=')) {
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

		if(batch_queue_type == BATCH_QUEUE_TYPE_CONDOR && strchr(filename, '/')) {
			int rv = translate_filename(d, filename, &newname);
			if(rv && !clean_mode) {
				debug(D_DEBUG, "creating symlink \"./%s\" for file \"%s\"\n", newname, filename);
				rv = symlink(filename, newname);
				if(rv < 0 && errno != EEXIST) {
					/* TODO: Check for if symlink points to right place? */
					fprintf(stderr, "makeflow: could not create symbolic link (%s)\n", strerror(errno));
					goto failure;
				}

				/* Create symlink for output files, otherwise Condor will fail on write-back */
				if(!source && access(filename, R_OK) < 0) {
					int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0700);
					if(fd < 0) {
						fprintf(stderr, "makeflow: could not create symbolic link target (%s): %s\n", filename, strerror(errno));
						goto failure;
					}
					close(fd);
				}
			}

			if(newname == NULL)
				newname = filename;

			if(source) {
				dag_node_add_source_file(n, newname);
				n->source_file_names_size += strlen(filename) + 1;
			} else {
				dag_node_add_target_file(n, newname);
				n->target_file_names_size += strlen(filename) + 1;
			}
		} else if(filename[0] == '/' && batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			/* Translate only explicit absolute paths for work queue tasks.
			 * TODO: should we check this return value? */
			translate_filename(d, filename, &newname);
			/* We have to reserve extra space so we can do renaming later. */
			if(source) {
				dag_node_add_source_file(n, filename);
				n->source_file_names_size += strlen(filename) + strlen(newname) + 2;
			} else {
				dag_node_add_target_file(n, filename);
				n->target_file_names_size += strlen(filename) + strlen(newname) + 2;
			}
		} else {
			if(source) {
				dag_node_add_source_file(n, filename);
				n->source_file_names_size += strlen(filename) + 1;
			} else {
				dag_node_add_target_file(n, filename);
				n->target_file_names_size += strlen(filename) + 1;
			}
		}

		if(newname != filename)
			free(newname);
	}
	free(argv);
	return 1;

failure:
	free(argv);
	return 0;
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
	return 1;
}

int dag_parse_export(struct dag *d, char *line)
{
	int i, argc;
	char **argv;

	string_split_quotes(line + 7, &argc, &argv);
	for(i = 0; i < argc; i++) {
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
	char *filename = NULL;
	struct dag_file *f;

	struct batch_queue *thequeue;

	if(n->local_job) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}

	debug(D_DEBUG, "%s\n", n->command);

	input_files = malloc((n->source_file_names_size + 1) * sizeof(char));
	input_files[0] = '\0';
	for(f = n->source_files; f; f = f->next) {
		strcat(input_files, f->filename);
		if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			filename = hash_table_lookup(d->filename_translation_fwd, f->filename);
			if(filename) {
				strcat(input_files, "=");
				strcat(input_files, filename);
			}
		}
		strcat(input_files, ",");
	}

	output_files = malloc((n->target_file_names_size + 1) * sizeof(char));
	output_files[0] = '\0';
	for(f = n->target_files; f; f = f->next) {
		if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			filename = hash_table_lookup(d->filename_translation_fwd, f->filename);
			if(filename) {
				strcat(output_files, filename);
				strcat(output_files, "=");
			}
		}
		strcat(output_files, f->filename);
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

		debug(D_DEBUG, "couldn't submit batch job, still trying...\n");

		if(time(0) > stoptime) {
			debug(D_DEBUG, "unable to submit job after %d seconds!\n", dag_submit_timeout);
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
				debug(D_DEBUG, "%s did not create file %s\n", n->command, f->filename);
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
			debug(D_DEBUG, "%s failed with exit code %d\n", n->command, info->exit_code);
		} else {
			debug(D_DEBUG, "%s crashed with signal %d (%s)\n", n->command, info->exit_signal, strsignal(info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		dag_node_state_change(d, n, DAG_NODE_STATE_FAILED);
		if(dag_retry_flag || info->exit_code == 101) {
			n->failure_count++;
			if(n->failure_count > dag_retry_max) {
				debug(D_DEBUG, "job %s failed too many times.\n", n->command);
				dag_failed_flag = 1;
			} else {
				debug(D_DEBUG, "will retry failed job %s\n", n->command);
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
		}
		dag_node_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
}

int dag_check(struct dag *d)
{
	struct dag_node *n;
	struct dag_file *f;

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

			fprintf(stderr, "makeflow: %s does not exist, and is not created by any rule.\n", f->filename);
			clean_symlinks(d, 1);
			return 0;
		}
	}

	return 1;
}

int dag_gc_file(struct dag *d, const char *file, int count)
{
	if(unlink(file) < 0) {
		debug(D_NOTICE, "makeflow: unable to collect %s: %s", file, strerror(errno));
		return 0;
	} else {
		debug(D_DEBUG, "Garbage collected %s (%llu)", file, count - 1);
		hash_table_remove(d->collect_table, file);
		return 1;
	}
}

void dag_gc_all(struct dag *d, size_t threshold, size_t maxfiles, time_t stoptime)
{
	size_t collected = 0;
	char *key;
	PTRINT_T value;

	/* This will walk the table of files to collect and will remove any
	 * that are below or equal to the threshold. */
	hash_table_firstkey(d->collect_table);
	while(hash_table_nextkey(d->collect_table, &key, (void **)&value) && time(0) < stoptime && collected < maxfiles) {
		if(value <= threshold && dag_gc_file(d, key, value))
			collected++;
	}
}

void dag_gc_ref_incr(struct dag *d, const char *file, int increment)
{
	/* Increment the garbage file by specified amount */
	PTRINT_T ref_count = (PTRINT_T)hash_table_remove(d->collect_table, file);
	if (ref_count) {
		ref_count = ref_count + increment;
		hash_table_insert(d->collect_table, file, (void *)ref_count);
		debug(D_DEBUG, "Marked file %s references (%llu)", file, ref_count - 1);

		/* Perform collection immediately if we are using reference
		 * counting. */
		if (ref_count <= MAKEFLOW_GC_MIN_THRESHOLD && dag_gc_method == DAG_GC_REF_COUNT)
			dag_gc_file(d, file, ref_count);
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
			dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, dag_gc_param, UINT_MAX);
			break;
		case DAG_GC_INCR_TIME:
			debug(D_DEBUG, "Performing incremental time (%d) garbage collection", dag_gc_param);
			dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, UINT_MAX, time(0) + dag_gc_param);
			break;
		case DAG_GC_ON_DEMAND:
			getcwd(cwd, PATH_MAX);
			if(directory_inode_count(cwd) >= dag_gc_param || directory_low_disk(cwd)) {
				debug(D_DEBUG, "Performing on demand (%d) garbage collection", dag_gc_param);
				dag_gc_all(d, MAKEFLOW_GC_MIN_THRESHOLD, UINT_MAX, UINT_MAX);
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
			debug(D_DEBUG, "Waiting %d seconds for any job to finish ...\n", tmp_timeout);
			jobid = batch_job_wait_timeout(remote_queue, &info, time(0) + tmp_timeout);
			if(jobid > 0) {
				debug(D_DEBUG, "Job %d has returned.\n", jobid);
				n = itable_remove(d->remote_job_table, jobid);
				if(n)
					dag_node_complete(d, n, &info);
			} else {
				debug(D_DEBUG, "No job has finished in the last %d seconds.\n", tmp_timeout);
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

			debug(D_DEBUG, "Waiting %d seconds for any job to finish ...\n", tmp_timeout);
			jobid = batch_job_wait_timeout(local_queue, &info, stoptime);
			if(jobid > 0) {
				debug(D_DEBUG, "Job %d has returned.\n", jobid);
				n = itable_remove(d->local_job_table, jobid);
				if(n)
					dag_node_complete(d, n, &info);
			} else {
				debug(D_DEBUG, "No job has finished in the last %d seconds.\n", tmp_timeout);
			}
		}

		dag_gc(d);
	}

	if(dag_abort_flag) {
		dag_abort_all(d);
	} else {
		if(dag_gc_method != DAG_GC_NONE) {
			dag_gc_all(d, UINT_MAX, UINT_MAX, UINT_MAX);
		}
	}
}

static void handle_abort(int sig)
{
	dag_abort_flag = 1;
}

static void show_version(const char *cmd)
{
	fprintf(stdout, "%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " -c             Clean up: remove logfile and all targets.\n");
	fprintf(stdout, " -T <type>      Batch system type: %s. (default is local)\n", batch_queue_type_string());
	fprintf(stdout, " -j <#>         Max number of local jobs to run at once.    (default is # of cores)\n");
	fprintf(stdout, " -J <#>         Max number of remote jobs to run at once.   (default is 100)\n");
	fprintf(stdout, " -p <port>      Port number to use with work queue.         (default is %d, -1=random)\n", WORK_QUEUE_DEFAULT_PORT);
	fprintf(stdout, " -N <project>   Set the project name to <project>\n");
	fprintf(stdout, " -P <integer>   Priority. Higher the value, higher the priority.\n");
	fprintf(stdout, " -a             Advertise the master information to a catalog server.\n");
	fprintf(stdout, " -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT \n");
	fprintf(stdout, " -e             Set the work queue master to only accept workers that have the same -N <project> option.\n");
	fprintf(stdout, " -E             Enable master capacity estimation in Work Queue. Estimated master capcity may be viewed in the work queue log file or through the  work_queue_status command.\n");
	fprintf(stdout, " -M             Enable automatic excessive worker removal in Work Queue. (-E option will be automatically added when this option is given.)\n");
	fprintf(stdout, " -F <#>         Work Queue fast abort multiplier.           (default is deactivated)\n");
	fprintf(stdout, " -I             Show input files.\n");
	fprintf(stdout, " -O             Show output files.\n");
	fprintf(stdout, " -g <gc_method> Set garbage collection method.\n");
	fprintf(stdout, " -G <gc_param>  Set garbage collection parameter.\n");
	fprintf(stdout, " -D             Display the Makefile as a Dot graph.\n");
	fprintf(stdout, " -B <options>   Add these options to all batch submit files.\n");
	fprintf(stdout, " -S <timeout>   Time to retry failed batch job submission.  (default is %ds)\n", dag_submit_timeout);
	fprintf(stdout, " -r <n>         Automatically retry failed batch jobs up to n times.\n");
	fprintf(stdout, " -l <logfile>   Use this file for the makeflow log.         (default is X.makeflowlog)\n");
	fprintf(stdout, " -L <logfile>   Use this file for the batch system log.     (default is X.condorlog)\n");
	fprintf(stdout, " -A             Disable the check for AFS.                  (experts only.)\n");;
	fprintf(stdout, " -k             Syntax check.\n");
	fprintf(stdout, " -w <mode>      Auto Work Queue mode. Mode is either 'width' or 'group' (DAG [width] or largest [group] of tasks).\n");
	fprintf(stdout, " -W <mode>      Work Queue scheduling algorithm.            (time|files|fcfs)\n");
	fprintf(stdout, " -d <subsystem> Enable debugging for this subsystem\n");
	fprintf(stdout, " -o <file>      Send debugging to this file.\n");
	fprintf(stdout, " -K             Preserve (i.e., do not clean) intermediate symbolic links\n");
	fprintf(stdout, " -z             Force failure on zero-length output files \n");
	fprintf(stdout, " -v             Show version string\n");
	fprintf(stdout, " -h             Show this help screen\n");
}

int main(int argc, char *argv[])
{
	char c;
	char *logfilename = NULL;
	char *batchlogfilename = NULL;
	int clean_mode = 0;
	int display_mode = 0;
	int syntax_check = 0;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int skip_afs_check = 0;
	int preserve_symlinks = 0;
	const char *batch_submit_options = getenv("BATCH_OPTIONS");
	int auto_workers = 0;
	int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	int work_queue_worker_mode = WORK_QUEUE_WORKER_MODE_SHARED;
	int work_queue_estimate_capacity_on = 0;
	int work_queue_auto_remove_workers_on = 0;
	int work_queue_wait_routine = WORK_QUEUE_WAIT_UNSPECIFIED;
	char *catalog_host;
	int catalog_port;

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "aAB:cC:d:DeEF:g:G:hiIj:J:kKl:L:MN:o:Op:P:r:RS:t:T:vw:W:z:Z:")) != (char) -1) {
		switch (c) {
		case 'A':
			skip_afs_check = 1;
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'c':
			clean_mode = 1;
			break;
		case 'N':
			free(project);
			project = xxstrdup(optarg);
			setenv("WORK_QUEUE_NAME", project, 1);
			break;
		case 'P':
			priority = atoi(optarg);
			setenv("WORK_QUEUE_PRIORITY", optarg, 1);
			break;
		case 'a':
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'e':
			work_queue_worker_mode = WORK_QUEUE_WORKER_MODE_EXCLUSIVE;
			break;
		case 'E':
			work_queue_estimate_capacity_on = 1;
			break;
		case 'M':
			work_queue_auto_remove_workers_on = 1;
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

			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'I':
			display_mode = SHOW_INPUT_FILES;
			break;
		case 'O':
			display_mode = SHOW_OUTPUT_FILES;
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
		case 'l':
			logfilename = xxstrdup(optarg);
			break;
		case 'L':
			batchlogfilename = xxstrdup(optarg);
			break;
		case 'D':
			display_mode = 1;
			break;
		case 'k':
			syntax_check = 1;
			break;
		case 'S':
			dag_submit_timeout = atoi(optarg);
			break;
		case 'R':
			dag_retry_flag = 1;
			break;
		case 'r':
			dag_retry_flag = 1;
			dag_retry_max = atoi(optarg);
			break;
		case 'j':
			explicit_local_jobs_max = atoi(optarg);
			break;
		case 'J':
			explicit_remote_jobs_max = atoi(optarg);
			break;
		case 'B':
			batch_submit_options = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'h':
			show_help(argv[0]);
			return 0;
		case 'T':
			batch_queue_type = batch_queue_type_from_string(optarg);
			if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
				fprintf(stderr, "makeflow: unknown batch queue type: %s\n", optarg);
				return 1;
			}
			break;
		case 'w':
			if(!strcmp(optarg, "width")) {
				auto_workers = MAKEFLOW_AUTO_WIDTH;
			} else if(!strcmp(optarg, "group")) {
				auto_workers = MAKEFLOW_AUTO_GROUP;
			} else {
				show_help(argv[0]);
				exit(1);
			}
			break;
		case 'F':
			wq_option_fast_abort_multiplier = atof(optarg);
			break;
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
		case 'K':
			preserve_symlinks = 1;
			break;
		case 'Z':
			if(!strcmp(optarg, "fcfs")) {
				work_queue_wait_routine = WORK_QUEUE_WAIT_FCFS;
			} else if(!strcmp(optarg, "fd")) {
				work_queue_wait_routine = WORK_QUEUE_WAIT_FAST_DISPATCH;
			} else if(!strcmp(optarg, "adaptive")) {
				work_queue_wait_routine = WORK_QUEUE_WAIT_ADAPTIVE;
			} else {
				work_queue_wait_routine = WORK_QUEUE_WAIT_UNSPECIFIED;
			}
			break;
		case 't':
			setenv("WORK_QUEUE_CAPACITY_TOLERANCE", optarg, 1);
			break;
		case 'z':
			output_len_check = 1;
			break;
		default:
			show_help(argv[0]);
			return 1;
		}
	}

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

		char *value = string_format("%d", work_queue_worker_mode);
		setenv("WORK_QUEUE_WORKER_MODE", value, 1);
		free(value);

		value = string_format("%d", work_queue_master_mode);
		setenv("WORK_QUEUE_MASTER_MODE", value, 1);
		free(value);

		if(work_queue_estimate_capacity_on) {
			value = string_format("%d", WORK_QUEUE_SWITCH_ON);
			setenv("WORK_QUEUE_ESTIMATE_CAPACITY_ON", value, 1);
			free(value);
		}

		if(work_queue_auto_remove_workers_on) {
			value = string_format("%d", WORK_QUEUE_SWITCH_ON);
			setenv("WORK_QUEUE_AUTO_REMOVE_WORKERS_ON", value, 1);
			free(value);
		}

		value = string_format("%d", work_queue_wait_routine);
		setenv("WORK_QUEUE_WAIT_ROUTINE", value, 1);
		free(value);

		if(port != 0) {
			value = string_format("%d", port);
			setenv("WORK_QUEUE_PORT", value, 1);
			free(value);
		} else {
			// Use work queue default port in standalone mode when port is not
			// specified with -p option. In work queue catalog mode, work queue
			// would choose a random port when port is not explicitly specified.
			if(work_queue_master_mode == WORK_QUEUE_MASTER_MODE_STANDALONE) {
				value = string_format("%d", WORK_QUEUE_DEFAULT_PORT);
				setenv("WORK_QUEUE_PORT", value, 1);
				free(value);
			}
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

	struct dag *d = dag_create();
	if(!d || !dag_parse(d, dagfile, clean_mode || syntax_check || display_mode)) {
		fprintf(stderr, "makeflow: couldn't parse %s\n", dagfile);
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

	char *s = getenv("MAKEFLOW_MAX_REMOTE_JOBS");
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

	if(display_mode) {
		free(logfilename);
		free(batchlogfilename);
		dag_print(d);
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
			fprintf(stderr, "makeflow: Or, use the work queue with -T wq and condor_submit_workers.\n");

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

	if(batch_submit_options) {
		debug(D_DEBUG, "setting batch options to %s\n", batch_submit_options);
		batch_queue_set_options(remote_queue, batch_submit_options);
	}

	if(batchlogfilename) {
		batch_queue_set_logfile(remote_queue, batchlogfilename);
	}

	port = batch_queue_port(remote_queue);
	if(port > 0)
		debug(D_DEBUG, "listening on port %d.\n", port);

	if(auto_workers > 0) {
		if(!handle_auto_workers(d, auto_workers)) {
			exit(1);
		}
	}

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
