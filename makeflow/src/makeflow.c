/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth_all.h"
#include "auth_ticket.h"
#include "batch_job.h"
#include "catalog_query.h"
#include "cctools.h"
#include "copy_stream.h"
#include "debug.h"
#include "disk_info.h"
#include "getopt_aux.h"
#include "get_line.h"
#include "hash_table.h"
#include "int_sizes.h"
#include "itable.h"
#include "link.h"
#include "list.h"
#include "load_average.h"
#include "macros.h"
#include "path.h"
#include "random_init.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "work_queue.h"
#include "work_queue_catalog.h"
#include "xxmalloc.h"

#include "dag.h"
#include "visitors.h"
#include "makeflow_common.h"

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/* Notes:
 *
 * o Makeflow now uses batch_fs_* functions to access DAG files. This is mostly
 *   important for Chirp where the files are all remote.
 * o APIs like work_queue_* should be indirectly accessed by setting options
 *   in Batch Job using batch_queue_set_option. See batch_job_work_queue.c for
 *   an example.
 */

#define RANDOM_PORT_RETRY_TIME 300

#define MAKEFLOW_AUTO_WIDTH 1
#define MAKEFLOW_AUTO_GROUP 2

#define	MAKEFLOW_MIN_SPACE 10*1024*1024	/* 10 MB */

#define MONITOR_ENV_VAR "CCTOOLS_RESOURCE_MONITOR"

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%06.6d"
#define DEFAULT_MONITOR_INTERVAL   1

/* Unique integers for long options. */

enum {
	LONG_OPT_MONITOR_INTERVAL = 1,
	LONG_OPT_MONITOR_LOG_NAME,
	LONG_OPT_MONITOR_LIMITS,
	LONG_OPT_MONITOR_TIME_SERIES,
	LONG_OPT_MONITOR_OPENED_FILES,
	LONG_OPT_DISABLE_BATCH_CACHE,
	LONG_OPT_PASSWORD,
	LONG_OPT_VERBOSE_PARSING,
	LONG_OPT_WQ_WAIT_FOR_WORKERS,
	LONG_OPT_DOT_CONDENSE,
	LONG_OPT_AUTH,
	LONG_OPT_TICKETS,
	LONG_OPT_WORKING_DIR,
};

typedef enum {
	DAG_GC_NONE,
	DAG_GC_REF_COUNT,
	DAG_GC_ON_DEMAND,
} dag_gc_method_t;

static sig_atomic_t dag_abort_flag = 0;
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
static int port = 0;
static int output_len_check = 0;

static int cache_mode = 1;

static char *monitor_exe  = "resource_monitor_cctools";

static int monitor_mode = 0;
static int monitor_enable_time_series = 0;
static int monitor_enable_list_files  = 0;

static char *monitor_limits_name = NULL;
static int monitor_interval = 1;	// in seconds
static char *monitor_log_format = NULL;
static char *monitor_log_dir = NULL;

static char *wq_password = 0;
int wq_wait_queue_size = 0;

int verbose_parsing = 0;

void dag_export_variables(struct dag *d, struct dag_node *n);

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

	if(batch_fs_unlink(remote_queue, filename) == 0) {
		if(!silent)
			printf("deleted path %s\n", filename);
	} else if(errno == ENOENT) {
		// say nothing
	} else if(!silent) {
		fprintf(stderr, "couldn't delete %s: %s\n", filename, strerror(errno));
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
	if(n->state == DAG_NODE_STATE_RUNNING && !(n->local_job && local_queue) && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
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
		if(batch_fs_stat(remote_queue, f->filename, &filestat) >= 0) {
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
		if(batch_fs_stat(remote_queue, f->filename, &filestat) < 0) {
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
		if(n->local_job && local_queue) {
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

int copy_monitor(void)
{
	char *monitor_orig = resource_monitor_locate(NULL);

	if(!monitor_orig)
	{
		fatal("Could not locate resource_monitor executable");
	}

	struct stat original;
	struct stat current;

	if(stat(monitor_orig, &original))
	{
		fatal("Could not stat resource_monitor executable");
	}

	if(batch_fs_stat(remote_queue, monitor_exe, &current) == -1 || difftime(original.st_mtime, current.st_mtime) > 0)
	{
		if(batch_fs_putfile(remote_queue, monitor_orig, monitor_exe) < original.st_size)
		{
			fatal("Could not copy resource_monitor executable");
		}
	}

	free(monitor_orig);

	return 1;
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
			if(n->nested_job && ((n->local_job && local_queue) || batch_queue_type == BATCH_QUEUE_TYPE_LOCAL)) {
				char *command = xxmalloc(strlen(n->command) + 20);
				sprintf(command, "%s -j %d", n->command, d->local_jobs_max / dag_nested_width);
				free((char *) n->command);
				n->command = command;
			}
		}
	}
}

char *monitor_log_name(char *dirname, int nodeid)
{
	char *name = string_format(monitor_log_format, nodeid);
	char *path = string_format("%s/%s", dirname, name);
	free(name);

	return path;
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
		char *log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
		char *log_name;

		dag_node_add_source_file(n, monitor_exe, NULL);

		log_name = string_format("%s.summary", log_name_prefix);
		dag_node_add_target_file(n, log_name, NULL);
		free(log_name);

		if(monitor_enable_time_series)
		{
			log_name = string_format("%s.series", log_name_prefix);
			dag_node_add_target_file(n, log_name, NULL);
			free(log_name);
		}

		if(monitor_enable_list_files)
		{
			log_name = string_format("%s.files", log_name_prefix);
			dag_node_add_target_file(n, log_name, NULL);
			free(log_name);
		}

		free(log_name_prefix);
	}

	return 1;
}

void dag_export_variables(struct dag *d, struct dag_node *n)
{
	struct dag_lookup_set s = { d, n->category, n, NULL };
	char *key;

	set_first_element(d->export_vars);
	while((key = set_next_element(d->export_vars))) {
		char *value = dag_lookup_str(key, &s);
		if(value) {
			setenv(key, value, 1);
			debug(D_DEBUG, "export %s=%s", key, value);
		}
	}
}

const char *dag_node_rmonitor_wrap_command(struct dag_node *n)
{
	char *log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
	char *limits_str = dag_task_resources_wrap_as_rmonitor_options(n);
	char *extra_options = string_format("%s -V '%-15s%s'",
			limits_str ? limits_str : "",
			"category:",
			n->category->label);

	log_name_prefix     = monitor_log_name(monitor_log_dir, n->nodeid);

	if(n->monitor_command)
		free(n->monitor_command);

	n->monitor_command  = resource_monitor_rewrite_command((char *) n->command,
			monitor_exe,
			log_name_prefix,
			monitor_limits_name,
			extra_options,
			1,                           /* summaries always enabled */
			monitor_enable_time_series,
			monitor_enable_list_files);

	free(log_name_prefix);
	free(extra_options);
	free(limits_str);

	return n->monitor_command;
}


void dag_node_submit(struct dag *d, struct dag_node *n)
{
	char *input_files  = NULL;
	char *output_files = NULL;
	struct dag_file *f;
	const char *remotename;
	char current_dir[PATH_MAX];
	char abs_name[PATH_MAX];
	struct batch_queue *thequeue;
	int len = 0, len_temp;
	char *tmp;

	if(n->local_job && local_queue) {
		thequeue = local_queue;
	} else {
		thequeue = remote_queue;
	}

	batch_fs_getcwd(remote_queue, current_dir, PATH_MAX);

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
				path_collapse(tmp_name, abs_name, 1);
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
				path_collapse(tmp_name, abs_name, 1);
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
	char *batch_submit_options = dag_task_resources_wrap_options(n, batch_options_env, batch_queue_get_type(thequeue));
	char *old_batch_submit_options = NULL;

	free(batch_options_env);
	if(batch_submit_options) {
		debug(D_DEBUG, "Batch options: %s\n", batch_submit_options);
		if(batch_queue_get_option(thequeue, "batch-options"))
			old_batch_submit_options = xxstrdup(batch_queue_get_option(thequeue, "batch-options"));
		batch_queue_set_option(thequeue, "batch-options", batch_submit_options);
		free(batch_submit_options);
	}

	time_t stoptime = time(0) + dag_submit_timeout;
	int waittime = 1;

	/* Export variables before each submit. We have to do this before each
	 * node submission because each node may have local variables
	 * definitions. */
	dag_export_variables(d, n);

	while(1) {
		const char *command;

		if(monitor_mode)
			command = dag_node_rmonitor_wrap_command(n);
		else
			command = n->command;

		n->jobid = batch_job_submit_simple(thequeue, command, input_files, output_files);
		if(n->jobid >= 0)
			break;

		fprintf(stderr, "couldn't submit batch job, still trying...\n");

		if(dag_abort_flag)
			break;

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
		batch_queue_set_option(thequeue, "batch-options", old_batch_submit_options);
		free(old_batch_submit_options);
	}

	if(n->jobid >= 0) {
		dag_node_state_change(d, n, DAG_NODE_STATE_RUNNING);
		if(n->local_job && local_queue) {
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

	if(n->local_job && local_queue) {
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

	if(n->state != DAG_NODE_STATE_RUNNING)
		return;

	if(n->local_job && local_queue) {
		d->local_jobs_running--;
	} else {
		d->remote_jobs_running--;
	}

	if(info->exited_normally && info->exit_code == 0) {
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			struct stat buf;
			if(batch_fs_stat(remote_queue, f->filename, &buf) < 0) {
				fprintf(stderr, "%s did not create file %s\n", n->command, f->filename);
				job_failed = 1;
			} else {
				if(output_len_check && buf.st_size <= 0) {
					debug(D_DEBUG, "%s created a file of length %ld\n", n->command, (long) buf.st_size);
					job_failed = 1;
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
			struct stat buf;

			if(hash_table_lookup(d->completed_files, f->filename)) {
				continue;
			}

			if(batch_fs_stat(remote_queue, f->filename, &buf) >= 0) {
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
	struct stat buf;
	if(batch_fs_stat(remote_queue, f->filename, &buf) == 0 && batch_fs_unlink(remote_queue, f->filename) == -1) {
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
		batch_fs_getcwd(remote_queue, cwd, PATH_MAX);
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
				debug(D_DEBUG, "Job %" PRIbjid " has returned.\n", jobid);
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
				debug(D_DEBUG, "Job %" PRIbjid " has returned.\n", jobid);
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

static void show_help_run(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, "Frequently used options:\n\n");
	fprintf(stdout, " %-30s Clean up: remove logfile and all targets.\n", "-c,--clean");
	fprintf(stdout, " %-30s Batch system type: (default is local)\n", "-T,--batch-type=<type>");
	fprintf(stdout, " %-30s %s\n\n", "", batch_queue_type_string());
	fprintf(stdout, "Other options are:\n");
	fprintf(stdout, " %-30s Advertise the master information to a catalog server.\n", "-a,--advertise");
	fprintf(stdout, " %-30s Disable the check for AFS. (experts only.)\n", "-A,--disable-afs-check");
	fprintf(stdout, " %-30s Add these options to all batch submit files.\n", "-B,--batch-options=<options>");
	fprintf(stdout, " %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT \n", "-C,--catalog-server=<catalog>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem\n", "-d,--debug=<subsystem>");
	fprintf(stdout, " %-30s Write summary of workflow to this file upon success or failure.\n", "-f,--summary-log=<file>");
	fprintf(stdout, " %-30s Work Queue fast abort multiplier.           (default is deactivated)\n", "-F,--wq-fast-abort=<#>");
	fprintf(stdout, " %-30s Show this help screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Max number of local jobs to run at once.    (default is # of cores)\n", "-j,--max-local=<#>");
	fprintf(stdout, " %-30s Max number of remote jobs to run at once.\n", "-J,--max-remote=<#>");
	fprintf(stdout, "                                                            (default %d for -Twq, %d otherwise.)\n", 10*MAX_REMOTE_JOBS_DEFAULT, MAX_REMOTE_JOBS_DEFAULT );
	fprintf(stdout, " %-30s Preserve (i.e., do not clean intermediate symbolic links)\n", "-K,--preserve-links");
	fprintf(stdout, " %-30s Use this file for the makeflow log.         (default is X.makeflowlog)\n", "-l,--makeflow-log=<logfile>");
	fprintf(stdout, " %-30s Use this file for the batch system log.     (default is X.<type>log)\n", "-L,--batch-log=<logfile>");
	fprintf(stdout, " %-30s Send summary of workflow to this email address upon success or failure.\n", "-m,--email=<email>");
	fprintf(stdout, " %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	fprintf(stdout, " %-30s Send debugging to this file.\n", "-o,--debug-file=<file>");
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
	fprintf(stdout, " %-30s Disable Work Queue caching.                 (default is false)\n", "--disable-wq-cache");

	fprintf(stdout, "\n*Monitor Options:\n\n");
	fprintf(stdout, " %-30s Enable the resource monitor, and write the monitor logs to <dir>.\n", "-M,--monitor=<dir>");
	fprintf(stdout, " %-30s Use <file> as value-pairs for resource limits.\n", "--monitor-limits=<file>");
	fprintf(stdout, " %-30s Set monitor interval to <#> seconds.        (default is 1 second)\n", "--monitor-interval=<#>");
	fprintf(stdout, " %-30s Enable monitor time series.                 (default is disabled)\n", "--monitor-with-time-series");
	fprintf(stdout, " %-30s Enable monitoring of openened files.        (default is disabled)\n", "--monitor-with-opened-files");
	fprintf(stdout, " %-30s Format for monitor logs.                    (default %s)\n", "--monitor-log-fmt=<fmt>", DEFAULT_MONITOR_LOG_FORMAT);
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
			const char *size;
			struct stat buf;
			batch_fs_stat(remote_queue, fn, &buf);
			size = string_metric(buf.st_size, -1, NULL);
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
	typedef enum {MAKEFLOW_RUN, MAKEFLOW_VIZ, MAKEFLOW_ANALYZE} runtime_mode;

	int c;
	random_init();
	set_makeflow_exe(argv[0]);
	debug_config(get_makeflow_exe());

	cctools_version_debug(D_DEBUG, get_makeflow_exe());
	const char *dagfile;

	char *batchlogfilename = NULL;
	const char *batch_submit_options = getenv("BATCH_OPTIONS");
	char *catalog_host;
	int catalog_port;
	int clean_mode = 0;
	char *email_summary_to = NULL;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	char *logfilename = NULL;
	int port_set = 0;
	int preserve_symlinks = 0;
	timestamp_t runtime = 0;
	int skip_afs_check = 0;
	timestamp_t time_completed = 0;
	int work_queue_estimate_capacity_on = 1; // capacity estimation is on by default
	int work_queue_keepalive_interval = WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL;
	int work_queue_keepalive_timeout = WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT;
	int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	char *work_queue_password = NULL;
	char *wq_wait_queue_size = 0;
	int did_explicit_auth = 0;
	char *chirp_tickets = NULL;
	char *working_dir = NULL;
	char *write_summary_to = NULL;
	char *s;

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
		master_mode = s;
	}
	s = getenv("WORK_QUEUE_NAME");
	if(s) {
		project = xxstrdup(s);
	}
	s = getenv("WORK_QUEUE_FAST_ABORT_MULTIPLIER");
	if(s) {
		wq_option_fast_abort_multiplier = atof(s);
	}

	static struct option long_options_run[] = {
		{"auth", required_argument, 0, LONG_OPT_AUTH},
		{"tickets", required_argument, 0, LONG_OPT_TICKETS},
		{"debug", required_argument, 0, 'd'},
		{"help", no_argument, 0, 'h'},
		{"clean", no_argument, 0, 'c'},
		{"batch-type", required_argument, 0, 'T'},
		{"advertise", no_argument, 0, 'a'},
		{"disable-afs-check", no_argument, 0, 'A'},
		{"batch-options", required_argument, 0, 'B'},
		{"clean", no_argument, 0, 'c'},
		{"catalog-server", required_argument, 0, 'C'},
		{"disable-cache", no_argument, 0, LONG_OPT_DISABLE_BATCH_CACHE},
		{"wq-estimate-capacity", no_argument, 0, 'E'},
		{"summary-log", no_argument, 0, 'f'},
		{"wq-fast-abort", required_argument, 0, 'F'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
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
		{"project-name", required_argument, 0, 'N'},
		{"debug-output", required_argument, 0, 'o'},
		{"show-output", no_argument, 0, 'O'},
		{"port", required_argument, 0, 'p'},
		{"priority", required_argument, 0, 'P'},
		{"retry-count", required_argument, 0, 'r'},
		{"retry", no_argument, 0, 'R'},
		{"submission-timeout", required_argument, 0, 'S'},
		{"wq-keepalive-timeout", required_argument, 0, 't'},
		{"batch-type", required_argument, 0, 'T'},
		{"wq-keepalive-interval", required_argument, 0, 'u'},
		{"wq-wait-queue-size", required_argument, 0, LONG_OPT_WQ_WAIT_FOR_WORKERS},
		{"version", no_argument, 0, 'v'},
		{"wq-schedule", required_argument, 0, 'W'},
		{"zero-length-error", no_argument, 0, 'z'},
		{"port-file", required_argument, 0, 'Z'},
		{"password", required_argument, 0, LONG_OPT_PASSWORD},
		{"working-dir", required_argument, 0, LONG_OPT_WORKING_DIR},
		{0, 0, 0, 0}
	};

	const char *option_string_run = "aAB:cC:d:EfF:g:G:hj:J:Kl:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:zZ:";
	       //                         "aAB:cC:d:D:eEf:F:g:G:hiIj:J:kKl:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:zZ:"
	while((c = getopt_long(argc, argv, option_string_run, long_options_run, NULL)) >= 0) {
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
			case 'E':
				// This option is deprecated. Capacity estimation is now on by default.
				break;
			case LONG_OPT_AUTH:
				auth_register_byname(optarg);
				did_explicit_auth = 1;
				break;
			case LONG_OPT_TICKETS:
				tickets = strdup(optarg);
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
				show_help_run(get_makeflow_exe());
				return 0;
			case 'j':
				explicit_local_jobs_max = atoi(optarg);
				break;
			case 'J':
				explicit_remote_jobs_max = atoi(optarg);
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
			case 'M':
				monitor_mode = 1;
				if(monitor_log_dir)
					free(monitor_log_dir);
				monitor_log_dir = xxstrdup(optarg);
				break;
			case LONG_OPT_MONITOR_LIMITS:
				monitor_mode = 1;
				if(monitor_limits_name)
					free(monitor_limits_name);
				monitor_limits_name = xxstrdup(optarg);
				break;
			case LONG_OPT_MONITOR_INTERVAL:
				monitor_mode = 1;
				monitor_interval = atoi(optarg);
				break;
			case LONG_OPT_MONITOR_TIME_SERIES:
				monitor_mode = 1;
				monitor_enable_time_series = 1;
				break;
			case LONG_OPT_MONITOR_OPENED_FILES:
				monitor_mode = 1;
				monitor_enable_list_files = 1;
				break;
			case LONG_OPT_MONITOR_LOG_NAME:
				monitor_mode = 1;
				if(monitor_log_format)
					free(monitor_log_format);
				monitor_log_format = xxstrdup(optarg);
				break;
			case 'N':
				free(project);
				project = xxstrdup(optarg);
				work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
				break;
			case 'o':
				debug_config_file(optarg);
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
				cctools_version_print(stdout, get_makeflow_exe());
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
			case LONG_OPT_PASSWORD:
				if(copy_file_to_buffer(optarg, &wq_password) < 0) {
					fprintf(stderr, "makeflow: couldn't open %s: %s\n", optarg, strerror(errno));
					return 1;
				}
				break;
			case LONG_OPT_DISABLE_BATCH_CACHE:
				cache_mode = 0;
				break;
			case LONG_OPT_WQ_WAIT_FOR_WORKERS:
				wq_wait_queue_size = atoi(optarg);
				break;
			default:
				show_help_run(get_makeflow_exe());
				return 1;
		}
	}

	if(!did_explicit_auth)
		auth_register_all();
	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else {
		auth_ticket_load(NULL);
	}

	if((argc - optind) != 1) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", get_makeflow_exe());
			return 1;
		}

		dagfile = "./Makeflow";
	} else {
		dagfile = argv[optind];
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE || batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE_SHAREDFS) {
		if(strcmp(master_mode, "catalog") == 0 && project == NULL) {
			fprintf(stderr, "makeflow: Makeflow running in catalog mode. Please use '-N' option to specify the name of this project.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", get_makeflow_exe());
			return 1;
		}
		// Use Work Queue default port in standalone mode when port is not
		// specified with -p option. In Work Queue catalog mode, Work Queue
		// would choose an arbitrary port when port is not explicitly specified.
		if(!port_set && strcmp(master_mode, "standalone") == 0) {
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
			unlink(cleanlog);
			free(cleanlog);
			cleanlog = string_format("%s.wqlog", dagfile);
			unlink(cleanlog);
			free(cleanlog);
			cleanlog = string_format("%s.batchlog", dagfile);
			unlink(cleanlog);
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
		if(logfilename) free(logfilename);
		if(batchlogfilename) free(batchlogfilename);
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
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

	if(monitor_mode && !dag_prepare_for_monitoring(d)) {
		fatal("Could not prepare for monitoring.\n");
	}

	remote_queue = batch_queue_create(batch_queue_type);
	if(!remote_queue) {
		fprintf(stderr, "makeflow: couldn't create batch queue.\n");
		if(port != 0)
			fprintf(stderr, "makeflow: perhaps port %d is already in use?\n", port);
		exit(1);
	}

	batch_queue_set_logfile(remote_queue, batchlogfilename);
	batch_queue_set_option(remote_queue, "batch-options", batch_submit_options);
	batch_queue_set_option(remote_queue, "skip-afs-check", skip_afs_check ? "yes" : "no");
	batch_queue_set_option(remote_queue, "password", password);
	batch_queue_set_option(remote_queue, "master-mode", master_mode);
	batch_queue_set_option(remote_queue, "name", project);
	batch_queue_set_option(remote_queue, "priority", priority);
	batch_queue_set_option(remote_queue, "estimate-capacity", "yes"); // capacity estimation is on by default
	batch_queue_set_option(remote_queue, "keepalive-interval", keepalive_interval);
	batch_queue_set_option(remote_queue, "keepalive-timeout", keepalive_timeout);
	batch_queue_set_option(remote_queue, "caching", cache_mode ? "yes" : "no");
	batch_queue_set_option(remote_queue, "wait-queue-size", wq_wait_queue_size);
	batch_queue_set_option(remote_queue, "working-dir", working_dir);

	if(batch_queue_type == BATCH_QUEUE_TYPE_CHIRP || batch_queue_type == BATCH_QUEUE_TYPE_HADOOP) {
		local_queue = 0; /* all local jobs must be run on Chirp */
		if(dag_gc_method == DAG_GC_ON_DEMAND /* NYI */ ) {
			dag_gc_method = DAG_GC_REF_COUNT;
		}
	} else {
		local_queue = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL);
		if(!local_queue) {
			fprintf(stderr, "makeflow: couldn't create local job queue.\n");
			exit(1);
		}
	}

	if(!dag_prepare_for_batch_system(d)) {
		fatal("Could not prepare for submission to batch system.\n");
	}

	if(dag_gc_method != DAG_GC_NONE)
		dag_prepare_gc(d);

	dag_prepare_nested_jobs(d);

	if(clean_mode) {
		dag_clean(d);
		unlink(logfilename);
		unlink(batchlogfilename);
		free(logfilename);
		free(batchlogfilename);
		return 0;
	}

	if(!dag_check(d)) {
		free(logfilename);
		free(batchlogfilename);
		return 1;
	}

	fprintf(stdout, "\r     Total rules: %d", d->nodeid_counter);
	fprintf(stdout, "\nStarting execution of workflow: %s.\n", dagfile);

	setlinebuf(stdout);
	setlinebuf(stderr);

	dag_log_recover(d, logfilename);

	port = batch_queue_port(remote_queue);
	if(port_file)
		opts_write_port_file(port_file, port);
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

	if(local_queue)
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

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
