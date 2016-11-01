/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth_all.h"
#include "auth_ticket.h"
#include "batch_job.h"
#include "cctools.h"
#include "copy_stream.h"
#include "debug.h"
#include "getopt_aux.h"
#include "hash_table.h"
#include "int_sizes.h"
#include "itable.h"
#include "link.h"
#include "list.h"
#include "load_average.h"
#include "macros.h"
#include "path.h"
#include "random.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "work_queue.h"
#include "work_queue_catalog.h"
#include "xxmalloc.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include "dag.h"
#include "dag_visitors.h"
#include "makeflow_summary.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_docker.h"
#include "makeflow_wrapper_monitor.h"
#include "parser.h"
#include "parser_jx.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
Code organization notes:

- The modules dag/dag_node/dag_file etc contain the data structures that
represent the dag structure by itself.  Functions named dag_*() create
and manipulate those data structures, but do not execute the dag itself.
These are shared between makeflow and other tools that read and manipulate
the dag, like makeflow_viz, makeflow_linker, and so forth.

- The modules makeflow/makeflow_log/makeflow_gc etc contain the functions
that execute the dag by invoking batch operations, processing the log, etc.
These are all functions named makeflow_*() to distinguish them from dag_*().

- The separation between dag structure and execution state is imperfect,
because some of the execution state (note states, node counts, etc)
is stored in struct dag and struct dag_node.  Perhaps this can be improved.

- All operations on files should use the batch_fs_*() functions, rather
than invoking Unix I/O directly.  This is because some batch systems
(Hadoop, Confuga, etc) also include the storage where the files to be
accessed are located.

- APIs like work_queue_* should be indirectly accessed by setting options
in Batch Job using batch_queue_set_option. See batch_job_work_queue.c for
an example.
*/

#define MAX_REMOTE_JOBS_DEFAULT 100
#define MAX_BUF_SIZE 4096
#define MESOS_DONE_FILE "mesos_done" 

static sig_atomic_t makeflow_abort_flag = 0;
static int makeflow_failed_flag = 0;
static int makeflow_submit_timeout = 3600;
static int makeflow_retry_flag = 0;
static int makeflow_retry_max = MAX_REMOTE_JOBS_DEFAULT;

/* makeflow_gc_method indicates the type of garbage collection
 * indicated by the user. Refer to makeflow_gc.h for specifics */
static makeflow_gc_method_t makeflow_gc_method = MAKEFLOW_GC_NONE;
/* Disk size at which point GC is run */
static uint64_t makeflow_gc_size   = 0;
/* # of files after which GC is run */
static int makeflow_gc_count  = -1;
/* Iterations of wait loop prior ot GC check */
static int makeflow_gc_barrier = 1;
/* Determines next gc_barrier to make checks less frequent with
 * large number of tasks */
static double makeflow_gc_task_ratio = 0.05;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static int local_jobs_max = 1;
static int remote_jobs_max = 100;

static char *project = NULL;
static int port = 0;
static int output_len_check = 0;
static int skip_file_check = 0;

static int cache_mode = 1;

static int json_input = 0;

static container_mode_t container_mode = CONTAINER_MODE_NONE;
static char *container_image = NULL;
static char *image_tar = NULL;

/* wait upto this many seconds for an output file of a succesfull task
 * to appear on the local filesystem (e.g, to deal with NFS
 * semantics. . */
static int file_creation_patience_wait_time = 0;

/* Write a verbose transaction log with SYMBOL tags.
 * SYMBOLs are category labels (SYMBOLs should be deprecated
 * once weaver/pbui tools are updated.) */
static int log_verbose_mode = 0;

/* Wrapper control functions for wrapping command and adding input/output
 *  * files. */
static struct makeflow_wrapper *wrapper = 0;
static struct makeflow_monitor *monitor = 0;

/* Generates file list for node based on node files, wrapper
 *  * input files, and monitor input files. Relies on %% nodeid
 *   * replacement for monitor file names. */
static struct list *makeflow_generate_input_files( struct dag_node *n, struct makeflow_wrapper *w, struct makeflow_monitor *m )
{
	struct list *result = list_duplicate(n->source_files);

	if(w){
		result = makeflow_wrapper_generate_files(result, w->input_files, n, w);
	}

	if(m){
		result = makeflow_wrapper_generate_files(result, m->wrapper->input_files, n, m->wrapper);
	}

	return result;
}

static struct list *makeflow_generate_output_files( struct dag_node *n, struct makeflow_wrapper *w, struct makeflow_monitor *m )
{
	struct list *result = list_duplicate(n->target_files);

	if(w){
		result = makeflow_wrapper_generate_files(result, w->output_files, n, w);
	}

	if(m){
		result = makeflow_wrapper_generate_files(result, m->wrapper->output_files, n, m->wrapper);
	}

	return result;
}

/*
Abort the dag by removing all batch jobs from all queues.
*/

static void makeflow_abort_all(struct dag *d)
{
	UINT64_T jobid;
	struct dag_node *n;
	struct dag_file *f;

	printf("got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table, &jobid, (void **) &n)) {
		printf("aborting local job %" PRIu64 "\n", jobid);
		batch_job_remove(local_queue, jobid);
		makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);
		struct list *outputs = makeflow_generate_output_files(n, wrapper, monitor);
		list_first_item(outputs);
		while((f = list_next_item(outputs)))
			makeflow_clean_file(d, local_queue, f, 0);
		makeflow_clean_node(d, local_queue, n, 1);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		printf("aborting remote job %" PRIu64 "\n", jobid);
		batch_job_remove(remote_queue, jobid);
		makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);
		struct list *outputs = makeflow_generate_output_files(n, wrapper, monitor);
		list_first_item(outputs);
		while((f = list_next_item(outputs)))
			makeflow_clean_file(d, remote_queue, f, 0);
		makeflow_clean_node(d, remote_queue, n, 1);
	}
}
static void makeflow_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n);

/*
Decide whether to rerun a node based on batch and file system status. The silent
option was added for to prevent confusing debug output when in clean mode. When
clean_mode is not NONE we silence the node reseting output.
*/

void makeflow_node_decide_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n, int silent)
{
	struct dag_file *f;

	if(itable_lookup(rerun_table, n->nodeid))
		return;

	// Below are a bunch of situations when a node has to be rerun.

	// If a job was submitted to Condor, then just reconnect to it.
	if(n->state == DAG_NODE_STATE_RUNNING && !(n->local_job && local_queue) && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		// Reconnect the Condor jobs
		if(!silent) fprintf(stderr, "rule still running: %s\n", n->command);
		itable_insert(d->remote_job_table, n->jobid, n);

		// Otherwise, we cannot reconnect to the job, so rerun it
	} else if(n->state == DAG_NODE_STATE_RUNNING || n->state == DAG_NODE_STATE_FAILED || n->state == DAG_NODE_STATE_ABORTED) {
		if(!silent) fprintf(stderr, "will retry failed rule: %s\n", n->command);
		goto rerun;
	}
	// Rerun if an input file has been updated since the last execution.
	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
		if(dag_file_should_exist(f)) {
			continue;
		} else {
			if(!f->created_by) {
				if(!silent) fprintf(stderr, "makeflow: input file %s does not exist and is not created by any rule.\n", f->filename);
				exit(1);
			} else {
				/* If input file is missing, but node completed and file was garbage, then avoid rerunning. */
				if(n->state == DAG_NODE_STATE_COMPLETE && f->state == DAG_FILE_STATE_DELETE) {
					continue;
				}
				goto rerun;
			}
		}
	}

	// Rerun if an output file is missing.
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		if(dag_file_should_exist(f))
			continue;
		/* If output file is missing, but node completed and file was gc'ed, then avoid rerunning. */
		if(n->state == DAG_NODE_STATE_COMPLETE && f->state == DAG_FILE_STATE_DELETE)
			continue;
		goto rerun;
	}

	// Do not rerun this node
	return;

	  rerun:
	makeflow_node_force_rerun(rerun_table, d, n);
}

/*
Reset all state to cause a node to be re-run.
*/

void makeflow_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n)
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
			itable_remove(d->local_job_table, n->jobid);
		} else {
			batch_job_remove(remote_queue, n->jobid);
			itable_remove(d->remote_job_table, n->jobid);
		}
	}
	// Clean up things associated with this node
	struct list *outputs = makeflow_generate_output_files(n, wrapper, monitor);
	list_first_item(outputs);
	while((f1 = list_next_item(outputs)))
		makeflow_clean_file(d, remote_queue, f1, 0);
	makeflow_clean_node(d, remote_queue, n, 0);
	makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);

	// For each parent node, rerun it if input file was garbage collected
	list_first_item(n->source_files);
	while((f1 = list_next_item(n->source_files))) {
		if(dag_file_should_exist(f1))
			continue;

		p = f1->created_by;
		if(p) {
			makeflow_node_force_rerun(rerun_table, d, p);
			f1->reference_count += 1;
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
				makeflow_node_force_rerun(rerun_table, d, p);
			}
		}
	}
}

static void makeflow_prepare_nested_jobs(struct dag *d)
{
	/* Update nested jobs with appropriate number of local jobs (total
	 * local jobs max / maximum number of concurrent nests). */
	int dag_nested_width = dag_width(d, 1);
	int update_dag_nests = 1;
	char *s = getenv("MAKEFLOW_UPDATE_NESTED_JOBS");
	if(s)
		update_dag_nests = atoi(s);

	if(dag_nested_width > 0 && update_dag_nests) {
		dag_nested_width = MIN(dag_nested_width, local_jobs_max);
		struct dag_node *n;
		for(n = d->nodes; n; n = n->next) {
			if(n->nested_job && ((n->local_job && local_queue) || batch_queue_type == BATCH_QUEUE_TYPE_LOCAL)) {
				char *command = xxmalloc(strlen(n->command) + 20);
				sprintf(command, "%s -j %d", n->command, local_jobs_max / dag_nested_width);
				free((char *) n->command);
				n->command = command;
			}
		}
	}
}

/*
Given a file, return the string that identifies it appropriately
for the given batch system, combining the local and remote name
and making substitutions according to the node.
*/

static char * makeflow_file_format( struct dag_node *n, struct dag_file *f, struct batch_queue *queue, struct makeflow_wrapper *w, struct makeflow_monitor *m)
{
	const char *remotename = dag_node_get_remote_name(n, f->filename);
	if(!remotename && w) remotename = makeflow_wrapper_get_remote_name(w, n->d, f->filename);
	if(!remotename && m) remotename = makeflow_wrapper_get_remote_name(m->wrapper, n->d, f->filename);
	if(!remotename) remotename = f->filename;

	switch (batch_queue_get_type(queue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			return string_format("%s=%s,", f->filename, remotename);
		default:
			return string_format("%s,", f->filename);
	}
}

/*
Given a list of files, set theses files' states to EXPECT.
*/

void makeflow_log_file_expectation( struct dag *d, struct list *file_list )
{
	struct dag_file *f;

	if(!file_list) return ;

	list_first_item(file_list);
	while((f=list_next_item(file_list))) {
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXPECT);
	}
}


/*
Given a list of files, add the files to the given string.
Returns the original string, realloced if necessary
*/

static char * makeflow_file_list_format( struct dag_node *node, char *file_str, struct list *file_list, struct batch_queue *queue, struct makeflow_wrapper *w, struct makeflow_monitor *m )
{
	struct dag_file *file;

	if(!file_str) file_str = strdup("");

	if(!file_list) return file_str;

	list_first_item(file_list);
	while((file=list_next_item(file_list))) {
		char *f = makeflow_file_format(node,file,queue,w,m);
		file_str = string_combine(file_str,f);
		free(f);
	}

	return file_str;
}

/*
Submit one fully formed job, retrying failures up to the makeflow_submit_timeout.
This is necessary because busy batch systems occasionally do not accept a job submission.
*/

static batch_job_id_t makeflow_node_submit_retry( struct batch_queue *queue, const char *command, const char *input_files, const char *output_files, struct jx *envlist, const struct rmsummary *resources)
{
	time_t stoptime = time(0) + makeflow_submit_timeout;
	int waittime = 1;
	batch_job_id_t jobid = 0;

	/* Display the fully elaborated command, just like Make does. */
	printf("submitting job: %s\n", command);

	while(1) {
		jobid = batch_job_submit(queue, command, input_files, output_files, envlist, resources);
		if(jobid >= 0) {
			printf("submitted job %"PRIbjid"\n", jobid);
			return jobid;
		}

		fprintf(stderr, "couldn't submit batch job, still trying...\n");

		if(makeflow_abort_flag) break;

		if(time(0) > stoptime) {
			fprintf(stderr, "unable to submit job after %d seconds!\n", makeflow_submit_timeout);
			break;
		}

		sleep(waittime);
		waittime *= 2;
		if(waittime > 60) waittime = 60;
	}

	return 0;
}

/*
Submit a node to the appropriate batch system, after materializing
the necessary list of input and output files, and applying all
wrappers and options.
*/

static void makeflow_node_submit(struct dag *d, struct dag_node *n)
{
	struct batch_queue *queue;

	if(n->local_job && local_queue) {
		queue = local_queue;
	} else {
		queue = remote_queue;
	}

	struct list *input_list  = makeflow_generate_input_files(n, wrapper, monitor);
	struct list *output_list = makeflow_generate_output_files(n, wrapper, monitor);

	/* Create strings for all the files mentioned by this node. */
	char *input_files  = makeflow_file_list_format(n, 0, input_list,  queue, wrapper, monitor);
	char *output_files = makeflow_file_list_format(n, 0, output_list, queue, wrapper, monitor);

	/* Apply the wrapper(s) to the command, if it is (they are) enabled. */
	char *command = strdup(n->command);
	command = makeflow_wrap_wrapper(command, n, wrapper);
	command = makeflow_wrap_monitor(command, n, queue, monitor);

	/* Before setting the batch job options (stored in the "BATCH_OPTIONS"
	 * variable), we must save the previous global queue value, and then
	 * restore it after we submit. */
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *batch_options          = dag_variable_lookup_string("BATCH_OPTIONS", &s);

	char *previous_batch_options = NULL;
	if(batch_queue_get_option(queue, "batch-options"))
		previous_batch_options = xxstrdup(batch_queue_get_option(queue, "batch-options"));

	if(batch_options) {
		debug(D_MAKEFLOW_RUN, "Batch options: %s\n", batch_options);
		batch_queue_set_option(queue, "batch-options", batch_options);
		free(batch_options);
	}

	/* Generate the environment vars specific to this node. */
	struct jx *envlist = dag_node_env_create(d,n);

	/* Logs the creation of output files. */
	makeflow_log_file_expectation(d, output_list);

	/* Now submit the actual job, retrying failures as needed. */
	n->jobid = makeflow_node_submit_retry(queue,command,input_files,output_files,envlist, dag_node_dynamic_label(n));

	/* Restore old batch job options. */
	if(previous_batch_options) {
		batch_queue_set_option(queue, "batch-options", previous_batch_options);
		free(previous_batch_options);
	}

	/* Update all of the necessary data structures. */
	if(n->jobid >= 0) {
		makeflow_log_state_change(d, n, DAG_NODE_STATE_RUNNING);
		if(n->local_job && local_queue) {
			itable_insert(d->local_job_table, n->jobid, n);
		} else {
			itable_insert(d->remote_job_table, n->jobid, n);
		}
	} else {
		makeflow_log_state_change(d, n, DAG_NODE_STATE_FAILED);
		makeflow_failed_flag = 1;
	}

	free(command);
	free(input_list);
	free(output_list);
	free(input_files);
	free(output_files);
	jx_delete(envlist);
}

static int makeflow_node_ready(struct dag *d, struct dag_node *n)
{
	struct dag_file *f;

	if(n->state != DAG_NODE_STATE_WAITING)
		return 0;

	if(n->local_job && local_queue) {
		if(dag_local_jobs_running(d) >= local_jobs_max)
			return 0;
	} else {
		if(dag_remote_jobs_running(d) >= remote_jobs_max)
			return 0;
	}

	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))) {
		if(dag_file_should_exist(f)) {
			continue;
		} else {
			return 0;
		}
	}

	return 1;
}

/*
Find all jobs ready to be run, then submit them.
*/

static void makeflow_dispatch_ready_jobs(struct dag *d)
{
	struct dag_node *n;

	for(n = d->nodes; n; n = n->next) {

		if(dag_remote_jobs_running(d) >= remote_jobs_max && dag_local_jobs_running(d) >= local_jobs_max)
			break;

		if(makeflow_node_ready(d, n)) {
			makeflow_node_submit(d, n);
		}
	}
}

/*
Check the the indicated file was created and log, error, or retry as appropriate.
*/

int makeflow_node_check_file_was_created(struct dag_node *n, struct dag_file *f)
{
	struct stat buf;
	int file_created = 0;

	int64_t start_check = time(0);

	while(!file_created) {
		if(batch_fs_stat(remote_queue, f->filename, &buf) < 0) {
			fprintf(stderr, "%s did not create file %s\n", n->command, f->filename);
		}
		else if(output_len_check && buf.st_size <= 0) {
			debug(D_MAKEFLOW_RUN, "%s created a file of length %ld\n", n->command, (long) buf.st_size);
		}
		else {
			/* File was created and has length larger than zero. */
			debug(D_MAKEFLOW_RUN, "File %s created by rule %d.\n", f->filename, n->nodeid);
			f->actual_size = buf.st_size;
			makeflow_log_file_state_change(n->d, f, DAG_FILE_STATE_EXISTS);
			file_created = 1;
			break;
		}

		if(file_creation_patience_wait_time > 0 && time(0) - start_check < file_creation_patience_wait_time) {
			/* Failed to see the file. Sleep and try again. */
			debug(D_MAKEFLOW_RUN, "Checking again for file %s.\n", f->filename);
			sleep(1);
		} else {
			/* Failed was not seen by makeflow in the aloted tries. */
			debug(D_MAKEFLOW_RUN, "File %s was not created by rule %d.\n", f->filename, n->nodeid);
			file_created = 0;
			break;
		}
	}

	return file_created;
}

/*
Mark the given task as completing, using the batch_job_info completion structure provided by batch_job.
*/

static void makeflow_node_complete(struct dag *d, struct dag_node *n, struct batch_queue *queue, struct batch_job_info *info)
{
	struct dag_file *f;
	int job_failed = 0;
	int monitor_retried = 0;

	if(n->state != DAG_NODE_STATE_RUNNING)
		return;

	if(monitor) {
		char *nodeid = string_format("%d",n->nodeid);
		char *output_prefix = NULL;
 		if(batch_queue_supports_feature(queue, "output_directories") || n->local_job) {
			output_prefix = xxstrdup(monitor->log_prefix);
		} else {
			output_prefix = xxstrdup(path_basename(monitor->log_prefix));
		}
		char *log_name_prefix = string_replace_percents(output_prefix, nodeid);
		char *summary_name = string_format("%s.summary", log_name_prefix);

		if(n->resources_measured)
			rmsummary_delete(n->resources_measured);
		n->resources_measured = rmsummary_parse_file_single(summary_name);

		category_accumulate_summary(n->category, n->resources_measured, NULL);

		makeflow_monitor_move_output_if_needed(n, queue, monitor);

		free(nodeid);
		free(log_name_prefix);
		free(summary_name);
	}

	struct list *outputs = makeflow_generate_output_files(n, wrapper, monitor);


	if(info->disk_allocation_exhausted) {
		job_failed = 1;
	}
	else if(info->exited_normally && info->exit_code == 0) {
		list_first_item(outputs);
		while((f = list_next_item(outputs))) {
			if(!makeflow_node_check_file_was_created(n, f))
			{
				job_failed = 1;
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
		makeflow_log_state_change(d, n, DAG_NODE_STATE_FAILED);

		/* Clean files created in node. Clean existing and expected and record deletion. */
		list_first_item(outputs);
		while((f = list_next_item(outputs))) {
			if(f->state == DAG_FILE_STATE_EXPECT) {
				makeflow_clean_file(d, remote_queue, f, 1);
			} else {
				makeflow_clean_file(d, remote_queue, f, 0);
			}
		}

		if(info->disk_allocation_exhausted) {
			fprintf(stderr, "\nrule %d failed because it exceeded its loop device allocation capacity.\n", n->nodeid);
			if(n->resources_measured)
			{
				rmsummary_print(stderr, n->resources_measured, /* pprint */ 0, /* extra fields */ NULL);
				fprintf(stderr, "\n");
			}

			category_allocation_t next = category_next_label(n->category, n->resource_request, /* resource overflow */ 1, n->resources_requested, n->resources_measured);

			if(next != CATEGORY_ALLOCATION_ERROR) {
				debug(D_MAKEFLOW_RUN, "Rule %d resubmitted using new resource allocation.\n", n->nodeid);
				n->resource_request = next;
				fprintf(stderr, "\nrule %d resubmitting with maximum resources.\n", n->nodeid);
				makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);
				if(monitor) { monitor_retried = 1; }
			}
		}

		if(monitor && info->exit_code == RM_OVERFLOW)
		{
			debug(D_MAKEFLOW_RUN, "rule %d failed because it exceeded the resources limits.\n", n->nodeid);
			if(n->resources_measured && n->resources_measured->limits_exceeded)
			{
				char *str = rmsummary_print_string(n->resources_measured->limits_exceeded, 1);
				debug(D_MAKEFLOW_RUN, "%s", str);
				free(str);
			}

			category_allocation_t next = category_next_label(n->category, n->resource_request, /* resource overflow */ 1, n->resources_requested, n->resources_measured);

			if(next != CATEGORY_ALLOCATION_ERROR) {
				debug(D_MAKEFLOW_RUN, "Rule %d resubmitted using new resource allocation.\n", n->nodeid);
				n->resource_request = next;
				makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);
				monitor_retried = 1;
			}
		}

		if(!monitor_retried) {
			if(makeflow_retry_flag || info->exit_code == 101) {
				n->failure_count++;
				if(n->failure_count > makeflow_retry_max) {
					notice(D_MAKEFLOW_RUN, "job %s failed too many times.", n->command);
					makeflow_failed_flag = 1;
				} else {
					notice(D_MAKEFLOW_RUN, "will retry failed job %s", n->command);
					makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);
				}
			}
			else
			{
				makeflow_failed_flag = 1;
			}
		}
		else
		{
			makeflow_failed_flag = 1;
		}
	} else {
		/* Mark source files that have been used by this node */
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			f->reference_count+= -1;
			if(f->reference_count == 0 && f->state == DAG_FILE_STATE_EXISTS)
				makeflow_log_file_state_change(d, f, DAG_FILE_STATE_COMPLETE);
		}

		makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
}

/*
Check the dag for consistency, and emit errors if input dependencies, etc are missing.
*/

static int makeflow_check(struct dag *d)
{
	struct stat buf;
	struct dag_node *n;
	struct dag_file *f;
	int error = 0;

	debug(D_MAKEFLOW_RUN, "checking rules for consistency...\n");

	for(n = d->nodes; n; n = n->next) {
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			if(f->created_by) {
				continue;
			}

			if(skip_file_check || batch_fs_stat(remote_queue, f->filename, &buf) >= 0) {
				continue;
			}
			fprintf(stderr, "makeflow: %s does not exist, and is not created by any rule.\n", f->filename);
			error++;
		}
	}

	if(error) {
		fprintf(stderr, "makeflow: found %d errors during consistency check.\n", error);
		return 0;
	} else {
		return 1;
	}
}

/*
Used to check that features used are supported by the batch system.
This would be where we added checking of selected options to verify they
are supported by the batch system, such as work_queue specific options.
*/

static int makeflow_check_batch_consistency(struct dag *d)
{
	struct dag_node *n;
	struct dag_file *f;
	int error = 0;

	debug(D_MAKEFLOW_RUN, "checking for consistency of batch system support...\n");

	for(n = d->nodes; n; n = n->next) {

		if(itable_size(n->remote_names) > 0 || (wrapper && wrapper->uses_remote_rename)){
			if(n->local_job) {
				debug(D_ERROR, "Remote renaming is not supported with -Tlocal or LOCAL execution. Rule %d.\n", n->nodeid);
				error = 1;
				break;
			} else if (!batch_queue_supports_feature(remote_queue, "remote_rename")) {
				debug(D_ERROR, "Remote renaming is not supported on selected batch system. Rule %d.\n", n->nodeid);
				error = 1;
				break;
			}
		}

		if(!batch_queue_supports_feature(remote_queue, "absolute_path") && !n->local_job){
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)) && !error) {
				const char *remotename = dag_node_get_remote_name(n, f->filename);
				if((remotename && *remotename == '/') || (*f->filename == '/' && !remotename)) {
					debug(D_ERROR, "Absolute paths are not supported on selected batch system. Rule %d.\n", n->nodeid);
					error = 1;
					break;
				}
			}
	
			list_first_item(n->target_files);
			while((f = list_next_item(n->target_files)) && !error) {
				const char *remotename = dag_node_get_remote_name(n, f->filename);
				if((remotename && *remotename == '/') || (*f->filename == '/' && !remotename)) {
					debug(D_ERROR, "Absolute paths are not supported on selected batch system. Rule %d.\n", n->nodeid);
					error = 1;
					break;
				}
			}
		}
	}

	if(error) {
		return 0;
	} else {
		return 1;
	}
}

/*
Main loop for running a makeflow: submit jobs, wait for completion, keep going until everything done.
*/

static void makeflow_run( struct dag *d )
{
	struct dag_node *n;
	batch_job_id_t jobid;
	struct batch_job_info info;


	while(!makeflow_abort_flag) {
		makeflow_dispatch_ready_jobs(d);

		if(dag_local_jobs_running(d)==0 && dag_remote_jobs_running(d)==0 )
			break;

		if(dag_remote_jobs_running(d)) {
			int tmp_timeout = 5;
			jobid = batch_job_wait_timeout(remote_queue, &info, time(0) + tmp_timeout);
			if(jobid > 0) {
				printf("job %"PRIbjid" completed\n",jobid);
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
				n = itable_remove(d->remote_job_table, jobid);
				if(n)
					makeflow_node_complete(d, n, remote_queue, &info);
			}
		}

		if(dag_local_jobs_running(d)) {
			time_t stoptime;
			int tmp_timeout = 5;

			if(dag_remote_jobs_running(d)) {
				stoptime = time(0);
			} else {
				stoptime = time(0) + tmp_timeout;
			}

			jobid = batch_job_wait_timeout(local_queue, &info, stoptime);
			if(jobid > 0) {
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
				n = itable_remove(d->local_job_table, jobid);
				if(n)
					makeflow_node_complete(d, n, local_queue, &info);
			}
		}

		/* Rather than try to garbage collect after each time in this
		 * wait loop, perform garbage collection after a proportional
		 * amount of tasks have passed. */
		makeflow_gc_barrier--;
		if(makeflow_gc_method != MAKEFLOW_GC_NONE && makeflow_gc_barrier == 0) {
			makeflow_gc(d, remote_queue, makeflow_gc_method, makeflow_gc_size, makeflow_gc_count);
			makeflow_gc_barrier = MAX(d->nodeid_counter * makeflow_gc_task_ratio, 1);
		}
	}

	if(makeflow_abort_flag) {
		makeflow_abort_all(d);
	} else {
		if(!makeflow_failed_flag && makeflow_gc_method != MAKEFLOW_GC_NONE) {
			makeflow_gc(d,remote_queue,MAKEFLOW_GC_ALL,0,0);
		}
	}
}

/*
Signal handler to catch abort signals.  Note that permissible actions in signal handlers are very limited, so we emit a message to the terminal and update a global variable noticed by makeflow_run.
*/

static void handle_abort(int sig)
{
	static int abort_count_to_exit = 5;
	
	abort_count_to_exit -= 1;
	int fd = open("/dev/tty", O_WRONLY);
	if (fd >= 0) {
		char buf[256];
		snprintf(buf, sizeof(buf), "Received signal %d, will try to clean up remote resources. Send signal %d more times to force exit.\n", sig, abort_count_to_exit);
		write(fd, buf, strlen(buf));
		close(fd);
	}

	if (abort_count_to_exit == 1)
		signal(sig, SIG_DFL);
	makeflow_abort_flag = 1;

}






static void show_help_run(const char *cmd)
{
	printf("Use: %s [options] <dagfile>\n", cmd);
	printf("Frequently used options:\n\n");
	printf(" %-30s Clean up: remove logfile and all targets. Optional specification [intermediates, outputs] removes only the indicated files.\n", "-c,--clean=<type>");
	printf(" %-30s Batch system type: (default is local)\n", "-T,--batch-type=<type>");
	printf(" %-30s %s\n\n", "", batch_queue_type_string());
	printf("Other options are:\n");
	printf(" %-30s Advertise the master information to a catalog server.\n", "-a,--advertise");
	printf(" %-30s Specify path to Amazon credentials (for use with -T amazon)\n", "--amazon-credentials");
	printf(" %-30s Specify amazon-ami (for use with -T amazon)\n", "--amazon-ami");
	printf(" %-30s Disable the check for AFS. (experts only.)\n", "-A,--disable-afs-check");
	printf(" %-30s Add these options to all batch submit files.\n", "-B,--batch-options=<options>");
	printf(" %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT \n", "-C,--catalog-server=<catalog>");
	printf(" %-30s Enable debugging for this subsystem\n", "-d,--debug=<subsystem>");
	printf(" %-30s Write summary of workflow to this file upon success or failure.\n", "-f,--summary-log=<file>");
	printf(" %-30s Work Queue fast abort multiplier.		   (default is deactivated)\n", "-F,--wq-fast-abort=<#>");
	printf(" %-30s Show this help screen.\n", "-h,--help");
	printf(" %-30s Max number of local jobs to run at once.	(default is # of cores)\n", "-j,--max-local=<#>");
	printf(" %-30s Max number of remote jobs to run at once.\n", "-J,--max-remote=<#>");
	printf("															(default %d for -Twq, %d otherwise.)\n", 10*MAX_REMOTE_JOBS_DEFAULT, MAX_REMOTE_JOBS_DEFAULT );
	printf(" %-30s Use this file for the makeflow log.		 (default is X.makeflowlog)\n", "-l,--makeflow-log=<logfile>");
	printf(" %-30s Use this file for the batch system log.	 (default is X.<type>log)\n", "-L,--batch-log=<logfile>");
	printf(" %-30s Send summary of workflow to this email address upon success or failure.\n", "-m,--email=<email>");
	printf(" %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	printf(" %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf(" %-30s Rotate debug file once it reaches this size.\n", "   --debug-rotate-max=<bytes>");
	printf(" %-30s Password file for authenticating workers.\n", "   --password");
	printf(" %-30s Port number to use with Work Queue.	   (default is %d, 0=arbitrary)\n", "-p,--port=<port>", WORK_QUEUE_DEFAULT_PORT);
	printf(" %-30s Priority. Higher the value, higher the priority.\n", "-P,--priority=<integer>");
	printf(" %-30s Automatically retry failed batch jobs up to %d times.\n", "-R,--retry", makeflow_retry_max);
	printf(" %-30s Automatically retry failed batch jobs up to n times.\n", "-r,--retry-count=<n>");
	printf(" %-30s Wait for output files to be created upto n seconds (e.g., to deal with NFS semantics).\n", "   --wait-for-files-upto=<n>");
	printf(" %-30s Time to retry failed batch job submission.  (default is %ds)\n", "-S,--submission-timeout=<#>", makeflow_submit_timeout);
	printf(" %-30s Work Queue keepalive timeout.			   (default is %ds)\n", "-t,--wq-keepalive-timeout=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	printf(" %-30s Work Queue keepalive interval.			  (default is %ds)\n", "-u,--wq-keepalive-interval=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	printf(" %-30s Show version string\n", "-v,--version");
	printf(" %-30s Work Queue scheduling algorithm.			(time|files|fcfs)\n", "-W,--wq-schedule=<mode>");
	printf(" %-30s Working directory for the batch system.\n", "   --working-dir=<dir|url>");
	printf(" %-30s Wrap all commands with this prefix.\n", "   --wrapper=<cmd>");
	printf(" %-30s Wrapper command requires this input file.\n", "   --wrapper-input=<cmd>");
	printf(" %-30s Wrapper command produces this output file.\n", "   --wrapper-output=<cmd>");
	printf(" %-30s Change directory: chdir to enable executing the Makefile in other directory.\n", "-X,--change-directory");
	printf(" %-30s Force failure on zero-length output files \n", "-z,--zero-length-error");
	printf(" %-30s Select port at random and write it to this file.\n", "-Z,--port-file=<file>");
	printf(" %-30s Disable batch system caching.				 (default is false)\n", "   --disable-cache");
	printf(" %-30s Add node id symbol tags in the makeflow log.		(default is false)\n", "   --log-verbose");
	printf(" %-30s Run each task with a container based on this docker image.\n", "--docker=<image>");
	printf(" %-30s Load docker image from the tar file.\n", "--docker-tar=<tar file>");
	printf(" %-30s Indicate user trusts inputs exist.\n", "--skip-file-check");
	printf(" %-30s Indicate preferred master connection. Choose one of by_ip or by_hostname. (default is by_ip)\n", "--work-queue-preferred-connection");
	printf(" %-30s Use JSON format rather than Make-style format for the input file.\n", "--json");
	printf(" %-30s Indicate preferred mesos master.\n", "--mesos-master=<ip_adr:port>");
	printf(" %-30s Indicate the path to mesos python2 site-packages.\n", "--mesos-path=</path/to/mesos/python/site-packages>");

	printf("\n*Monitor Options:\n\n");
	printf(" %-30s Enable the resource monitor, and write the monitor logs to <dir>.\n", "--monitor=<dir>");
	printf(" %-30s Set monitor interval to <#> seconds.		(default is 1 second)\n", "   --monitor-interval=<#>");
	printf(" %-30s Enable monitor time series.				 (default is disabled)\n", "   --monitor-with-time-series");
	printf(" %-30s Enable monitoring of openened files.		(default is disabled)\n", "   --monitor-with-opened-files");
	printf(" %-30s Format for monitor logs.					(default %s)\n", "   --monitor-log-fmt=<fmt>", DEFAULT_MONITOR_LOG_FORMAT);
}

int main(int argc, char *argv[])
{
	int c;
	const char *dagfile;
	char *change_dir = NULL;
	char *batchlogfilename = NULL;
	const char *batch_submit_options = getenv("BATCH_OPTIONS");
	makeflow_clean_depth clean_mode = MAKEFLOW_CLEAN_NONE;
	char *email_summary_to = NULL;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	char *logfilename = NULL;
	int port_set = 0;
	timestamp_t runtime = 0;
	int skip_afs_check = 0;
	timestamp_t time_completed = 0;
	const char *work_queue_keepalive_interval = NULL;
	const char *work_queue_keepalive_timeout = NULL;
	const char *work_queue_master_mode = "standalone";
	const char *work_queue_port_file = NULL;
	double wq_option_fast_abort_multiplier = -1.0;
	const char *amazon_credentials = NULL;
	const char *amazon_ami = NULL;
	const char *priority = NULL;
	char *work_queue_password = NULL;
	char *wq_wait_queue_size = 0;
	int did_explicit_auth = 0;
	char *chirp_tickets = NULL;
	char *working_dir = NULL;
	char *work_queue_preferred_connection = NULL;
	char *write_summary_to = NULL;
	char *s;
	char *log_dir = NULL;
	char *log_format = NULL;
	category_mode_t allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;
	char *mesos_master = "127.0.0.1:5050/";
	char *mesos_path = NULL;


	random_init();
	debug_config(argv[0]);

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
		work_queue_master_mode = s;
	}

	s = getenv("WORK_QUEUE_NAME");
	if(s) {
		project = xxstrdup(s);
	}
	s = getenv("WORK_QUEUE_FAST_ABORT_MULTIPLIER");
	if(s) {
		wq_option_fast_abort_multiplier = atof(s);
	}

	enum {
		LONG_OPT_AUTH = UCHAR_MAX+1,
		LONG_OPT_DEBUG_ROTATE_MAX,
		LONG_OPT_DISABLE_BATCH_CACHE,
		LONG_OPT_DOT_CONDENSE,
		LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME,
		LONG_OPT_GC_SIZE,
		LONG_OPT_MONITOR,
		LONG_OPT_MONITOR_INTERVAL,
		LONG_OPT_MONITOR_LOG_NAME,
		LONG_OPT_MONITOR_OPENED_FILES,
		LONG_OPT_MONITOR_TIME_SERIES,
		LONG_OPT_PASSWORD,
		LONG_OPT_TICKETS,
		LONG_OPT_VERBOSE_PARSING,
		LONG_OPT_LOG_VERBOSE_MODE,
		LONG_OPT_WORKING_DIR,
		LONG_OPT_PREFERRED_CONNECTION,
		LONG_OPT_WQ_WAIT_FOR_WORKERS,
		LONG_OPT_WRAPPER,
		LONG_OPT_WRAPPER_INPUT,
		LONG_OPT_WRAPPER_OUTPUT,
		LONG_OPT_DOCKER,
		LONG_OPT_DOCKER_TAR,
		LONG_OPT_AMAZON_CREDENTIALS,
		LONG_OPT_AMAZON_AMI,
		LONG_OPT_JSON,
		LONG_OPT_SKIP_FILE_CHECK,
		LONG_OPT_ALLOCATION_MODE,
		LONG_OPT_MESOS_MASTER,
		LONG_OPT_MESOS_PATH
	};

	static const struct option long_options_run[] = {
		{"advertise", no_argument, 0, 'a'},
		{"allocation", required_argument, 0, LONG_OPT_ALLOCATION_MODE},
		{"auth", required_argument, 0, LONG_OPT_AUTH},
		{"batch-log", required_argument, 0, 'L'},
		{"batch-options", required_argument, 0, 'B'},
		{"batch-type", required_argument, 0, 'T'},
		{"catalog-server", required_argument, 0, 'C'},
		{"clean", optional_argument, 0, 'c'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, LONG_OPT_DEBUG_ROTATE_MAX},
		{"disable-afs-check", no_argument, 0, 'A'},
		{"disable-cache", no_argument, 0, LONG_OPT_DISABLE_BATCH_CACHE},
		{"email", required_argument, 0, 'm'},
		{"wait-for-files-upto", required_argument, 0, LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME},
		{"gc", required_argument, 0, 'g'},
		{"gc-size", required_argument, 0, LONG_OPT_GC_SIZE},
		{"gc-count", required_argument, 0, 'G'},
		{"wait-for-files-upto", required_argument, 0, LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME},
		{"help", no_argument, 0, 'h'},
		{"makeflow-log", required_argument, 0, 'l'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
		{"monitor", required_argument, 0, LONG_OPT_MONITOR},
		{"monitor-interval", required_argument, 0, LONG_OPT_MONITOR_INTERVAL},
		{"monitor-log-name", required_argument, 0, LONG_OPT_MONITOR_LOG_NAME},
		{"monitor-with-opened-files", no_argument, 0, LONG_OPT_MONITOR_OPENED_FILES},
		{"monitor-with-time-series",  no_argument, 0, LONG_OPT_MONITOR_TIME_SERIES},
		{"password", required_argument, 0, LONG_OPT_PASSWORD},
		{"port", required_argument, 0, 'p'},
		{"port-file", required_argument, 0, 'Z'},
		{"priority", required_argument, 0, 'P'},
		{"project-name", required_argument, 0, 'N'},
		{"retry", no_argument, 0, 'R'},
		{"retry-count", required_argument, 0, 'r'},
		{"show-output", no_argument, 0, 'O'},
		{"submission-timeout", required_argument, 0, 'S'},
		{"summary-log", required_argument, 0, 'f'},
		{"tickets", required_argument, 0, LONG_OPT_TICKETS},
		{"version", no_argument, 0, 'v'},
		{"log-verbose", no_argument, 0, LONG_OPT_LOG_VERBOSE_MODE},
		{"working-dir", required_argument, 0, LONG_OPT_WORKING_DIR},
		{"skip-file-check", no_argument, 0, LONG_OPT_SKIP_FILE_CHECK},
		{"work-queue-preferred-connection", required_argument, 0, LONG_OPT_PREFERRED_CONNECTION},
		{"wq-estimate-capacity", no_argument, 0, 'E'},
		{"wq-fast-abort", required_argument, 0, 'F'},
		{"wq-keepalive-interval", required_argument, 0, 'u'},
		{"wq-keepalive-timeout", required_argument, 0, 't'},
		{"wq-schedule", required_argument, 0, 'W'},
		{"wq-wait-queue-size", required_argument, 0, LONG_OPT_WQ_WAIT_FOR_WORKERS},
		{"wrapper", required_argument, 0, LONG_OPT_WRAPPER},
		{"wrapper-input", required_argument, 0, LONG_OPT_WRAPPER_INPUT},
		{"wrapper-output", required_argument, 0, LONG_OPT_WRAPPER_OUTPUT},
		{"zero-length-error", no_argument, 0, 'z'},
		{"change-directory", required_argument, 0, 'X'},
		{"docker", required_argument, 0, LONG_OPT_DOCKER},
		{"docker-tar", required_argument, 0, LONG_OPT_DOCKER_TAR},
		{"amazon-credentials", required_argument, 0, LONG_OPT_AMAZON_CREDENTIALS},
		{"amazon-ami", required_argument, 0, LONG_OPT_AMAZON_AMI},
		{"json", no_argument, 0, LONG_OPT_JSON},
		{"mesos-master", required_argument, 0, LONG_OPT_MESOS_MASTER},
		{"mesos-path", required_argument, 0, LONG_OPT_MESOS_PATH},
		{0, 0, 0, 0}
	};

	static const char option_string_run[] = "aAB:c::C:d:Ef:F:g:G:hj:J:l:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:X:zZ:";

	while((c = getopt_long(argc, argv, option_string_run, long_options_run, NULL)) >= 0) {
		switch (c) {
			case 'a':
				work_queue_master_mode = "catalog";
				break;
			case 'A':
				skip_afs_check = 1;
				break;
			case 'B':
				batch_submit_options = optarg;
				break;
			case 'c':
				clean_mode = MAKEFLOW_CLEAN_ALL;
				if(optarg){
					if(strcasecmp(optarg, "intermediates") == 0){
						clean_mode = MAKEFLOW_CLEAN_INTERMEDIATES;
					} else if(strcasecmp(optarg, "outputs") == 0){
						clean_mode = MAKEFLOW_CLEAN_OUTPUTS;
					} else if(strcasecmp(optarg, "all") != 0){
						fprintf(stderr, "makeflow: unknown clean option %s", optarg);
						exit(1);
					}
				}
				break;
			case 'C':
				setenv("CATALOG_HOST", optarg, 1);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'E':
				// This option is deprecated. Capacity estimation is now on by default.
				break;
			case LONG_OPT_AUTH:
				if (!auth_register_byname(optarg))
					fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
				did_explicit_auth = 1;
				break;
			case LONG_OPT_TICKETS:
				chirp_tickets = strdup(optarg);
				break;
			case 'f':
				write_summary_to = xxstrdup(optarg);
				break;
			case 'F':
				wq_option_fast_abort_multiplier = atof(optarg);
				break;
			case 'g':
				if(strcasecmp(optarg, "none") == 0) {
					makeflow_gc_method = MAKEFLOW_GC_NONE;
				} else if(strcasecmp(optarg, "ref_count") == 0) {
					makeflow_gc_method = MAKEFLOW_GC_COUNT;
					if(makeflow_gc_count < 0)
						makeflow_gc_count = 16;	/* Try to collect at most 16 files. */
				} else if(strcasecmp(optarg, "on_demand") == 0) {
					makeflow_gc_method = MAKEFLOW_GC_ON_DEMAND;
					if(makeflow_gc_count < 0)
						makeflow_gc_count = 16;	/* Try to collect at most 16 files. */
				} else if(strcasecmp(optarg, "all") == 0) {
					makeflow_gc_method = MAKEFLOW_GC_ALL;
					if(makeflow_gc_count < 0)
						makeflow_gc_count = 1 << 14;	/* Inode threshold of 2^14. */
				} else {
					fprintf(stderr, "makeflow: invalid garbage collection method: %s\n", optarg);
					exit(1);
				}
				break;
			case LONG_OPT_GC_SIZE:
				makeflow_gc_size = string_metric_parse(optarg);
				break;
			case 'G':
				makeflow_gc_count = atoi(optarg);
				break;
			case LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME:
				file_creation_patience_wait_time = MAX(0,atoi(optarg));
				break;
			case 'h':
				show_help_run(argv[0]);
				return 0;
			case 'j':
				explicit_local_jobs_max = atoi(optarg);
				break;
			case 'J':
				explicit_remote_jobs_max = atoi(optarg);
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
			case LONG_OPT_MONITOR:
				if (!monitor) monitor = makeflow_monitor_create();
				if(log_dir) free(log_dir);
				log_dir = xxstrdup(optarg);
				break;
			case LONG_OPT_MONITOR_INTERVAL:
				if (!monitor) monitor = makeflow_monitor_create();
				monitor->interval = atoi(optarg);
				break;
			case LONG_OPT_MONITOR_TIME_SERIES:
				if (!monitor) monitor = makeflow_monitor_create();
				monitor->enable_time_series = 1;
				break;
			case LONG_OPT_MONITOR_OPENED_FILES:
				if (!monitor) monitor = makeflow_monitor_create();
				monitor->enable_list_files = 1;
				break;
			case LONG_OPT_MONITOR_LOG_NAME:
				if (!monitor) monitor = makeflow_monitor_create();
				if(log_format) free(log_format);
				log_format = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_CREDENTIALS:
				amazon_credentials = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_AMI:
				amazon_ami = xxstrdup(optarg);
				break;
			case 'M':
			case 'N':
				free(project);
				project = xxstrdup(optarg);
				work_queue_master_mode = "catalog";
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'p':
				port_set = 1;
				port = atoi(optarg);
				break;
			case 'P':
				priority = optarg;
				break;
			case 'r':
				makeflow_retry_flag = 1;
				makeflow_retry_max = atoi(optarg);
				break;
			case 'R':
				makeflow_retry_flag = 1;
				break;
			case 'S':
				makeflow_submit_timeout = atoi(optarg);
				break;
			case 't':
				work_queue_keepalive_timeout = optarg;
				break;
			case 'T':
				batch_queue_type = batch_queue_type_from_string(optarg);
				if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr, "makeflow: unknown batch queue type: %s\n", optarg);
					return 1;
				}
				break;
			case 'u':
				work_queue_keepalive_interval = optarg;
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
				work_queue_port_file = optarg;
				port = 0;
				port_set = 1;	//WQ is going to set the port, so we continue as if already set.
				break;
			case LONG_OPT_PASSWORD:
				if(copy_file_to_buffer(optarg, &work_queue_password, NULL) < 0) {
					fprintf(stderr, "makeflow: couldn't open %s: %s\n", optarg, strerror(errno));
					return 1;
				}
				break;
			case LONG_OPT_DISABLE_BATCH_CACHE:
				cache_mode = 0;
				break;
			case LONG_OPT_WQ_WAIT_FOR_WORKERS:
				wq_wait_queue_size = optarg;
				break;
			case LONG_OPT_WORKING_DIR:
				free(working_dir);
				working_dir = xxstrdup(optarg);
				break;
			case LONG_OPT_PREFERRED_CONNECTION:
				free(work_queue_preferred_connection);
				work_queue_preferred_connection = xxstrdup(optarg);
				break;
			case LONG_OPT_DEBUG_ROTATE_MAX:
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case LONG_OPT_LOG_VERBOSE_MODE:
				log_verbose_mode = 1;
				break;
			case LONG_OPT_WRAPPER:
				if(!wrapper) wrapper = makeflow_wrapper_create();
				makeflow_wrapper_add_command(wrapper, optarg);
				break;
			case LONG_OPT_WRAPPER_INPUT:
				if(!wrapper) wrapper = makeflow_wrapper_create();
				makeflow_wrapper_add_input_file(wrapper, optarg);
				break;
			case LONG_OPT_WRAPPER_OUTPUT:
				if(!wrapper) wrapper = makeflow_wrapper_create();
				makeflow_wrapper_add_output_file(wrapper, optarg);
				break;
			case LONG_OPT_DOCKER:
				if(!wrapper) wrapper = makeflow_wrapper_create();
				container_mode = CONTAINER_MODE_DOCKER;
				container_image = xxstrdup(optarg);
				break;
			case LONG_OPT_SKIP_FILE_CHECK:
				skip_file_check = 1;
				break;
			case LONG_OPT_DOCKER_TAR:
				image_tar = xxstrdup(optarg);
				break;
			case LONG_OPT_ALLOCATION_MODE:
				if(!strcmp(optarg, "throughput")) {
					allocation_mode = CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT;
				} else if(!strcmp(optarg, "waste")) {
					allocation_mode = CATEGORY_ALLOCATION_MODE_MIN_WASTE;
				} else if(!strcmp(optarg, "fixed")) {
					allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;
				} else {
					fatal("Allocation mode '%s' is not valid. Use one of: throughput waste fixed");
				}
			case LONG_OPT_JSON:
				json_input = 1;
				break;
			case LONG_OPT_MESOS_MASTER:
				mesos_master = xxstrdup(optarg);
				break;
			case LONG_OPT_MESOS_PATH:
				mesos_path = xxstrdup(optarg);
				break;
			default:
				show_help_run(argv[0]);
				return 1;
			case 'X':
				change_dir = optarg;
				break;
		}
	}

	cctools_version_debug(D_MAKEFLOW_RUN, argv[0]);

	if(!did_explicit_auth)
		auth_register_all();
	if(chirp_tickets) {
		auth_ticket_load(chirp_tickets);
		free(chirp_tickets);
	} else {
		auth_ticket_load(NULL);
	}

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
		if(strcmp(work_queue_master_mode, "catalog") == 0 && project == NULL) {
			fprintf(stderr, "makeflow: Makeflow running in catalog mode. Please use '-N' option to specify the name of this project.\n");
			fprintf(stderr, "makeflow: Run \"makeflow -h\" for help with options.\n");
			return 1;
		}
		// Use Work Queue default port in standalone mode when port is not
		// specified with -p option. In Work Queue catalog mode, Work Queue
		// would choose an arbitrary port when port is not explicitly specified.
		if(!port_set && strcmp(work_queue_master_mode, "standalone") == 0) {
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

	printf("parsing %s...\n",dagfile);
	struct dag *d;
	if (json_input) {
		struct jx *j = jx_parse_file(dagfile);
		d = dag_from_jx(j);
		jx_delete(j);
	} else {
		d = dag_from_file(dagfile);
	}
	if(!d) {
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

	d->allocation_mode = allocation_mode;

	// Makeflows running LOCAL batch type have only one queue that behaves as if remote
	// This forces -J vs -j to behave correctly
	if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
		explicit_remote_jobs_max = explicit_local_jobs_max;
	}

	if(explicit_local_jobs_max) {
		local_jobs_max = explicit_local_jobs_max;
	} else {
		local_jobs_max = load_average_get_cpus();
	}

	if(explicit_remote_jobs_max) {
		remote_jobs_max = explicit_remote_jobs_max;
	} else {
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			remote_jobs_max = load_average_get_cpus();
		} else if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
			remote_jobs_max = 10 * MAX_REMOTE_JOBS_DEFAULT;
		} else {
			remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
		}
	}

	s = getenv("MAKEFLOW_MAX_REMOTE_JOBS");
	if(s) {
		remote_jobs_max = MIN(remote_jobs_max, atoi(s));
	}

	s = getenv("MAKEFLOW_MAX_LOCAL_JOBS");
	if(s) {
		int n = atoi(s);
		local_jobs_max = MIN(local_jobs_max, n);
		if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
			remote_jobs_max = MIN(local_jobs_max, n);
		}
	}

	remote_queue = batch_queue_create(batch_queue_type);

	if(!remote_queue) {
		fprintf(stderr, "makeflow: couldn't create batch queue.\n");
		if(port != 0)
			fprintf(stderr, "makeflow: perhaps port %d is already in use?\n", port);
		exit(EXIT_FAILURE);
	}

	if(!batchlogfilename) {
		if(batch_queue_supports_feature(remote_queue, "batch_log_name")){
			batchlogfilename = string_format(batch_queue_supports_feature(remote_queue, "batch_log_name"), dagfile);
		} else {
			batchlogfilename = string_format("%s.batchlog", dagfile);
		}
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_MESOS) {
		batch_queue_set_option(remote_queue, "mesos-path", mesos_path);
		batch_queue_set_option(remote_queue, "mesos-master", mesos_master);
		batch_queue_set_feature(remote_queue, "batch_log_name", batchlogfilename);
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_DRYRUN) {
		FILE *file = fopen(batchlogfilename,"w");
		if(!file) fatal("unable to open log file %s: %s\n", batchlogfilename, strerror(errno));
		fprintf(file, "#!/bin/sh\n");
		fprintf(file, "set -x\n");
		fprintf(file, "set -e\n");
		fprintf(file, "\n# %s version %s (released %s)\n\n", argv[0], CCTOOLS_VERSION, CCTOOLS_RELEASE_DATE);
		fclose(file);
	}

	batch_queue_set_logfile(remote_queue, batchlogfilename);
	batch_queue_set_option(remote_queue, "batch-options", batch_submit_options);
	batch_queue_set_option(remote_queue, "skip-afs-check", skip_afs_check ? "yes" : "no");
	batch_queue_set_option(remote_queue, "password", work_queue_password);
	batch_queue_set_option(remote_queue, "master-mode", work_queue_master_mode);
	batch_queue_set_option(remote_queue, "name", project);
	batch_queue_set_option(remote_queue, "priority", priority);
	batch_queue_set_option(remote_queue, "keepalive-interval", work_queue_keepalive_interval);
	batch_queue_set_option(remote_queue, "keepalive-timeout", work_queue_keepalive_timeout);
	batch_queue_set_option(remote_queue, "caching", cache_mode ? "yes" : "no");
	batch_queue_set_option(remote_queue, "wait-queue-size", wq_wait_queue_size);
	batch_queue_set_option(remote_queue, "amazon-credentials", amazon_credentials);
	batch_queue_set_option(remote_queue, "amazon-ami", amazon_ami);
	batch_queue_set_option(remote_queue, "working-dir", working_dir);
	batch_queue_set_option(remote_queue, "master-preferred-connection", work_queue_preferred_connection);

	char *fa_multiplier = string_format("%f", wq_option_fast_abort_multiplier);
	batch_queue_set_option(remote_queue, "fast-abort", fa_multiplier);
	free(fa_multiplier);

	/* Do not create a local queue for systems where local and remote are the same. */

	if(!batch_queue_supports_feature(remote_queue, "local_job_queue")) {
		local_queue = 0;
	} else {
		local_queue = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL);
		if(!local_queue) {
			fatal("couldn't create local job queue.");
		}
	}

	/* Remote storage modes do not (yet) support measuring storage for garbage collection. */

	if(makeflow_gc_method == MAKEFLOW_GC_SIZE && !batch_queue_supports_feature(remote_queue, "gc_size")) {
		makeflow_gc_method = MAKEFLOW_GC_ALL;
	}

	makeflow_parse_input_outputs(d);
	makeflow_prepare_nested_jobs(d);

	if (change_dir)
		chdir(change_dir);

	printf("checking %s for consistency...\n",dagfile);
	if(!makeflow_check(d)) {
		exit(EXIT_FAILURE);
	}
	if(!makeflow_check_batch_consistency(d) && clean_mode == MAKEFLOW_CLEAN_NONE) {
		exit(EXIT_FAILURE);
	}

	printf("%s has %d rules.\n",dagfile,d->nodeid_counter);

	setlinebuf(stdout);
	setlinebuf(stderr);

	makeflow_log_recover(d, logfilename, log_verbose_mode, remote_queue, clean_mode, skip_file_check );

	if(monitor) {
		if(!log_dir)
			fatal("Monitor mode was enabled, but a log output directory was not specified (use --monitor=<dir>)");

		if(!log_format)
			log_format = xxstrdup(DEFAULT_MONITOR_LOG_FORMAT);

		if(monitor->interval < 1)
			fatal("Monitoring interval should be positive.");

		makeflow_prepare_for_monitoring(d, monitor, remote_queue, log_dir, log_format);
		free(log_dir);
		free(log_format);
	}

	struct dag_file *f = dag_file_lookup_or_create(d, batchlogfilename);
	makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXPECT);

	if(clean_mode != MAKEFLOW_CLEAN_NONE) {
		printf("cleaning filesystem...\n");
		makeflow_clean(d, remote_queue, clean_mode);
		if(clean_mode == MAKEFLOW_CLEAN_ALL) {
			unlink(logfilename);
		}
		exit(0);
	}


	printf("starting workflow....\n");

	port = batch_queue_port(remote_queue);
	if(work_queue_port_file)
		opts_write_port_file(work_queue_port_file, port);
	if(port > 0)
		printf("listening for workers on port %d.\n", port);

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);

	makeflow_log_started_event(d);

	runtime = timestamp_get();

	if (container_mode == CONTAINER_MODE_DOCKER) {
		makeflow_wrapper_docker_init(wrapper, container_image, image_tar);
	}
	
	makeflow_run(d);
	time_completed = timestamp_get();
	runtime = time_completed - runtime;

	if(local_queue)
		batch_queue_delete(local_queue);
	
	if (batch_queue_type == BATCH_QUEUE_TYPE_MESOS) {
		batch_queue_set_int_option(remote_queue, "batch-queue-abort-flag", (int)makeflow_abort_flag);
		batch_queue_set_int_option(remote_queue, "batch-queue-failed-flag", (int)makeflow_failed_flag);
	}

	batch_queue_delete(remote_queue);

	if(write_summary_to || email_summary_to)
		makeflow_summary_create(d, write_summary_to, email_summary_to, runtime, time_completed, argc, argv, dagfile, remote_queue, makeflow_abort_flag, makeflow_failed_flag );

	/* XXX better to write created files to log, then delete those listed in log. */
	if (container_mode == CONTAINER_MODE_DOCKER) {
		char *cmd = string_format("rm %s", CONTAINER_SH);
		system(cmd);
		free(cmd);
	}

	if(makeflow_abort_flag) {
		makeflow_log_aborted_event(d);
		fprintf(stderr, "workflow was aborted.\n");
		exit(EXIT_FAILURE);
	} else if(makeflow_failed_flag) {
		makeflow_log_failed_event(d);
		fprintf(stderr, "workflow failed.\n");
		exit(EXIT_FAILURE);
	} else {
		makeflow_log_completed_event(d);
		printf("nothing left to do.\n");
		exit(EXIT_SUCCESS);
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
