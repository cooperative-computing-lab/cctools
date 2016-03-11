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
#include "create_dir.h"
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
#include "jx_eval.h"
#include "create_dir.h"
#include "sha1.h"

#include "dag.h"
#include "dag_node.h"
#include "dag_visitors.h"
#include "parser.h"
#include "parser_jx.h"

#include "makeflow_summary.h"
#include "makeflow_alloc.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_docker.h"
#include "makeflow_wrapper_monitor.h"
#include "makeflow_wrapper_umbrella.h"
#include "makeflow_mounts.h"
#include "makeflow_wrapper_enforcement.h"
#include "makeflow_wrapper_singularity.h"
#include "makeflow_archive.h"
#include "makeflow_catalog_reporter.h"
#include "makeflow_local_resources.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <libgen.h>

#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

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

static sig_atomic_t makeflow_abort_flag = 0;
static int makeflow_failed_flag = 0;
static int makeflow_submit_timeout = 3600;
static int makeflow_retry_flag = 0;
static int makeflow_retry_max = 5;

/* makeflow_gc_method indicates the type of garbage collection
 * indicated by the user. Refer to makeflow_gc.h for specifics */
static makeflow_gc_method_t makeflow_gc_method = MAKEFLOW_GC_NONE;
/* Disk size at which point GC is run */
static uint64_t makeflow_gc_size   = 0;
/* # of files after which GC is run */
static int makeflow_gc_count  = -1;
/* Iterations of wait loop prior ot GC check */
static int makeflow_gc_barrier = 1;
/* Determines next gc_barrier to make checks less frequent with large number of tasks */
static double makeflow_gc_task_ratio = 0.05;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static struct rmsummary *local_resources = 0;

static int local_jobs_max = 1;
static int remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;

static char *project = NULL;
static int port = 0;
static int output_len_check = 0;
static int skip_file_check = 0;

static int cache_mode = 1;

static container_mode_t container_mode = CONTAINER_MODE_NONE;
static char *container_image = NULL;
static char *container_image_tar = NULL;

static char *parrot_path = "./parrot_run";

/*
Wait upto this many seconds for an output file of a succesfull task
to appear on the local filesystem (e.g, to deal with NFS
semantics.
*/
static int file_creation_patience_wait_time = 0;

/*
Write a verbose transaction log with SYMBOL tags.
SYMBOLs are category labels (SYMBOLs should be deprecated
once weaver/pbui tools are updated.)
*/
static int log_verbose_mode = 0;

static struct makeflow_wrapper *wrapper = 0;
static struct makeflow_monitor *monitor = 0;
static struct makeflow_wrapper *enforcer = 0;
static struct makeflow_wrapper_umbrella *umbrella = 0;

static int catalog_reporting_on = 0;

static char *mountfile = NULL;
static char *mount_cache = NULL;
static int use_mountfile = 0;

static struct list *shared_fs_list = NULL;

static int did_find_archived_job = 0;

static struct makeflow_alloc *storage_allocation = 0;

/*
Determines if this is a local job that will consume
local resources, regardless of the batch queue type.
*/

static int is_local_job( struct dag_node *n )
{
	return n->local_job || batch_queue_type==BATCH_QUEUE_TYPE_LOCAL;
}

/*
Generates file list for node based on node files, wrapper
input files, and monitor input files. Relies on %% nodeid
replacement for monitor file names.
*/

static struct list *makeflow_generate_input_files( struct dag_node *n )
{
	struct list *result = list_duplicate(n->source_files);

	if(wrapper)  result = makeflow_wrapper_generate_files(result, wrapper->input_files, n, wrapper);
	if(enforcer) result = makeflow_wrapper_generate_files(result, enforcer->input_files, n, enforcer);
	if(umbrella) result = makeflow_wrapper_generate_files(result, umbrella->wrapper->input_files, n, umbrella->wrapper);
	if(monitor)  result = makeflow_wrapper_generate_files(result, monitor->wrapper->input_files, n, monitor->wrapper);

	return result;
}

static struct list *makeflow_generate_output_files( struct dag_node *n )
{
	struct list *result = list_duplicate(n->target_files);

	if(wrapper)  result = makeflow_wrapper_generate_files(result, wrapper->output_files, n, wrapper);
	if(enforcer) result = makeflow_wrapper_generate_files(result, enforcer->output_files, n, enforcer);
	if(umbrella) result = makeflow_wrapper_generate_files(result, umbrella->wrapper->output_files, n, umbrella->wrapper);
	if(monitor)  result = makeflow_wrapper_generate_files(result, monitor->wrapper->output_files, n, monitor->wrapper);

	return result;
}

/*
Abort one job in a given batch queue.
*/

static void makeflow_abort_job( struct dag *d, struct dag_node *n, struct batch_queue *q, UINT64_T jobid, const char *name )
{
	printf("aborting %s job %" PRIu64 "\n", name, jobid);

	batch_job_remove(q, jobid);
	makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);

	struct list *outputs = makeflow_generate_output_files(n);
	struct dag_file *f;
	list_first_item(outputs);

	while((f = list_next_item(outputs)))
		makeflow_clean_file(d, q, f, 0);

	makeflow_clean_node(d, q, n, 1);
}

/*
Abort the dag by removing all batch jobs from all queues.
*/

static void makeflow_abort_all(struct dag *d)
{
	UINT64_T jobid;
	struct dag_node *n;

	printf("got abort signal...\n");

	itable_firstkey(d->local_job_table);
	while(itable_nextkey(d->local_job_table, &jobid, (void **) &n)) {
		makeflow_abort_job(d,n,local_queue,jobid,"local");
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		makeflow_abort_job(d,n,remote_queue,jobid,"remote");
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
	struct list *outputs = makeflow_generate_output_files(n);
	list_first_item(outputs);
	while((f1 = list_next_item(outputs)))
		makeflow_clean_file(d, remote_queue, f1, 0, storage_allocation);
	list_delete(outputs);
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

static void makeflow_prepare_node_sizes(struct dag *d, char *storage_print)
{
	struct dag_node *n = dag_node_create(d, -1);
	n->state = DAG_NODE_STATE_COMPLETE;
	struct dag_node *p;

	for(p = d->nodes; p; p = p->next) {
		if(set_size(p->ancestors) == 0) {
			set_push(n->descendants, p);
		}
	}

	dag_node_determine_children(n);
	dag_node_prepare_node_size(n);
	dag_node_determine_footprint(n);
	if(storage_print){
		dag_node_print_footprint(d, storage_print);
		exit(0);
	}
}

/*
Update nested jobs with appropriate number of local jobs
(total local jobs max / maximum number of concurrent nests).
*/

static void makeflow_prepare_nested_jobs(struct dag *d)
{
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
Match a filename (/home/fred) to a path stem (/home).
Returns 0 on match, non-zero otherwise.
*/

static int prefix_match(void *stem, const void *filename) {
	assert(stem);
	assert(filename);
	return strncmp(stem, filename, strlen(stem));
}

/*
Returns true if the given filename is located in
a shared filesystem, as given by the shared_fs_list.
*/

static int makeflow_file_on_sharedfs( const char *filename )
{
	return !list_iterate(shared_fs_list,prefix_match,filename);
}

/*
Given a file, return the string that identifies it appropriately
for the given batch system, combining the local and remote name
and making substitutions according to the node.
*/

static char * makeflow_file_format( struct dag_node *n, struct dag_file *f, struct batch_queue *queue )
{
	const char *remotename = dag_node_get_remote_name(n, f->filename);
	if(!remotename && wrapper) remotename = makeflow_wrapper_get_remote_name(wrapper, n->d, f->filename);
	if(!remotename && enforcer) remotename = makeflow_wrapper_get_remote_name(enforcer, n->d, f->filename);
	if(!remotename && monitor) remotename = makeflow_wrapper_get_remote_name(monitor->wrapper, n->d, f->filename);
	if(!remotename && umbrella) remotename = makeflow_wrapper_get_remote_name(umbrella->wrapper, n->d, f->filename);
	if(!remotename) remotename = f->filename;

	if(batch_queue_supports_feature(queue,"remote_rename")) {
			return string_format("%s=%s,", f->filename, remotename);
	} else {
			return string_format("%s,", f->filename);
	}
}

/*
Given a list of files, add the files to the given string.
Returns the original string, realloced if necessary
*/

static char * makeflow_file_list_format( struct dag_node *node, char *file_str, struct list *file_list, struct batch_queue *queue )
{
	struct dag_file *file;

	if(!file_str) file_str = strdup("");

	if(!file_list) return file_str;

	list_first_item(file_list);
	while((file=list_next_item(file_list))) {
		if (makeflow_file_on_sharedfs(file->filename)) {
			debug(D_MAKEFLOW_RUN, "Skipping file %s on shared fs\n",
				file->filename);
			continue;
		}
		char *f = makeflow_file_format(node,file,queue);
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
		if(makeflow_abort_flag) break;

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
Expand a dag_node into a text list of input files,
output files, and a command, by applying all wrappers
and settings.  Used at both job submission and completion
to obtain identical strings.
*/

static void makeflow_node_expand( struct dag_node *n, struct batch_queue *queue, struct list **input_list, struct list **output_list, char **input_files, char **output_files, char **command )
{
	makeflow_wrapper_umbrella_set_input_files(umbrella, queue, n);

	if (*input_list == NULL) {
		*input_list  = makeflow_generate_input_files(n);
	}

	if (*output_list == NULL) {
		*output_list = makeflow_generate_output_files(n);
	}

	/* Create strings for all the files mentioned by this node. */
	*input_files  = makeflow_file_list_format(n, 0, *input_list,  queue);
	*output_files = makeflow_file_list_format(n, 0, *output_list, queue);

	/* Expand the command according to each of the wrappers */
	*command = strdup(n->command);
	*command = makeflow_wrap_wrapper(*command, n, wrapper);
	*command = makeflow_wrap_enforcer(*command, n, enforcer, *input_list, *output_list);
	*command = makeflow_wrap_umbrella(*command, n, umbrella, queue, *input_files, *output_files);
	*command = makeflow_wrap_monitor(*command, n, queue, monitor);
}

/*
Submit a node to the appropriate batch system, after materializing
the necessary list of input and output files, and applying all
wrappers and options.
*/

static void makeflow_node_submit(struct dag *d, struct dag_node *n, const struct rmsummary *resources)
{
	struct batch_queue *queue;
	struct dag_file *f;
	struct list *input_list = NULL, *output_list = NULL;
	char *input_files = NULL, *output_files = NULL, *command = NULL;

	if(n->local_job && local_queue) {
		queue = local_queue;
	} else {
		queue = remote_queue;
	}

	if(makeflow_alloc_commit_space(storage_allocation, n)){
		makeflow_log_alloc_event(d, storage_allocation);
	} else {
		printf("Unable to commit enough space for execution");
	}

	makeflow_node_expand(n, queue, &input_list, &output_list, &input_files, &output_files, &command);

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

	batch_queue_set_int_option(queue, "task-id", n->nodeid);

	/* Generate the environment vars specific to this node. */
	struct jx *envlist = dag_node_env_create(d,n);

	/* Logs the creation of output files. */
	makeflow_log_file_list_state_change(d,output_list,DAG_FILE_STATE_EXPECT);

	/* check archiving directory to see if node has already been preserved */
	if (d->should_read_archive && makeflow_archive_is_preserved(d, n, command, input_list, output_list)) {
		printf("node %d already exists in archive, replicating output files\n", n->nodeid);

		/* copy archived files to working directory and update state for node and dag_files */
		makeflow_archive_copy_preserved_files(d, n, output_list);
		n->state = DAG_NODE_STATE_RUNNING;
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
			makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXISTS);
		}
		makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);
		did_find_archived_job = 1;
	} else {
		/* Now submit the actual job, retrying failures as needed. */

		const struct rmsummary *resources = dag_node_dynamic_label(n);

		n->jobid = makeflow_node_submit_retry(queue,command,input_files,output_files,envlist, resources);

		/* Update all of the necessary data structures. */
		if(n->jobid >= 0) {
			memcpy(n->resources_allocated, resources, sizeof(struct rmsummary));
			makeflow_log_state_change(d, n, DAG_NODE_STATE_RUNNING);

			if(is_local_job(n)) {
				makeflow_local_resources_subtract(local_resources,n);
			}

			if(n->local_job && local_queue) {
				itable_insert(d->local_job_table, n->jobid, n);
			} else {
				itable_insert(d->remote_job_table, n->jobid, n);
			}
		} else {
			makeflow_log_state_change(d, n, DAG_NODE_STATE_FAILED);
			makeflow_failed_flag = 1;
		}
	}

	/* Restore old batch job options. */
	if(previous_batch_options) {
		batch_queue_set_option(queue, "batch-options", previous_batch_options);
		free(previous_batch_options);
	}

	free(command);
	list_delete(input_list);
	list_delete(output_list);
	free(input_files);
	free(output_files);
	jx_delete(envlist);
}

static int makeflow_node_ready(struct dag *d, struct dag_node *n, const struct rmsummary *resources)
{
	struct dag_file *f;

	if(n->state != DAG_NODE_STATE_WAITING)
		return 0;

	if(is_local_job(n)) {
		if(!makeflow_local_resources_available(local_resources,resources))
			return 0;
	}

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

	if(storage_allocation->locked && !(
			makeflow_alloc_check_space(storage_allocation, n) &&
			dag_node_dependencies_active(n))){
		return 0;
	}

	return 1;
}

int makeflow_nodes_local_waiting_count(const struct dag *d) {
	int count = 0;

	
	struct dag_node *n;
	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_WAITING && is_local_job(n))
			count++;
	}

	return count;
}

/*
Find all jobs ready to be run, then submit them.
*/

static void makeflow_dispatch_ready_jobs(struct dag *d)
{
	struct dag_node *n;

	for(n = d->nodes; n; n = n->next) {
		if(dag_remote_jobs_running(d) >= remote_jobs_max && dag_local_jobs_running(d) >= local_jobs_max) {
			break;
		}

		const struct rmsummary *resources = dag_node_dynamic_label(n);
		if(makeflow_node_ready(d, n, resources)) {
			makeflow_node_submit(d, n, resources);
		}
	}
}

/*
Check the the indicated file was created and log, error, or retry as appropriate.
*/

int makeflow_node_check_file_was_created(struct dag *d, struct dag_node *n, struct dag_file *f)
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
			d->total_file_size += f->actual_size;
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

	if(is_local_job(n)) {
		makeflow_local_resources_add(local_resources,n);
	}

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

	struct list *outputs = makeflow_generate_output_files(n);

	if(info->disk_allocation_exhausted) {
		job_failed = 1;
	}
	else if(info->exited_normally && info->exit_code == 0) {
		list_first_item(outputs);
		while((f = list_next_item(outputs))) {
			if(!makeflow_node_check_file_was_created(d, n, f))
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
		int prep_failed = makeflow_clean_prep_fail_dir(d, n, remote_queue);
		if (prep_failed) {
			fprintf(stderr, "rule %d failed, cannot move outputs\n",
					n->nodeid);
		}

		/* Clean files created in node. Clean existing and expected and record deletion. */
		list_first_item(outputs);
		while((f = list_next_item(outputs))) {
			if(f->state == DAG_FILE_STATE_EXPECT) {
				makeflow_clean_failed_file(d, n, remote_queue,
						f, prep_failed, 1, storage_allocation);
			} else {
				makeflow_clean_failed_file(d, n, remote_queue,
						f, prep_failed, 0, storage_allocation);
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
		makeflow_clean_rm_fail_dir(d, n, remote_queue);

		/* Mark source files that have been used by this node */
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))) {
			f->reference_count+= -1;
			if(f->reference_count == 0 && f->state == DAG_FILE_STATE_EXISTS){
				makeflow_log_file_state_change(d, f, DAG_FILE_STATE_COMPLETE);
				if(storage_allocation->locked)
					makeflow_clean_file(d, remote_queue, f, 0, storage_allocation);
			}
		}

		/* store node into archiving directory  */
		if (d->should_write_to_archive) {
			printf("archiving node within archiving directory\n");
			struct list *input_list = NULL;
			char *input_files = NULL, *output_files = NULL, *command = NULL;

			makeflow_node_expand(n, queue, &input_list, &outputs, &input_files, &output_files, &command);

			makeflow_archive_populate(d, n, command, input_list, outputs, info);

			free(command);
			free(input_files);
			free(output_files);
			list_delete(input_list);
		}

		if(makeflow_alloc_use_space(storage_allocation, n) &&
			makeflow_alloc_release_space(storage_allocation, n, 0, MAKEFLOW_ALLOC_COMMIT)) {
			makeflow_log_alloc_event(d, storage_allocation);
		} else {
			printf("Unable to commit enough space for execution");
		}
		makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
	list_delete(outputs);
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

			if(f->source) {
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
				debug(D_ERROR, "Remote renaming is not supported with -Tlocal or LOCAL execution. Rule %d (line %d).\n", n->nodeid, n->linenum);
				error = 1;
				break;
			} else if (!batch_queue_supports_feature(remote_queue, "remote_rename")) {
				debug(D_ERROR, "Remote renaming is not supported on selected batch system. Rule %d (line %d).\n", n->nodeid, n->linenum);
				error = 1;
				break;
			}
		}

		if(!batch_queue_supports_feature(remote_queue, "absolute_path") && !n->local_job){
			list_first_item(n->source_files);
			while((f = list_next_item(n->source_files)) && !error) {
				const char *remotename = dag_node_get_remote_name(n, f->filename);
				if (makeflow_file_on_sharedfs(f->filename)) {
					if (remotename)
						fatal("Remote renaming for %s is not supported on a shared filesystem",
							f->filename);
					continue;
				}
				if((remotename && *remotename == '/') || (*f->filename == '/' && !remotename)) {
					debug(D_ERROR, "Absolute paths are not supported on selected batch system. Rule %d (line %d).\n", n->nodeid, n->linenum);
					error = 1;
					break;
				}
			}

			list_first_item(n->target_files);
			while((f = list_next_item(n->target_files)) && !error) {
				const char *remotename = dag_node_get_remote_name(n, f->filename);
				if (makeflow_file_on_sharedfs(f->filename)) {
					if (remotename)
						fatal("Remote renaming for %s is not supported on a shared filesystem",
							f->filename);
					continue;
				}
				if((remotename && *remotename == '/') || (*f->filename == '/' && !remotename)) {
					debug(D_ERROR, "Absolute paths are not supported on selected batch system. Rule %d (line %d).\n", n->nodeid, n->linenum);
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
    timestamp_t last_time = timestamp_get();
    timestamp_t start = timestamp_get();
    int first_report = 1;
	// Relevant to GC and allocations
	int cleaned_completed_jobs = 0;

    //reporting to catalog
    if(catalog_reporting_on){
        makeflow_catalog_summary(d, project, batch_queue_type, start);
    }

	while(!makeflow_abort_flag) {
		did_find_archived_job = 0;
		makeflow_dispatch_ready_jobs(d);
		/*
			We continue the loop under 4 conditions:
			1. We have local jobs running
			2. We have remote jobs running
			3. We have archival jobs to be found (See Note)
			4. We have cleaned completed jobs to ensure allocated jobs can run

		Note:
 		Due to the fact that archived jobs are never "run", no local or remote jobs are added
 		to the remote or local job table if all ready jobs were found within the archive.
 		Thus makeflow_dispatch_ready_jobs must run at least once more if an archived job was found.
 		*/
		if(dag_local_jobs_running(d)==0 && 
			dag_remote_jobs_running(d)==0 && 
			did_find_archived_job == 0 && 
			cleaned_completed_jobs == 0 )
			break;

		cleaned_completed_jobs = 0;

		if(dag_remote_jobs_running(d)) {
			int tmp_timeout = 5;
			jobid = batch_job_wait_timeout(remote_queue, &info, time(0) + tmp_timeout);
			if(jobid > 0) {
				cleaned_completed_jobs = 1;
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
				cleaned_completed_jobs = 1;
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
				n = itable_remove(d->local_job_table, jobid);
				if(n)
					makeflow_node_complete(d, n, local_queue, &info);
			}
		}

		/* Make periodic report to catalog. */
                timestamp_t now = timestamp_get();
                if(catalog_reporting_on && (((now-last_time) > (60 * 1000 * 1000)) || first_report==1)){ //if we are in reporting mode, and if either it's our first report, or 1 min has transpired
			makeflow_catalog_summary(d, project,batch_queue_type,start);
			last_time = now;
			first_report = 0;
                }

		/* Rather than try to garbage collect after each time in this
		 * wait loop, perform garbage collection after a proportional
		 * amount of tasks have passed. */
		makeflow_gc_barrier--;
		if(makeflow_gc_method != MAKEFLOW_GC_NONE && makeflow_gc_barrier == 0) {
			makeflow_gc(d, remote_queue, makeflow_gc_method, makeflow_gc_size, makeflow_gc_count, storage_allocation);
			makeflow_gc_barrier = MAX(d->nodeid_counter * makeflow_gc_task_ratio, 1);
		}
	}

	/* Always make final report to catalog when workflow ends. */

	if(catalog_reporting_on){
		makeflow_catalog_summary(d, project,batch_queue_type,start);
	}

	if(makeflow_abort_flag) {
		makeflow_abort_all(d);
	} else {
		if(!makeflow_failed_flag && makeflow_gc_method != MAKEFLOW_GC_NONE) {
			makeflow_gc(d,remote_queue,MAKEFLOW_GC_ALL,0,0, storage_allocation);
		}
	}
}

/*
Signal handler to catch abort signals.  Note that permissible actions in signal handlers are very limited, so we emit a message to the terminal and update a global variable noticed by makeflow_run.
*/

static void handle_abort(int sig)
{
	int fd = open("/dev/tty", O_WRONLY);
	if (fd >= 0) {
		char buf[256];
		snprintf(buf, sizeof(buf), "received signal %d (%s), cleaning up remote jobs and files...\n",sig,strsignal(sig));
		write(fd, buf, strlen(buf));
		close(fd);
	}

	makeflow_abort_flag = 1;

}

static void set_archive_directory_string(char **archive_directory, char *option_arg)
{
	if (*archive_directory != NULL) {
		// need to free archive directory to avoid memory leak since it has already been set once
		free(*archive_directory);
	}
	if (option_arg) {
		*archive_directory = xxstrdup(option_arg);
	} else {
		char *uid = xxmalloc(10);
		sprintf(uid, "%d",  getuid());
		*archive_directory = xxmalloc(sizeof(MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY) + 20 * sizeof(char));
		sprintf(*archive_directory, "%s%s", MAKEFLOW_ARCHIVE_DEFAULT_DIRECTORY, uid);
		free(uid);
	}
}

static void show_help_run(const char *cmd)
{
		/* Stars indicate 80-column limit.  Try to keep things within 79 columns.       */
	        /********************************************************************************/
	printf("Use: ./makeflow [options] <dagfile>\n");
	printf("Basic Options:\n");
	printf(" -c,--clean=<type>              Clean up logfile and all temporary files.\n");
	printf(" -d,--debug=<subsystem>         Enable debugging for this subsystem\n");
	printf(" -o,--debug-file=<file>         Send debugging to this file.\n");
	printf("    --debug-rotate-max=<bytes>  Rotate debug file once it reaches this size.\n");
	printf(" -T,--batch-type=<type>         Select batch system: %s\n",batch_queue_type_string());
	printf(" -v,--version                   Show version string\n");
	printf(" -h,--help                      Show this help screen.\n");
	        /********************************************************************************/
	printf("\nWorkflow Handling:\n");
	printf(" -a,--advertise                 Advertise workflow status to the global catalog.\n");
	printf(" -L,--batch-log=<logfile>       Use this file for the batch system log.\n");
	printf(" -m,--email=<email>             Send summary of workflow to this email at end\n");
	printf("    --json                      Use JSON format for the workflow specification.\n");
	printf("    --jx                        Use JX format for the workflow specification.\n");
	printf("    --jx-args=<file>            Evaluate the JX input with keys and values in file defined as variables.\n");
	printf("    --jx-context=<file>         Deprecated. Equivalent to --jx-args.\n");
	printf("    --jx-define=<VAR>=<EXPR>   Set the JX variable VAR to the JX expression EXPR.\n");
	printf("    --log-verbose               Add node id symbol tags in the makeflow log.\n");
	printf(" -j,--max-local=<#>             Max number of local jobs to run at once.\n");
	printf(" -J,--max-remote=<#>            Max number of remote jobs to run at once.\n");
	printf(" -l,--makeflow-log=<logfile>    Use this file for the makeflow log.\n");
	printf(" -R,--retry                     Retry failed batch jobs up to 5 times.\n");
	printf(" -r,--retry-count=<n>           Retry failed batch jobs up to n times.\n");
	printf(" -S,--submission-timeout=<#>    Time to retry failed batch job submission.\n");
	printf(" -f,--summary-log=<file>        Write summary of workflow to this file at end.\n");
	        /********************************************************************************/
	printf("\nData Handling:\n");
	printf("    --archive=<dir>             Archive job outputs in <dir> for future reuse.\n");
	printf("    --archive-read=<dir>        Same as --archive, but read-only.\n");
	printf("    --archive-write=<dir>       Same as --archive, but write-only.\n");
	printf(" -A,--disable-afs-check         Disable the check for AFS. (experts only.)\n");
	printf("    --cache=<dir>               Use this dir to cache downloaded mounted files.\n");
	printf(" -X,--change-directory=<dir>    Change to <dir> before executing the workflow.\n");
	printf(" -g,--gc=<type>                 Enable garbage collection. (ref_cnt|on_demand|all)\n");
	printf("    --gc-size=<int>             Set disk size to trigger GC. (on_demand only)\n");
	printf(" -G,--gc-count=<int>            Set number of files to trigger GC. (ref_cnt only)\n");
	printf("    --mounts=<mountfile>        Use this file as a mountlist.\n");
	printf("    --skip-file-check           Do not check for file existence before running.\n");
	printf("    --shared-fs=<dir>           Assume that <dir> is in a shared filesystem.\n");
	printf("    --storage-limit             Set storage limit for Makeflow (default is off)\n");
	printf("    --storage-print=<file>      Print storage limit calculated by Makeflow\n");
	printf("    --wait-for-files-upto=<n>   Wait up to <n> seconds for files to be created.\n");
	printf(" -z,--zero-length-error         Consider zero-length files to be erroneous.\n");
	        /********************************************************************************/
	printf("\nWork Queue Options:\n");
	printf(" -C,--catalog-server=<hst:port> Select alternate catalog server.\n");
	printf("    --password                  Password file for authenticating workers.\n");
	printf(" -p,--port=<port>               Port number to use with Work Queue.\n");
	printf(" -Z,--port-file=<file>          Select port at random and write it to this file.\n");
	printf(" -P,--priority=<integer>        Priority. Higher the value, higher the priority.\n");
	printf(" -N,--project-name=<project>    Set the Work Queue project name.\n");
	printf(" -F,--wq-fast-abort=<#>         Set the Work Queue fast abort multiplier.\n");
	printf(" -t,--wq-keepalive-timeout=<#>  Work Queue keepalive timeout. (default: 30s)\n");
	printf(" -u,--wq-keepalive-interval=<#> Work Queue keepalive interval. (default: 120s)\n");
	printf(" -W,--wq-schedule=<mode>        Work Queue scheduling algor. (time|files|fcfs)\n");
	printf(" --work-queue-preferred-connection    Preferred connection: by_ip | by_hostname\n");
	        /********************************************************************************/
	printf("\nBatch System Options:\n");
	printf("    --amazon-config             Amazon config file from makeflow_ec2_setup.\n");
	printf(" -B,--batch-options=<options>   Add these options to all batch submit files.\n");
	printf("    --disable-cache             Disable batch system caching.\n");
	printf("    --local-cores=#             Max number of local cores to use.\n");
	printf("    --local-memory=#            Max amount of local memory (MB) to use.\n");
	printf("    --local-disk=#              Max amount of local disk (MB) to use.\n");
	printf("    --working-dir=<dir|url>     Working directory for the batch system.\n");
	        /********************************************************************************/
	printf("\nContainers and Wrappers:\n");
	printf(" --docker=<image>               Run each task using the named Docker image.\n");
	printf(" --docker-tar=<tar file>        Load docker image from this tar file.\n");
	printf(" --singularity=<image>          Run each task using this Singularity image.\n");
	printf(" --umbrella-spec=<file>         Run each task using this Umbrella spec.\n");
	printf(" --umbrella-binary=<file>       Path to Umbrella binary.\n");
	printf(" --umbrella-log-prefix=<string> Umbrella log file prefix\n");
	printf(" --umbrella-mode=<mode>         Umbrella execution mode. (default is local)\n");
	printf(" --wrapper=<cmd>                Wrap all commands with this prefix.\n");
	printf(" --wrapper-input=<cmd>          Wrapper command requires this input file.\n");
	printf(" --wrapper-output=<cmd>         Wrapper command produces this output file.\n");
	printf(" --enforcement                  Enforce access to only named inputs/outputs.\n");
	printf(" --parrot-path=<path>           Path to parrot_run for --enforcement.\n");
	printf(" --mesos-master=<hostname:port> Mesos master address and port\n");
	printf(" --mesos-path=<path>            Path to mesos python2 site-packages.\n");
	printf(" --mesos-preload=<path>         Path to libraries needed by Mesos.\n");
	        /********************************************************************************/

	printf("\nResource Monitoring Options:\n");
	printf(" --monitor=<dir>                Enable resource monitor, write logs to <dir>\n");
	printf(" --monitor-interval=<#>         Set monitor interval, in seconds. (default: 1s)\n");
	printf(" --monitor-with-time-series     Enable monitor time series.\n");
	printf(" --monitor-with-opened-files    Enable monitoring of opened files.\n");
	printf(" --monitor-log-fmt=<fmt>        Format for monitor logs. (def: resource-rule-%%)\n");
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
	int explicit_local_cores = 0;
	int explicit_local_memory = 0;
	int explicit_local_disk = 0;

	char *logfilename = NULL;
	int port_set = 0;
	timestamp_t runtime = 0;
	int disable_afs_check = 0;
	int should_read_archive = 0;
	int should_write_to_archive = 0;
	timestamp_t time_completed = 0;
	const char *work_queue_keepalive_interval = NULL;
	const char *work_queue_keepalive_timeout = NULL;
	const char *work_queue_master_mode = "standalone";
	const char *work_queue_port_file = NULL;
	double wq_option_fast_abort_multiplier = -1.0;
	const char *amazon_config = NULL;
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
	char *archive_directory = NULL;
	category_mode_t allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;
	shared_fs_list = list_create();
	char *mesos_master = "127.0.0.1:5050/";
	char *mesos_path = NULL;
	char *mesos_preload = NULL;
	int json_input = 0;
	int jx_input = 0;
	struct jx *jx_args = jx_object(NULL);
	struct jx *jx_expr = NULL;
	struct jx *jx_tmp = NULL;
	uint64_t storage_limit = 0;
	char *storage_print = NULL;

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
		LONG_OPT_CACHE,
		LONG_OPT_DEBUG_ROTATE_MAX,
		LONG_OPT_DISABLE_BATCH_CACHE,
		LONG_OPT_DOT_CONDENSE,
		LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME,
		LONG_OPT_GC_SIZE,
		LONG_OPT_LOCAL_CORES,
		LONG_OPT_LOCAL_MEMORY,
		LONG_OPT_LOCAL_DISK,
		LONG_OPT_MONITOR,
		LONG_OPT_MONITOR_INTERVAL,
		LONG_OPT_MONITOR_LOG_NAME,
		LONG_OPT_MONITOR_OPENED_FILES,
		LONG_OPT_MONITOR_TIME_SERIES,
		LONG_OPT_MOUNTS,
		LONG_OPT_STORAGE_LIMIT,
		LONG_OPT_STORAGE_PRINT,
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
		LONG_OPT_AMAZON_CONFIG,
		LONG_OPT_JSON,
		LONG_OPT_JX,
		LONG_OPT_JX_ARGS,
		LONG_OPT_JX_DEFINE,
		LONG_OPT_SKIP_FILE_CHECK,
		LONG_OPT_UMBRELLA_BINARY,
		LONG_OPT_UMBRELLA_LOG_PREFIX,
		LONG_OPT_UMBRELLA_MODE,
		LONG_OPT_UMBRELLA_SPEC,
		LONG_OPT_ALLOCATION_MODE,
		LONG_OPT_ENFORCEMENT,
		LONG_OPT_PARROT_PATH,
		LONG_OPT_SINGULARITY,
		LONG_OPT_SHARED_FS,
		LONG_OPT_ARCHIVE,
		LONG_OPT_ARCHIVE_READ_ONLY,
		LONG_OPT_ARCHIVE_WRITE_ONLY,
		LONG_OPT_MESOS_MASTER,
		LONG_OPT_MESOS_PATH,
		LONG_OPT_MESOS_PRELOAD
	};

	static const struct option long_options_run[] = {
		{"advertise", no_argument, 0, 'a'},
		{"allocation", required_argument, 0, LONG_OPT_ALLOCATION_MODE},
		{"auth", required_argument, 0, LONG_OPT_AUTH},
		{"batch-log", required_argument, 0, 'L'},
		{"batch-options", required_argument, 0, 'B'},
		{"batch-type", required_argument, 0, 'T'},
		{"cache", required_argument, 0, LONG_OPT_CACHE},
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
		{"help", no_argument, 0, 'h'},
		{"local-cores", required_argument, 0, LONG_OPT_LOCAL_CORES},
		{"local-memory", required_argument, 0, LONG_OPT_LOCAL_MEMORY},
		{"local-disk", required_argument, 0, LONG_OPT_LOCAL_DISK},
		{"makeflow-log", required_argument, 0, 'l'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
		{"monitor", required_argument, 0, LONG_OPT_MONITOR},
		{"monitor-interval", required_argument, 0, LONG_OPT_MONITOR_INTERVAL},
		{"monitor-log-name", required_argument, 0, LONG_OPT_MONITOR_LOG_NAME},
		{"monitor-with-opened-files", no_argument, 0, LONG_OPT_MONITOR_OPENED_FILES},
		{"monitor-with-time-series",  no_argument, 0, LONG_OPT_MONITOR_TIME_SERIES},
		{"mounts",  required_argument, 0, LONG_OPT_MOUNTS},
		{"password", required_argument, 0, LONG_OPT_PASSWORD},
		{"port", required_argument, 0, 'p'},
		{"port-file", required_argument, 0, 'Z'},
		{"priority", required_argument, 0, 'P'},
		{"project-name", required_argument, 0, 'N'},
		{"retry", no_argument, 0, 'R'},
		{"retry-count", required_argument, 0, 'r'},
		{"shared-fs", required_argument, 0, LONG_OPT_SHARED_FS},
		{"show-output", no_argument, 0, 'O'},
		{"storage-limit", required_argument, 0, LONG_OPT_STORAGE_LIMIT},
		{"storage-print", required_argument, 0, LONG_OPT_STORAGE_PRINT},
		{"submission-timeout", required_argument, 0, 'S'},
		{"summary-log", required_argument, 0, 'f'},
		{"tickets", required_argument, 0, LONG_OPT_TICKETS},
		{"version", no_argument, 0, 'v'},
		{"log-verbose", no_argument, 0, LONG_OPT_LOG_VERBOSE_MODE},
		{"working-dir", required_argument, 0, LONG_OPT_WORKING_DIR},
		{"skip-file-check", no_argument, 0, LONG_OPT_SKIP_FILE_CHECK},
		{"umbrella-binary", required_argument, 0, LONG_OPT_UMBRELLA_BINARY},
		{"umbrella-log-prefix", required_argument, 0, LONG_OPT_UMBRELLA_LOG_PREFIX},
		{"umbrella-mode", required_argument, 0, LONG_OPT_UMBRELLA_MODE},
		{"umbrella-spec", required_argument, 0, LONG_OPT_UMBRELLA_SPEC},
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
		{"amazon-config", required_argument, 0, LONG_OPT_AMAZON_CONFIG},
		{"json", no_argument, 0, LONG_OPT_JSON},
		{"jx", no_argument, 0, LONG_OPT_JX},
		{"jx-context", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-args", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-define", required_argument, 0, LONG_OPT_JX_DEFINE},
		{"enforcement", no_argument, 0, LONG_OPT_ENFORCEMENT},
		{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
        {"singularity", required_argument, 0, LONG_OPT_SINGULARITY},
		{"archive", optional_argument, 0, LONG_OPT_ARCHIVE},
		{"archive-read", optional_argument, 0, LONG_OPT_ARCHIVE_READ_ONLY},
		{"archive-write", optional_argument, 0, LONG_OPT_ARCHIVE_WRITE_ONLY},
		{"mesos-master", required_argument, 0, LONG_OPT_MESOS_MASTER},
		{"mesos-path", required_argument, 0, LONG_OPT_MESOS_PATH},
		{"mesos-preload", required_argument, 0, LONG_OPT_MESOS_PRELOAD},
		{0, 0, 0, 0}
	};

	static const char option_string_run[] = "aAB:c::C:d:Ef:F:g:G:hj:J:l:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:X:zZ:";

	while((c = getopt_long(argc, argv, option_string_run, long_options_run, NULL)) >= 0) {
		switch (c) {
			case 'a':
				work_queue_master_mode = "catalog";
				break;
			case 'A':
				disable_afs_check = 1;
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
					} else if(strcasecmp(optarg, "cache") == 0){
						clean_mode = MAKEFLOW_CLEAN_CACHE;
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
				} else if(strcasecmp(optarg, "ref_cnt") == 0) {
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
			case LONG_OPT_LOCAL_CORES:
				explicit_local_cores = atoi(optarg);
				break;
			case LONG_OPT_LOCAL_MEMORY:
				explicit_local_memory = atoi(optarg);
				break;
			case LONG_OPT_LOCAL_DISK:
				explicit_local_disk = atoi(optarg);
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
			case LONG_OPT_CACHE:
				mount_cache = xxstrdup(optarg);
				break;
			case LONG_OPT_MOUNTS:
				mountfile = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_CONFIG:
				amazon_config = xxstrdup(optarg);
				break;
			case 'M':
			case 'N':
				free(project);
				project = xxstrdup(optarg);
				work_queue_master_mode = "catalog";
                                catalog_reporting_on = 1; //set to true
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
			case LONG_OPT_SHARED_FS:
				assert(shared_fs_list);
				if (optarg[0] != '/') fatal("Shared fs must be specified as an absolute path");
				list_push_head(shared_fs_list, xxstrdup(optarg));
				break;
			case LONG_OPT_STORAGE_LIMIT:
				storage_limit = string_metric_parse(optarg);
				break;
			case LONG_OPT_STORAGE_PRINT:
				if(storage_print) free(storage_print);
				storage_print = xxstrdup(optarg);
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
				container_image_tar = xxstrdup(optarg);
				break;
                        case LONG_OPT_SINGULARITY:
                                if(!wrapper) wrapper = makeflow_wrapper_create();
                                container_mode = CONTAINER_MODE_SINGULARITY;
                                container_image = xxstrdup(optarg);
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
			case LONG_OPT_JX:
				jx_input = 1;
			case LONG_OPT_JSON:
				json_input = 1;
				break;
			case LONG_OPT_JX_ARGS:
				jx_input = 1;
				jx_expr = jx_parse_file(optarg);
				if (!jx_expr)
						fatal("failed to parse context");
				jx_tmp = jx_eval(jx_expr, NULL);
				jx_delete(jx_expr);
				jx_expr = jx_tmp;
				if (jx_istype(jx_expr, JX_ERROR)) {
						jx_print_stream(jx_expr, stderr);
						fatal("\nError in JX args");
				}
				if (!jx_istype(jx_expr, JX_OBJECT))
						fatal("Args file must contain a JX object");
				jx_tmp = jx_merge(jx_args, jx_expr, NULL);
				jx_delete(jx_expr);
				jx_delete(jx_args);
				jx_args = jx_tmp;
				break;
			case LONG_OPT_JX_DEFINE:
				jx_input = 1;
				s = strchr(optarg, '=');
				if (!s)
						fatal("JX variable must be of the form VAR=EXPR");
				*s = '\0';
				jx_expr = jx_parse_string(s + 1);
				if (!jx_expr)
						fatal("Invalid JX expression");
				jx_insert(jx_args, jx_string(optarg), jx_expr);
				break;
			case LONG_OPT_UMBRELLA_BINARY:
				if(!umbrella) umbrella = makeflow_wrapper_umbrella_create();
				makeflow_wrapper_umbrella_set_binary(umbrella, (const char *)xxstrdup(optarg));
				break;
			case LONG_OPT_UMBRELLA_LOG_PREFIX:
				if(!umbrella) umbrella = makeflow_wrapper_umbrella_create();
				makeflow_wrapper_umbrella_set_log_prefix(umbrella, (const char *)xxstrdup(optarg));
				break;
			case LONG_OPT_UMBRELLA_MODE:
				if(!umbrella) umbrella = makeflow_wrapper_umbrella_create();
				makeflow_wrapper_umbrella_set_mode(umbrella, (const char *)xxstrdup(optarg));
				break;
			case LONG_OPT_UMBRELLA_SPEC:
				if(!umbrella) umbrella = makeflow_wrapper_umbrella_create();
				makeflow_wrapper_umbrella_set_spec(umbrella, (const char *)xxstrdup(optarg));
			case LONG_OPT_MESOS_MASTER:
				mesos_master = xxstrdup(optarg);
				break;
			case LONG_OPT_MESOS_PATH:
				mesos_path = xxstrdup(optarg);
				break;
			case LONG_OPT_MESOS_PRELOAD:
				mesos_preload = xxstrdup(optarg);
				break;
			case LONG_OPT_ARCHIVE:
				should_read_archive = 1;
				should_write_to_archive = 1;
				set_archive_directory_string(&archive_directory, optarg);
				break;
			case LONG_OPT_ARCHIVE_READ_ONLY:
				should_read_archive = 1;
				set_archive_directory_string(&archive_directory, optarg);
				break;
			case LONG_OPT_ARCHIVE_WRITE_ONLY:
				should_write_to_archive = 1;
				set_archive_directory_string(&archive_directory, optarg);
				break;
			default:
				show_help_run(argv[0]);
				return 1;
			case 'X':
				change_dir = optarg;
				break;
			case LONG_OPT_ENFORCEMENT:
				if(!enforcer) enforcer = makeflow_wrapper_create();
				break;
			case LONG_OPT_PARROT_PATH:
				parrot_path = xxstrdup(optarg);
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

	if (enforcer && umbrella) {
		fatal("enforcement and Umbrella are mutually exclusive\n");
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

	if(storage_limit){
		storage_allocation = makeflow_alloc_create(-1, NULL, storage_limit, 1);
		makeflow_gc_method = MAKEFLOW_GC_ALL;
	} else {
		storage_allocation = makeflow_alloc_create(-1, NULL, 0, 0);
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
	if (json_input || jx_input) {
		struct jx *dag = jx_parse_file(dagfile);
		if (!dag) fatal("failed to parse dagfile");
		if (jx_input) {
			jx_tmp = jx_eval(dag, jx_args);
			jx_delete(dag);
			jx_delete(jx_args);
			dag = jx_tmp;
		}
		d = dag_from_jx(dag);
		jx_delete(dag);
		// JX doesn't really use errno, so give something generic
		errno = EINVAL;
	} else {
		d = dag_from_file(dagfile);
	}
	if(!d) {
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

	d->allocation_mode = allocation_mode;

	/* Measure resources available for local job execution. */
	local_resources = rmsummary_create(-1);
	makeflow_local_resources_measure(local_resources);

	if(explicit_local_cores)  local_resources->cores = explicit_local_cores;
	if(explicit_local_memory) local_resources->memory = explicit_local_memory;
	if(explicit_local_disk)   local_resources->disk = explicit_local_disk;

	makeflow_local_resources_print(local_resources);

	/* Environment variables override explicit settings for maximum jobs. */
	s = getenv("MAKEFLOW_MAX_REMOTE_JOBS");
	if(s) {
		explicit_remote_jobs_max = MIN(explicit_remote_jobs_max, atoi(s));
	}

	s = getenv("MAKEFLOW_MAX_LOCAL_JOBS");
	if(s) {
		explicit_local_jobs_max = MIN(explicit_local_jobs_max, atoi(s));
	}

	/*
	Handle the confusing case of specifying local/remote max
	jobs when the job type is LOCAL.  Take either option to mean
	both, use the minimum if both are set, and the number of cores
	if neither is set.
	*/

	if(batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
		int j;
		if(explicit_remote_jobs_max && !explicit_local_jobs_max) {
			j = explicit_remote_jobs_max;
		} else if(explicit_local_jobs_max && !explicit_remote_jobs_max) {
			j = explicit_local_jobs_max;
		} else if(explicit_local_jobs_max && explicit_remote_jobs_max) {
			j = MIN(explicit_local_jobs_max,explicit_remote_jobs_max);
		} else {
			j = local_resources->cores;
		}
		local_jobs_max = remote_jobs_max = j;

	} else {
		/* We are using a separate local and remote queue, so set them separately. */

		if(explicit_local_jobs_max) {
			local_jobs_max = explicit_local_jobs_max;
		} else {
			local_jobs_max = local_resources->cores;
		}

		if(explicit_remote_jobs_max) {
			remote_jobs_max = explicit_remote_jobs_max;
		} else {
			if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE) {
				remote_jobs_max = 10 * MAX_REMOTE_JOBS_DEFAULT;
			} else {
				remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
			}
		}
		printf("max running remote jobs: %d\n",remote_jobs_max);
	}

	printf("max running local jobs: %d\n",local_jobs_max);

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
		batch_queue_set_option(remote_queue, "mesos-preload", mesos_preload);
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
	batch_queue_set_option(remote_queue, "password", work_queue_password);
	batch_queue_set_option(remote_queue, "master-mode", work_queue_master_mode);
	batch_queue_set_option(remote_queue, "name", project);
	batch_queue_set_option(remote_queue, "priority", priority);
	batch_queue_set_option(remote_queue, "keepalive-interval", work_queue_keepalive_interval);
	batch_queue_set_option(remote_queue, "keepalive-timeout", work_queue_keepalive_timeout);
	batch_queue_set_option(remote_queue, "caching", cache_mode ? "yes" : "no");
	batch_queue_set_option(remote_queue, "wait-queue-size", wq_wait_queue_size);
	batch_queue_set_option(remote_queue, "amazon-config", amazon_config);
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

	/* Set dag_node->umbrella_spec */
	if(!clean_mode) {
		struct dag_node *cur;
		cur = d->nodes;
		while(cur) {
			struct dag_variable_lookup_set s = {d, cur->category, cur, NULL};
			char *spec = NULL;
			spec = dag_variable_lookup_string("SPEC", &s);
			if(spec) {
				debug(D_MAKEFLOW_RUN, "setting dag_node->umbrella_spec (rule %d) from the makefile ...\n", cur->nodeid);
				dag_node_set_umbrella_spec(cur, xxstrdup(spec));
			} else if(umbrella && umbrella->spec) {
				debug(D_MAKEFLOW_RUN, "setting dag_node->umbrella_spec (rule %d) from the --umbrella_spec option ...\n", cur->nodeid);
				dag_node_set_umbrella_spec(cur, umbrella->spec);
			}
			free(spec);
			cur = cur->next;
		}

		debug(D_MAKEFLOW_RUN, "makeflow_wrapper_umbrella_preparation...\n");
		// When the user specifies umbrella specs in a makefile, but does not use any `--umbrella...` option,
		// an umbrella wrapper was created to hold the default values for umbrella-related setttings such as
		// log_prefix and default umbrella execution engine.
		if(!umbrella) umbrella = makeflow_wrapper_umbrella_create();
		makeflow_wrapper_umbrella_preparation(umbrella, d);
	}

	if(enforcer) {
		makeflow_wrapper_enforcer_init(enforcer, parrot_path);
	}

	makeflow_parse_input_outputs(d);

	makeflow_prepare_node_sizes(d, storage_print);

	makeflow_prepare_nested_jobs(d);

	if (change_dir)
		chdir(change_dir);

	if(!disable_afs_check && (batch_queue_type==BATCH_QUEUE_TYPE_CONDOR || container_mode==CONTAINER_MODE_DOCKER) ) {
		char *cwd = path_getcwd();
		if(!strncmp(cwd, "/afs", 4)) {
			fprintf(stderr,"error: The working directory is '%s'\n", cwd);
			if(batch_queue_type==BATCH_QUEUE_TYPE_CONDOR) {
				fprintf(stderr,"This won't work because Condor is not able to write to files in AFS.\n");
			}
			if(container_mode==CONTAINER_MODE_DOCKER) {
				fprintf(stderr,"This won't work because Docker cannot mount an AFS directory.\n");
			}
			fprintf(stderr,"Instead, run your workflow from a local disk like /tmp.");
			fprintf(stderr,"Or, use the Work Queue batch system with -T wq.\n");
			free(cwd);
			exit(EXIT_FAILURE);
		}
		free(cwd);
	}

	/* Prepare the input files specified in the mountfile. */
	if(mountfile && !clean_mode) {
		/* check the validity of the mountfile and load the info from the mountfile into the dag */
		printf("checking the consistency of the mountfile ...\n");
		if(makeflow_mounts_parse_mountfile(mountfile, d)) {
			fprintf(stderr, "Failed to parse the mountfile: %s.\n", mountfile);
			free(mountfile);
			return -1;
		}
		free(mountfile);
		use_mountfile = 1;
	}

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

	if(mount_cache) d->cache_dir = mount_cache;

	/* In case when the user uses --cache option to specify the mount cache dir and the log file also has
	 * a cache dir logged, these two dirs must be the same. Otherwise exit.
	 */
	if(makeflow_log_recover(d, logfilename, log_verbose_mode, remote_queue, clean_mode, skip_file_check )) {
		dag_mount_clean(d);
		exit(EXIT_FAILURE);
	}

	/* This check must happen after makeflow_log_recover which may load the cache_dir info into d->cache_dir.
	 * This check must happen before makeflow_mount_install to guarantee that the program ends before any mount is copied if any target is invliad.
	 */
	if(use_mountfile) {
		if(makeflow_mount_check_target(d)) {
			dag_mount_clean(d);
			exit(EXIT_FAILURE);
		}
	}

	if(use_mountfile && !clean_mode) {
		if(makeflow_mounts_install(d)) {
			fprintf(stderr, "Failed to install the dependencies specified in the mountfile!\n");
			dag_mount_clean(d);
			exit(EXIT_FAILURE);
		}
	}

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

	if(batch_queue_supports_feature(remote_queue, "batch_log_transactions")) {
		const char *transactions = batch_queue_get_option(remote_queue, "batch_log_transactions_name");
		f = dag_file_lookup_or_create(d, transactions);
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXPECT);
	}

	if(clean_mode != MAKEFLOW_CLEAN_NONE) {
		printf("cleaning filesystem...\n");
		if(makeflow_clean(d, remote_queue, clean_mode, storage_allocation)) {
			fprintf(stderr, "Failed to clean up makeflow!\n");
			exit(EXIT_FAILURE);
		}

		if(clean_mode == MAKEFLOW_CLEAN_ALL) {
			unlink(logfilename);
		}

		exit(0);
	}

	/* this func call guarantees the mount fields set up from the info of the makeflow log file are cleaned up
	 * even if the user does not use --mounts or -c option.
	 */
	dag_mount_clean(d);

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
		makeflow_wrapper_docker_init(wrapper, container_image, container_image_tar);
	} else if(container_mode == CONTAINER_MODE_SINGULARITY){
		makeflow_wrapper_singularity_init(wrapper, container_image);
	}

	d->archive_directory = archive_directory;
	d->should_read_archive = should_read_archive;
	d->should_write_to_archive = should_write_to_archive;

	makeflow_run(d);

	if(makeflow_failed_flag == 0 && makeflow_nodes_local_waiting_count(d) > 0) {
		makeflow_failed_flag = 1;
		debug(D_ERROR, "There are local jobs that could not be run. Usually this means that makeflow did not have enough local resources to run them.");
	}

	time_completed = timestamp_get();
	runtime = time_completed - runtime;

	makeflow_log_alloc_event(d, storage_allocation);

	if(local_queue)
		batch_queue_delete(local_queue);

	/*
	 * Set the abort and failed flag for batch_job_mesos mode.
	 * Since batch_queue_delete(struct batch_queue *q) will call
	 * batch_queue_mesos_free(struct batch_queue *q), which is defined 
	 * in batch_job/src/batch_job_mesos.c. Then this function will check 
	 * the abort and failed status of the batch_queue and inform 
	 * the makeflow mesos scheduler. 
	 */

	if (batch_queue_type == BATCH_QUEUE_TYPE_MESOS) {
		batch_queue_set_int_option(remote_queue, "batch-queue-abort-flag", (int)makeflow_abort_flag);
		batch_queue_set_int_option(remote_queue, "batch-queue-failed-flag", (int)makeflow_failed_flag);
	}

	batch_queue_delete(remote_queue);

	if(write_summary_to || email_summary_to)
		makeflow_summary_create(d, write_summary_to, email_summary_to, runtime, time_completed, argc, argv, dagfile, remote_queue, makeflow_abort_flag, makeflow_failed_flag );

	/* XXX better to write created files to log, then delete those listed in log. */
	if (container_mode == CONTAINER_MODE_DOCKER) {
            unlink(CONTAINER_DOCKER_SH);
	}else if(container_mode == CONTAINER_MODE_SINGULARITY){
            unlink(CONTAINER_SINGULARITY_SH);
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

	makeflow_log_close(d);

	free(archive_directory);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
