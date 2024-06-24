/*
Copyright (C) 2022 The University of Notre Dame
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
#include "jx_match.h"
#include "jx_parse.h"
#include "jx_getopt.h"
#include "jx_print.h"
#include "jx_eval.h"
#include "create_dir.h"
#include "sha1.h"
#include "tlq_config.h"

#include "dag.h"
#include "dag_node.h"
#include "dag_node_footprint.h"
#include "dag_visitors.h"
#include "parser.h"
#include "parser_jx.h"

#ifdef CCTOOLS_WITH_MPI
#include <mpi.h>
#endif

#include "makeflow_summary.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_mounts.h"
#include "makeflow_catalog_reporter.h"
#include "makeflow_local_resources.h"
#include "makeflow_hook.h"

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

- APIs like work_queue_* and vine_* should be indirectly accessed by setting options
in Batch Job using batch_queue_set_option. See batch_job_work_queue.c for
an example.
*/

#define MAX_REMOTE_JOBS_DEFAULT 100

/*
Flags to control the basic behavior of the Makeflow main loop.
*/
enum job_submit_status {
	JOB_SUBMISSION_HOOK_FAILURE = -1,
	JOB_SUBMISSION_SKIPPED,
	JOB_SUBMISSION_SUBMITTED,
	JOB_SUBMISSION_ABORTED,
	JOB_SUBMISSION_TIMEOUT
};

static sig_atomic_t makeflow_abort_flag = 0;
static int makeflow_failed_flag = 1; // Makeflow fails by default. This is changed at dag start to indicate correct start.
static int makeflow_submit_timeout = 32; // in seconds
static int makeflow_retry_flag = 0;
static int makeflow_retry_max = 5;

/*
Garbage Collection (GC) controls when and where intermediate
files are cleaned up, so as to minimize disk space consumption.
*/

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

/* Makeflow current executable*/
static char makeflow_exe[PATH_MAX];

/*
Makeflow manages two queues of jobs.
The remote_queue represents the cluster or distributed system
in which jobs are run in parallel.  The local_queue represents
local execution via Unix processes.
*/

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

/*
The local_resources describes the total CPU, RAM, DISK
available to run local jobs, so that the machine is not
over-subscribed.
*/

static struct rmsummary *local_resources = 0;

/*
local_jobs_max and remote_jobs_max describe manual limits
set on the number of jobs, so that a million-node workflow
doesn't instantly become a million jobs on the queue unless
the user takes some positive steps to that effect.
*/

static int local_jobs_max = 1;
static int remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;

/*
The project name and manual port number chosen for the 
Work Queue configuration.  A port number of zero indicates
any available port.
*/


static char *project = NULL;
static int port = 0;

/*
Check the size of files after creation.
This option doesn't seem to do much.
*/

static int output_len_check = 0;

/*
If enabled, do not check the filesystem for expected files
before starting the dag, instead proceed under optimistic assumptions.
*/

static int skip_file_check = 0;

/*
Control caching within the underlying batch system, if supported.
May be "task", "workflow", or "worker", or "forever".
*/

static const char *cache_mode = "workflow";

/*
Hack: Enable batch job feature to pass detailed name to batch system.
Would be better implemented as a batch system feature.
*/

extern int batch_job_verbose_jobnames;

/**
Hack: Disable batch job feature which generates and checks heartbeats.
*/

extern int batch_job_disable_heartbeat;

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

/*
Send periodic reports of type "makeflow" to the catalog
server, viewable by the makeflow_status command. 
*/

static int catalog_reporting_on = 0;

/*
Create a status file about makeflow status
*/ 

static int file_status_on = 0;

/*
Store name of file detailing makeflow status.
Default to status.html
*/
static char *file_status_name = "status.html";

/*
Store time interval for status file update.
Default to 60s.
*/
static unsigned int file_status_interval = 60;

/*
Options related to the "mounting" of external data
files at the DAG level.
*/

static char *mountfile = NULL;
static char *mount_cache = NULL;
static int use_mountfile = 0;

/*
Options related to TLQ debugging
*/
static int tlq_port = 0;

/*
Location of SSL key and cert for use by TaskVine or Work Queue
*/

static char *ssl_key_file = 0;
static char *ssl_cert_file = 0;

/*
If enabled, then all environment variables are sent
from the submission site to the job execution site.
*/

static int should_send_all_local_environment = 0;

struct batch_queue * makeflow_get_remote_queue(){
	return remote_queue;
}

struct batch_queue * makeflow_get_local_queue(){
	return local_queue;
}

/*
Gives the queue associated with a job, taking into
account the LOCAL flag in the node.
*/

struct batch_queue * makeflow_get_queue(struct dag_node *n){
	if(n->local_job && local_queue) {
		return local_queue;
	} else {
		return remote_queue;
	}
}

/*
Determines if this is a local job that will consume
local resources, regardless of the batch queue type.
*/

static int is_local_job( struct dag_node *n )
{
	return n->local_job || batch_queue_type==BATCH_QUEUE_TYPE_LOCAL;
}

/*
Consider a node in the dag and convert it into a batch task ready to execute,
with resources environments, and everything prior to calling hooks.
*/

struct batch_task *makeflow_node_to_task(struct dag_node *n, struct batch_queue *queue )
{
	struct batch_task *task = batch_task_create(queue);
	task->taskid = n->nodeid;

	if(n->type==DAG_NODE_TYPE_COMMAND) {

		/* A plain command just gets a command string. */
		batch_task_set_command(task, n->command);

	} else if(n->type==DAG_NODE_TYPE_WORKFLOW) {
		/* A sub-workflow must be expanded into a makeflow invocation */
		buffer_t b;
		buffer_init(&b);

		if (batch_queue_supports_feature(remote_queue, "remote_rename")) {
			const char *basename = path_basename(makeflow_exe);
			makeflow_hook_add_input_file(n->d,task,makeflow_exe,basename,DAG_FILE_TYPE_TEMP);
			buffer_printf(&b, "./%s", basename);
		} else {
			buffer_printf(&b, "%s", makeflow_exe);
		}

		buffer_printf(&b, " -T local %s", n->workflow_file);

		const char *log = dag_node_nested_workflow_filename(n, "makeflowlog");
		buffer_printf(&b, " -l %s", log);
		makeflow_hook_add_output_file(n->d,task,log,log,DAG_FILE_TYPE_TEMP);

		/* Select the workflow language */
		if(n->workflow_is_jx) {
			buffer_printf(&b, " --jx");
		}

		/* Generate the filenames for nested workflows */
		if(n->workflow_args) {
			const char *args_file = dag_node_nested_workflow_filename(n, "args");
			buffer_printf(&b, " --jx-args %s", args_file);
			makeflow_hook_add_input_file(n->d,task,args_file,args_file,DAG_FILE_TYPE_TEMP);
		}

		/* Add resource controls to the sub-workflow, if known. */
		if(n->resources_requested->cores>0) {
			buffer_printf(&b, " --local-cores %s", rmsummary_resource_to_str("cores", n->resources_requested->cores, 0));
		}
		if(n->resources_requested->memory>0) {
			buffer_printf(&b, " --local-memory %s", rmsummary_resource_to_str("memory", n->resources_requested->memory, 0));
		}
		if(n->resources_requested->disk>0) {
			buffer_printf(&b, " --local-disk %s", rmsummary_resource_to_str("disk", n->resources_requested->disk, 0));
		}

		batch_task_add_input_file(task,n->workflow_file,n->workflow_file);
		batch_task_set_command(task, buffer_tostring(&b));
		buffer_free(&b);
	} else {
		fatal("invalid job type %d in dag node (%s)",n->type,n->command);
	}

	/* Add all input and output files to the task */

	struct dag_file *f;
	list_first_item(n->source_files);
	while((f = list_next_item(n->source_files))){
		batch_task_add_input_file(task, f->filename, dag_node_get_remote_name(n, f->filename));
	}

	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))){
		batch_task_add_output_file(task, f->filename, dag_node_get_remote_name(n, f->filename));
	}

	batch_task_set_resources(task, dag_node_dynamic_label(n));

	struct jx *env = dag_node_env_create(n->d, n, should_send_all_local_environment);
	batch_task_set_envlist(task, env);
	jx_delete(env);

	return task;
}

/*
Abort one job in a given batch queue.
*/

static void makeflow_abort_job( struct dag *d, struct dag_node *n, struct batch_queue *q, uint64_t jobid, const char *name )
{
	printf("aborting %s job %" PRIu64 "\n", name, jobid);

	batch_job_remove(q, jobid);
	makeflow_hook_node_abort(n);
	makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);
	makeflow_clean_node(d,q,n);
}

/*
Abort the dag by removing all batch jobs from all queues.
*/

static void makeflow_abort_all(struct dag *d)
{
	uint64_t jobid;
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

/* A few forward prototypes to handle mutually-recursive definitions. */

static void makeflow_node_complete(struct dag *d, struct dag_node *n, struct batch_queue *queue, struct batch_task *task);
static void makeflow_node_reset_by_file( struct dag *d, struct dag_file *f );

/*
Reset all state to cause a node to be re-run.
*/

void makeflow_node_reset( struct dag *d, struct dag_node *n )
{
	/* Nothing to do if the node is still waiting. */

	if(n->state==DAG_NODE_STATE_WAITING) return;

	/* If the node is running, remove the corresponding job. */

	if(n->state == DAG_NODE_STATE_RUNNING) {
		if(n->local_job && local_queue) {
			batch_job_remove(local_queue, n->jobid);
			itable_remove(d->local_job_table, n->jobid);
		} else {
			batch_job_remove(remote_queue, n->jobid);
			itable_remove(d->remote_job_table, n->jobid);
		}
	}

	/* Reset the state associated with the node */

	makeflow_clean_node(d,remote_queue,n);

	/* Put the node back to the waiting state. */

	makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);

	/*
	Reset each descendant of this node as well.
	Note that the recursive behavior is self-limiting b/c each
	reset node is put into the WAITING state, and WAITING
	nodes are not reset.
	*/

	/* For each of my output files... */
	struct dag_file *f;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		/* Reset all nodes that consume that file. */
		makeflow_node_reset_by_file(d,f);
	}
}

/*
Reset all nodes that consume file f.
*/

static void makeflow_node_reset_by_file( struct dag *d, struct dag_file *f )
{
	/* For each node that consumes the file... */
	struct dag_node *n;
	list_first_item(f->needed_by);
	while((n = list_next_item(f->needed_by))) {
		/* Reset that node and its descendants */
		makeflow_node_reset(d,n);
	}
}


/*
Decide whether to reset a node based on batch and file system status. The silent
option was added for to prevent confusing debug output when in clean mode. When
clean_mode is not NONE we silence the node reseting output.
*/

void makeflow_node_decide_reset( struct dag *d, struct dag_node *n, int silent )
{
	if(n->state == DAG_NODE_STATE_WAITING) {
		// The job hasn't run yet, nothing to do.	
	} else if(n->state == DAG_NODE_STATE_RUNNING && !(n->local_job && local_queue) && batch_queue_type == BATCH_QUEUE_TYPE_CONDOR) {
		// It's a Condor job and still out there in the batch system, so note that and keep going.
		if(!silent) fprintf(stderr, "rule still running: %s\n", n->command);
		itable_insert(d->remote_job_table, n->jobid, n);
	} else if(n->state == DAG_NODE_STATE_RUNNING || n->state == DAG_NODE_STATE_FAILED || n->state == DAG_NODE_STATE_ABORTED) {
		// Otherwise, we cannot reconnect to the job, so rerun it
		if(!silent) fprintf(stderr, "will retry failed rule: %s\n", n->command);
		makeflow_node_reset(d,n);
	} else if(n->state==DAG_NODE_STATE_COMPLETE) {
		// The job succeeded, so nothing more to do.
	}
}

/*
Submit one fully formed job, retrying failures up to the makeflow_submit_timeout.
This is necessary because busy batch systems occasionally do not accept a job submission.
*/

static enum job_submit_status makeflow_node_submit_retry( struct batch_queue *queue, struct batch_task *task)
{
	time_t stoptime = time(0) + makeflow_submit_timeout;
	int waittime = 1;
	batch_job_id_t jobid = 0;


	/* Display the fully elaborated command, just like Make does. */
	printf("submitting job: %s\n", task->command);

	/* Hook Returns:
	 *  MAKEFLOW_HOOK_SKIP    : Submit is averted by hook
	 *  HAKEFLOW_HOOK_FAILURE : Hook failed and should not submit
	 *  MAKEFLOW_HOOK_SUCCESS : Hook was successful and should submit */
	int rc = makeflow_hook_batch_submit(task);
	if(rc == MAKEFLOW_HOOK_SKIP){
		return JOB_SUBMISSION_SKIPPED;
	} else if(rc != MAKEFLOW_HOOK_SUCCESS){
		return JOB_SUBMISSION_HOOK_FAILURE;
	}

	while(1) {
		if(makeflow_abort_flag) {
			break;
		}

		/* This will eventually be replaced by submit (queue, task )... */
		char *input_files  = batch_files_to_string(queue, task->input_files);
		char *output_files = batch_files_to_string(queue, task->output_files);
		jobid = batch_job_submit(queue,
								task->command,
								input_files,
								output_files,
								task->envlist,
								task->resources);
		free(input_files);
		free(output_files);

		if(jobid > 0) {
			printf("submitted job %"PRIbjid"\n", jobid);
			task->jobid = jobid;
			return JOB_SUBMISSION_SUBMITTED;
		} else if(jobid<0) {
			fprintf(stderr, "couldn't submit batch job, still trying...\n");
		} else if(jobid==0) {
			fprintf(stderr, "INTERNAL ERROR: %s returned a batch job id of zero!",
				batch_queue_type_to_string(batch_queue_get_type(queue)));
		}

		if(makeflow_abort_flag) {
			break;
		}

		if(time(0) > stoptime) {
			fprintf(stderr, "unable to submit job after %d seconds!\n", makeflow_submit_timeout);
			return JOB_SUBMISSION_TIMEOUT;
		}

		sleep(waittime);
		waittime = MIN(makeflow_submit_timeout, waittime * 2);
	}

	return JOB_SUBMISSION_ABORTED;
}


/*
Submit a node to the appropriate batch system, after materializing
the necessary list of input and output files, and applying all options.
*/

static enum job_submit_status makeflow_node_submit(struct dag *d, struct dag_node *n, const struct rmsummary *resources)
{
	struct batch_queue *queue = makeflow_get_queue(n);

	/* Before setting the batch job options (stored in the "BATCH_OPTIONS"
	 * variable), we must save the previous global queue value, and then
	 * restore it after we submit. */
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *batch_options	= dag_variable_lookup_string("BATCH_OPTIONS", &s);

	char *previous_batch_options = NULL;
	if(batch_queue_get_option(queue, "batch-options"))
		previous_batch_options = xxstrdup(batch_queue_get_option(queue, "batch-options"));

	if(batch_options) {
		debug(D_MAKEFLOW_RUN, "Batch options: %s\n", batch_options);
		batch_queue_set_option(queue, "batch-options", batch_options);
		free(batch_options);
	}

	/* Create task from node information */
	struct batch_task *task = makeflow_node_to_task(n, queue );
	batch_queue_set_int_option(queue, "task-id", task->taskid);
	n->task = task;

	int hook_return = makeflow_hook_node_submit(n, task);
	if (hook_return != MAKEFLOW_HOOK_SUCCESS){
		makeflow_failed_flag = 1;
		return JOB_SUBMISSION_HOOK_FAILURE;
	}

	/* Logs the expectation of output files. */
	makeflow_log_batch_file_list_state_change(d,task->output_files,DAG_FILE_STATE_EXPECT);

	enum job_submit_status submitted = makeflow_node_submit_retry(queue, task);

	/* Update all of the necessary data structures. */
	switch(submitted) {
		case JOB_SUBMISSION_HOOK_FAILURE:
			debug(D_MAKEFLOW_RUN, "node %d could not be submitted because of a hook failure.", n->nodeid);
			makeflow_log_state_change(d, n, DAG_NODE_STATE_FAILED);
			n->task = NULL;
			batch_task_delete(task);
			makeflow_failed_flag = 1;
			break;
		case JOB_SUBMISSION_SKIPPED:
			debug(D_MAKEFLOW_RUN, "node %d was not submitted because it was already handled.", n->nodeid);
			/* Exited Normally was updated and may have been handled elsewhere (e.g. Archive) */
			if (task->info->exited_normally) {
				makeflow_node_complete(d, n, queue, task);
			}
			break;
		case JOB_SUBMISSION_SUBMITTED:
			debug(D_MAKEFLOW_RUN, "node %d was successfully submitted.", n->nodeid);
			n->jobid = task->jobid;
			/* Not sure if this is necessary/what it does. */
			memcpy(n->resources_allocated, task->resources, sizeof(struct rmsummary));
			makeflow_log_state_change(d, n, DAG_NODE_STATE_RUNNING);

			if(is_local_job(n)) {
				makeflow_local_resources_subtract(local_resources,n);
			}

			if(n->local_job && local_queue) {
				itable_insert(d->local_job_table, n->jobid, n);
			} else {
				itable_insert(d->remote_job_table, n->jobid, n);
			}
			break;
		case JOB_SUBMISSION_ABORTED:
			/* do nothing, as node was aborted before it was submitted. */
			debug(D_MAKEFLOW_RUN, "node %d was not submitted because workflow was aborted.", n->nodeid);
			break;
		case JOB_SUBMISSION_TIMEOUT:
			debug(D_MAKEFLOW_RUN, "node %d submission timed-out, retrying later.", n->nodeid);
			/* do nothing, and let other rules to be waited/submitted. */
			break;
	}

	/* Restore old batch job options. */
	if(previous_batch_options) {
		batch_queue_set_option(queue, "batch-options", previous_batch_options);
		free(previous_batch_options);
	}

	return submitted;
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

	/* If all makeflow checks pass for this node we will
	return the result of the hooks, which will be 1 if all pass
	and 0 if any fail. */
	int rc = makeflow_hook_node_check(n, makeflow_get_queue(n));
	if(rc == MAKEFLOW_HOOK_FAILURE){
		makeflow_failed_flag = 1;
	}
	return (rc == MAKEFLOW_HOOK_SUCCESS);
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

int makeflow_nodes_remote_waiting_count(const struct dag *d) {
	int count = 0;

	struct dag_node *n;
	for(n = d->nodes; n; n = n->next) {
		if(n->state == DAG_NODE_STATE_WAITING && !is_local_job(n))
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

	/* When submitting to an external queue if there are no resources
	 * available, such as vms in amazon, then the submission fails with a
	 * timeout. When this occurs, submission_timeout is set to 1, and only
	 * local jobs are tried for submission. Batch jobs are tried again the next
	 * time makeflow_dispatch_ready_jobs is called. This allows batch_job_wait
	 * to be called in-between, which may free some batch resources and allow
	 * job submissions that do not timeout.
	 */
	int submission_timeout = 0;

	for(n = d->nodes; n; n = n->next) {
		if(dag_remote_jobs_running(d) >= remote_jobs_max && dag_local_jobs_running(d) >= local_jobs_max) {
			break;
		}

		const struct rmsummary *resources = dag_node_dynamic_label(n);

		if(makeflow_node_ready(d, n, resources)) {
			if(is_local_job(n) || !submission_timeout) {
				enum job_submit_status status = makeflow_node_submit(d, n, resources);

				if(status == JOB_SUBMISSION_ABORTED) {
					break;
				} else if(status == JOB_SUBMISSION_TIMEOUT) {
					debug(D_MAKEFLOW_RUN, "batch submissions are timing-out. Only submitting local jobs for the rest of this cycle.");
					submission_timeout = 1;
				}
			}
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

static void makeflow_node_complete(struct dag *d, struct dag_node *n, struct batch_queue *queue, struct batch_task *task)
{
	struct batch_file *bf;
	struct dag_file *f;
	int job_failed = 0;

	/* This is intended for changes to the batch_task that need no
		no context from dag_node/dag, such as shared_fs. */
	int rc = makeflow_hook_batch_retrieve(task);
	/* Batch Retrieve returns MAKEFLOW_HOOK_RUN if the node was
	 * run/sidestepped by a hook. Archive is an example. */
	if(rc == MAKEFLOW_HOOK_RUN){
		makeflow_log_state_change(d, n, DAG_NODE_STATE_RUNNING);
	} else if (rc != MAKEFLOW_HOOK_SUCCESS){
		makeflow_failed_flag = 1;
	}

	if(n->state != DAG_NODE_STATE_RUNNING)
		return;

	if(is_local_job(n)) {
		makeflow_local_resources_add(local_resources,n);
	}

	rc = makeflow_hook_node_end(n, task);
	if (rc != MAKEFLOW_HOOK_SUCCESS){
		makeflow_failed_flag = 1;
	}

	if (task->info->exited_normally && task->info->exit_code == 0) {
		list_first_item(n->task->output_files);
		while ((bf = list_next_item(n->task->output_files))) {
			f = dag_file_lookup_or_create(d, bf->outer_name);
			if (!makeflow_node_check_file_was_created(d, n, f)) {
				job_failed = 1;
			}
		}
	} else {
		if(task->info->exited_normally) {
			fprintf(stderr, "%s failed with exit code %d\n", n->command, task->info->exit_code);
		} else {
			fprintf(stderr, "%s crashed with signal %d (%s)\n", n->command, task->info->exit_signal, strsignal(task->info->exit_signal));
		}
		job_failed = 1;
	}

	if(job_failed) {
		/* As integration moves forward batch_task will also be passed. */
		/* If a hook indicates failure here, it is not fatal, but will result
			in a failed task. */
		int hook_success = makeflow_hook_node_fail(n, task);

		makeflow_log_state_change(d, n, DAG_NODE_STATE_FAILED);

		/* Clean files created in node. Clean existing and expected and record deletion. */
		list_first_item(n->task->output_files);
		while((bf = list_next_item(n->task->output_files))) {
			f = dag_file_lookup_or_create(d, bf->outer_name);
			makeflow_clean_file(d, remote_queue, f);
		}

		if(task->info->disk_allocation_exhausted) {
			fprintf(stderr, "\nrule %d failed because it exceeded its loop device allocation capacity.\n", n->nodeid);
			if(n->resources_measured)
			{
				rmsummary_print(stderr, n->resources_measured, /* pprint */ 0, /* extra fields */ NULL);
				fprintf(stderr, "\n");
			}
		}

		if (!hook_success || makeflow_retry_flag || task->info->exit_code == 101) {
			n->failure_count++;
			if (n->failure_count > makeflow_retry_max) {
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
	} else {

		/* Mark source files that have been used by this node */
		list_first_item(task->input_files);
		while((bf = list_next_item(task->input_files))) {
			f = dag_file_lookup_or_create(d, bf->inner_name);
			f->reference_count+= -1;
			if(f->reference_count == 0 && f->state == DAG_FILE_STATE_EXISTS){
				makeflow_log_file_state_change(d, f, DAG_FILE_STATE_COMPLETE);
				rc = makeflow_hook_file_complete(f);
				if (rc != MAKEFLOW_HOOK_SUCCESS){
					makeflow_failed_flag = 1;
				}
			}
		}

		/* node_success is after file_complete to allow for the final state of the
			files to be reflected in the structs. Allows for cleanup or archiving.*/
		rc = makeflow_hook_node_success(n, task);
		if (rc != MAKEFLOW_HOOK_SUCCESS){
			makeflow_failed_flag = 1;
		}

		makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}

	/* Clear TEMP files */
	list_first_item(task->input_files);
	while((bf = list_next_item(task->input_files))) {
		f = dag_file_lookup_or_create(d, bf->inner_name);
		if(f->type == DAG_FILE_TYPE_TEMP){
			makeflow_clean_file(d, makeflow_get_queue(n), f);
		}
	}
}

/*
Check the dag for all files that should exist,
whether provided by the user, or created by 
a prior run that was logged.
*/

static int makeflow_check_files(struct dag *d)
{
	struct stat buf;
	struct dag_file *f;
	char *name;
	int errors = 0;
	int warnings = 0;

	printf("checking files for unexpected changes...  (use --skip-file-check to skip this step)\n");

	hash_table_firstkey(d->files);
	while(hash_table_nextkey(d->files, &name, (void **) &f)) {

		/* Skip special files that are not connected to the DAG nodes. */
		if(!f->created_by && !list_size(f->needed_by)) continue;

		/* Skip any file that should not exist yet. */
		if(!dag_file_should_exist(f)) continue;

		/* Check for the presence of the file. */
		int result = batch_fs_stat(remote_queue, f->filename, &buf );

		if(dag_file_is_source(f)) {
			/* Source files must exist before running */
			if(result<0) {
				printf("error: %s does not exist, and is not created by any rule.\n", f->filename);
				errors++;
			}
		} else {
			/* Intermediate files can be re-created as needed. */
			if(result<0) {
				/* Recreate the file by running its parent. */
				printf("warning: %s was previously created by makeflow, but someone else deleted it!\n", f->filename);
				makeflow_log_file_state_change(d, f, DAG_FILE_STATE_UNKNOWN);
				makeflow_node_reset(d,f->created_by);
				warnings++;
			} else if(!S_ISDIR(buf.st_mode) && difftime(buf.st_mtime, f->creation_logged) > 0) {
				/* Recreate descendants by resetting all nodes that consume this file. */
				printf("warning: %s was previously created by makeflow, but someone else modified it!\n",f->filename);
				makeflow_node_reset_by_file(d,f);
				warnings++;
			}
		}
	}

	if(errors>0 || warnings>0) {
		printf("found %d errors and %d warnings during consistency check.\n", errors,warnings);
	}

	if(errors) {
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
	int error = 0;

	debug(D_MAKEFLOW_RUN, "checking for consistency of batch system support...\n");

	for(n = d->nodes; n; n = n->next) {

		if(itable_size(n->remote_names) > 0){
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
	// Start Catalog at current time
	timestamp_t start = timestamp_get();
	// Last Report is created stall for first reporting.
	timestamp_t last_time = start - (60 * 1000 * 1000);
	
	//reporting to catalog
	if(catalog_reporting_on){
		makeflow_catalog_summary(d, project, batch_queue_type, start);
	}
	
	if(file_status_on){
		makeflow_file_summary(d, project, batch_queue_type, start, file_status_name);
	}
	
	while(!makeflow_abort_flag) {
		makeflow_dispatch_ready_jobs(d);
		/*
			We continue the loop under 3 general conditions:
			1. We have local jobs running
			2. We have remote jobs running
			3. A Hook determined it needed to loop again 
				(e.g. Archived Jobs or Cleaned Jobs)
 		*/
		if(dag_local_jobs_running(d)==0 && 
			dag_remote_jobs_running(d)==0 && 
			(makeflow_hook_dag_loop(d) == MAKEFLOW_HOOK_END) &&
			makeflow_nodes_remote_waiting_count(d) == 0) {
			break;
		}

		if(dag_remote_jobs_running(d)) {
			int tmp_timeout = 5;
			jobid = batch_job_wait_timeout(remote_queue, &info, time(0) + tmp_timeout);
			if(jobid > 0) {
				printf("job %"PRIbjid" completed\n",jobid);
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
				n = itable_remove(d->remote_job_table, jobid);
				if(n){
					// Stop gap until batch_job_wait returns task struct
					batch_task_set_info(n->task, &info);
					makeflow_node_complete(d, n, remote_queue, n->task);
				}
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
				if(n){
					// Stop gap until batch_job_wait returns task struct
					batch_task_set_info(n->task, &info);
					makeflow_node_complete(d, n, local_queue, n->task);
				}
			}
		}

		/* Report to catalog */
		timestamp_t now = timestamp_get();
		/* If in reporting mode and 1 min has transpired */
		if(catalog_reporting_on && ((now-last_time) > (60 * 1000 * 1000))){ 
			makeflow_catalog_summary(d, project,batch_queue_type,start);	
			last_time = now;
		}

		/* Create status file */
		now = timestamp_get();
		/* If status file mode is on and a time interval has transpired */
		if(file_status_on && ((now-last_time) > (file_status_interval * 1000 * 1000))){
			makeflow_file_summary(d, project, batch_queue_type, start, file_status_name);
			last_time = now; 
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

	/* Always make final report to catalog when workflow ends. */
	if(catalog_reporting_on){
		makeflow_catalog_summary(d, project,batch_queue_type,start); 
	}
	
	/* Always make final status file when workflow ends. */
	if(file_status_on){
		makeflow_file_summary(d, project, batch_queue_type, start, file_status_name);
	}

	if(makeflow_abort_flag) {
		makeflow_abort_all(d);
	} else if(!makeflow_failed_flag && makeflow_gc_method != MAKEFLOW_GC_NONE) {
		makeflow_gc(d,remote_queue,MAKEFLOW_GC_ALL,0,0);
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
		string_nformat(buf, sizeof(buf), "received signal %d (%s), cleaning up remote jobs and files...\n",sig,strsignal(sig));
		write(fd, buf, strlen(buf));
		close(fd);
	}

	makeflow_abort_flag = 1;

}

static void show_help_run(const char *cmd)
{
		/* Stars indicate 80-column limit.  Try to keep things within 79 columns.       */
	        /********************************************************************************/
	printf("Use: ./makeflow [options] <dagfile>\n");
	printf("Basic Options:\n");
	printf(" -c,--clean=<type>              Clean up logfile and all temporary files.\n");
	printf("								  all: Cleans all files but inputs (defualt).\n");
	printf("								  outputs: Cleans output files.\n");
	printf("								  intermediates: Cleans intermediate files.\n");
	printf("								  cache: Cleans cache directory.\n");
	printf(" -d,--debug=<subsystem>         Enable debugging for this subsystem.\n");
	printf(" -o,--debug-file=<file>         Send debugging to this file.\n");
	printf("    --debug-rotate-max=<bytes>  Rotate debug file once it reaches this size.\n");
	printf(" -T,--batch-type=<type>         Select batch system: %s\n",batch_queue_type_string());
	printf("    --argv=<file>               Use command line arguments from a JSON file.\n");
	printf(" -v,--version                   Show version string.\n");
	printf(" -h,--help                      Show this help screen.\n");
	        /********************************************************************************/
	printf("\nWorkflow Handling:\n");
	printf(" -a,--advertise                 Advertise workflow status to catalog.\n");
	printf(" -l,--makeflow-log=<logfile>    Use this file for the makeflow log.\n");
	printf(" -L,--batch-log=<logfile>       Use this file for the batch system log.\n");
	printf(" -m,--email=<email>             Send summary of workflow to this email.\n");
	printf("    --json                      Use JSON format for the workflow specification.\n");
	printf("    --jx                        Use JX format for the workflow specification.\n");
	printf("    --jx-args=<file>            File defining JX variables for JX workflow.\n");
	printf("    --jx-define=<VAR>=<EXPR>	Set the JX variable VAR to JX expression EXPR.\n");
	printf("    --log-verbose               Add node id symbol tags in the makeflow log.\n");
	printf(" -j,--max-local=<#>             Max number of local jobs to run at once.\n");
	printf(" -J,--max-remote=<#>            Max number of remote jobs to run at once.\n");
	printf(" -R,--retry                     Retry failed batch jobs up to 5 times.\n");
	printf(" -r,--retry-count=<n>           Retry failed batch jobs up to n times.\n");
	printf("    --send-environment          Send local environment variables for execution.\n");
	printf(" -S,--submission-timeout=<#>    Time to retry failed batch job submission.\n");
	printf(" -f,--summary-log=<file>        Write summary of workflow to this file at end.\n");
	printf("    --file-status=<file>        Write summary of workflow to file periodically.\n");
	printf("    --file-status-interval=<file>	Set time interval for periodic workflow summary.\n");
	        /********************************************************************************/
	printf("\nData Handling:\n");
	printf("    --archive                   Archive and retrieve archived jobs from archive.\n");
	printf("    --archive-s3=<s3_bucket>    Base s3 bucket name (DEFAULT:makeflows3archive).\n");
	printf("    --archive-s3-no-check=<s3b> Blind upload files to s3 (No existence check).\n");
	printf("    --s3-hostname=<s3_hostname> Base s3 hostname. Used for AWS S3.\n");
	printf("    --s3-keyid=<key id>         Access Key for cloud server. Used for AWS S3.\n");
	printf("    --s3-secretkey=<secret key> Secret Key for cloud server. Used for AWS S3.\n");
	printf("    --archive-dir=<dir>         Archive directory(/tmp/makeflow.archive.USERID).\n");
	printf("    --archive-read              Read jobs from archive.\n");
	printf("    --archive-write             Write jobs into archive.\n");
	printf(" -A,--disable-afs-check         Disable the check for AFS. (experts only.)\n");
	printf("    --cache=<dir>               Use this dir to cache downloaded mounted files.\n");
	printf(" -X,--change-directory=<dir>    Change to <dir> before executing the workflow.\n");
	printf(" -g,--gc=<type>                 Enable garbage collector.(ref_cnt|on_demand|all)\n");
	printf("    --gc-size=<int>             Set disk size to trigger GC (on_demand only)\n");
	printf(" -G,--gc-count=<int>            Set number of files to trigger GC.(ref_cnt only)\n");
	printf("    --mounts=<mountfile>        Use this file as a mountlist\n");
	printf("    --skip-file-check           Do not check for file existence before running.\n");
	printf("    --do-not-save-failed-output Disables saving failed nodes to directory.\n"); 
	printf("    --shared-fs=<dir>           Assume that <dir> is in a shared filesystem.\n");
	printf("    --storage-limit=<int>       Set storage limit for Makeflow.(default is off)\n");
	printf("    --storage-type=<type>       Storage limit type(0:MAX|1:MIN|2:OUTPUT|3:OFF)\n");
	printf("    --storage-print=<file>      Print storage limit calculated by Makeflow.\n");
	printf("    --wait-for-files-upto=<n>   Wait up to <n> seconds for files to be created.\n");
	printf(" -z,--zero-length-error         Consider zero-length files to be erroneous.\n");
	        /********************************************************************************/
	printf("\nTaskVine and Work Queue Options:\n");
	printf(" -C,--catalog-server=<hst:port> Select alternate catalog server.\n");
	printf("    --password                  Password file for authenticating workers.\n");
	printf(" -p,--port=<port>               Port number to use with Work Queue.\n");
	printf(" -Z,--port-file=<file>          Select port at random and write it to this file.\n");
	printf(" -P,--priority=<integer>        Priority. Higher the value, higher the priority.\n");
	printf(" -N,--project-name=<project>    Set the Work Queue project name.\n");
	printf("    --cache-mode=never|workflow|forever   Change caching control.\n");
	printf(" -F,--fast-abort=<#>            Set the Work Queue fast abort multiplier.\n");
	printf(" -t,--keepalive-timeout=<#>     Work Queue keepalive timeout. (default: 30s)\n");
	printf(" -u,--keepalive-interval=<#>    Work Queue keepalive interval. (default: 120s)\n");
	printf(" -W,--schedule=<mode>           Work Queue scheduling algor. (time|files|fcfs)\n");
	printf("    --preferred-connection      Preferred connection: by_ip | by_hostname\n");
	        /********************************************************************************/
	printf("\nBatch System Options:\n");
	printf("    --amazon-config=<file>      Amazon EC2 config from makeflow_ec2_setup.\n");
	printf("    --lambda-config=<file>      Lambda config from makeflow_lambda_setup.\n");
	printf("    --amazon-batch-config=<file>Batch config from makeflow_amazon_batch_setup.\n");
	printf("    --amazon-batch-img=<img>    Specify Amazon ECS Image(Used for amazon-batch)\n");
	printf(" -B,--batch-options=<options>   Add these options to all batch submit files.\n");
	printf("    --disable-heartbeat         Disable job heartbeat check.\n");
	printf("    --local-cores=#             Max number of local cores to use.\n");
	printf("    --local-memory=#            Max amount of local memory (MB) to use.\n");
	printf("    --local-disk=#              Max amount of local disk (MB) to use.\n");
	printf("    --safe-submit-mode          Excludes resources at submission.\n");
	printf("                                  (SLURM, TORQUE, and PBS)\n");
	printf("    --verbose-jobnames          Set the job name based on the command.\n");
	printf("    --keep-wrapper-stdout       Do not redirect to /dev/null the stdout file from the batch system.");
	printf("    --ignore-memory-spec        Excludes memory at submission (SLURM).\n");
	printf("    --batch-mem-type=<type>     Specify memory resource type (SGE).\n");
	printf("    --working-dir=<dir|url>     Working directory for the batch system.\n");
	printf("    --mpi-cores=#               Manually set number of cores on each MPI worker. (-T mpi)\n");
	printf("    --mpi-memory=#              Manually set memory (MB) on each MPI worker. (-T mpi)\n");
	        /********************************************************************************/
	printf("\nContainers and Wrappers:\n");
	printf(" --docker=<image>               Run each task using the named Docker image.\n");
	printf(" --docker-tar=<tar file>        Load docker image from this tar file.\n");
	printf(" --docker-opt=<string>          Pass docker command line options.\n");
	printf(" --singularity=<image>          Run each task using Singularity exec with image.\n");
	printf(" --singularity-opt=<string>     Pass singularity command line options.\n");
	printf(" --umbrella-spec=<file>         Run each task using this Umbrella spec.\n");
	printf(" --umbrella-binary=<file>       Path to Umbrella binary.\n");
	printf(" --umbrella-log-prefix=<string> Umbrella log file prefix\n");
	printf(" --umbrella-mode=<mode>         Umbrella execution mode. (default is local)\n");
	printf(" --wrapper=<cmd>                Wrap all commands with this prefix.\n");
	printf(" --wrapper-input=<cmd>          Wrapper command requires this input file.\n");
	printf(" --wrapper-output=<cmd>         Wrapper command produces this output file.\n");
	printf(" --enforcement                  Enforce access to only named inputs/outputs.\n");
	printf(" --parrot-path=<path>           Path to parrot_run for --enforcement.\n");
	printf(" --env-replace-path=<path>      Path to env_replace for --enforcement.\n");
	printf(" --mesos-master=<hostname:port> Mesos manager address and port\n");
	printf(" --mesos-path=<path>            Path to mesos python2 site-packages.\n");
	printf(" --mesos-preload=<path>         Path to libraries needed by Mesos.\n");
	printf(" --k8s-image=<path>             Container image used by kubernetes.\n");
	printf(" --sandbox                      Surround node command with sandbox wrapper.\n");
	printf(" --vc3-builder                  VC3 Builder enabled.\n");
	printf(" --vc3-exe=<file>               VC3 Builder executable location.\n");
	printf(" --vc3-log=<file>               VC3 Builder log name.\n");
	printf(" --vc3-options=<string>         VC3 Builder option string.\n");
	        /********************************************************************************/

	printf("\nResource Monitoring Options:\n");
	printf(" --monitor=<dir>                Enable resource monitor, write logs to <dir>\n");
	printf(" --monitor-exe=<file>           Specify monitor executable.\n");
	printf(" --monitor-interval=<#>         Set monitor interval, in seconds. (default: 1s)\n");
	printf(" --monitor-with-time-series     Enable monitor time series.\n");
	printf(" --monitor-with-opened-files    Enable monitoring of opened files.\n");
	printf(" --monitor-log-fmt=<fmt>        Format for monitor logs.(def: resource-rule-%%)\n");
	printf(" --allocation=<mode>            Specify allocation mode (see manual).\n");
	        /********************************************************************************/
	
	printf("\nTLQ Options:\n");
	printf(" --tlq=<port> Set the port for TLQ URL lookup (-d and -o required).\n");
}

int main(int argc, char *argv[])
{
	int c;
	char *dagfile = NULL;
	char *change_dir = NULL;
	char *batchlogfilename = NULL;
	const char *batch_submit_options = NULL;
	makeflow_clean_depth clean_mode = MAKEFLOW_CLEAN_NONE;
	char *email_summary_to = NULL;
	int explicit_remote_jobs_max = 0;
	int explicit_local_jobs_max = 0;
	int explicit_local_cores = 0;
	int explicit_local_memory = 0;
	int explicit_local_disk = 0;

	/* Set d to NULL to allow for clean exit on failures
	 * prior to dag creation. */
	struct dag *d = NULL; 

	char *logfilename = NULL;
	int port_set = 0;
	timestamp_t runtime = 0;
	int disable_afs_check = 0;
	timestamp_t time_completed = 0;
	const char *option_scheduler = NULL;
	const char *option_keepalive_interval = NULL;
	const char *option_keepalive_timeout = NULL;
	const char *option_manager_mode = "standalone";
	const char *option_port_file = NULL;
	double option_fast_abort_multiplier = -1.0;
	const char *amazon_config = NULL;
	const char *lambda_config = NULL;
	const char *amazon_batch_img = NULL;
	const char *amazon_batch_cfg = NULL;
	const char *priority = NULL;
	char *option_password = NULL;
	char *option_wait_queue_size = 0;
	int did_explicit_auth = 0;
	char *chirp_tickets = NULL;
	char *working_dir = NULL;
	char *option_preferred_connection = NULL;
	char *write_summary_to = NULL;
	char *s;
	int safe_submit = 0;
	int ignore_mem_spec = 0;
	char *debug_file_name = 0;
	char *batch_mem_type = NULL;
	category_mode_t allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;
	char *mesos_manager = "127.0.0.1:5050/";
	char *mesos_path = NULL;
	char *mesos_preload = NULL;
	int keep_wrapper_stdout = 0;

	int mpi_cores = 0;
	int mpi_memory = 0;

	dag_syntax_type dag_syntax = DAG_SYNTAX_MAKE;
	struct jx *jx_args = jx_object(NULL);
	struct jx *base_hook_args = jx_object(NULL);
	struct jx *hook_args = base_hook_args;

	char *k8s_image = NULL;
	extern struct makeflow_hook makeflow_hook_basic_wrapper;
	extern struct makeflow_hook makeflow_hook_docker;
	extern struct makeflow_hook makeflow_hook_enforcement;
	extern struct makeflow_hook makeflow_hook_example;
	extern struct makeflow_hook makeflow_hook_fail_dir;
	/* Using fail directories is on by default */
	int save_failure = 1;
	extern struct makeflow_hook makeflow_hook_resource_monitor;
	extern struct makeflow_hook makeflow_hook_sandbox;
	extern struct makeflow_hook makeflow_hook_shared_fs;
	extern struct makeflow_hook makeflow_hook_singularity;
	extern struct makeflow_hook makeflow_hook_storage_allocation;
	extern struct makeflow_hook makeflow_hook_umbrella;
	extern struct makeflow_hook makeflow_hook_vc3_builder;

	/* save the name of the makeflow executable */
	path_absolute(argv[0], makeflow_exe, 0);

#ifdef HAS_CURL
	extern struct makeflow_hook makeflow_hook_archive;
#endif

#ifdef CCTOOLS_WITH_MPI
	MPI_Init(&argc,&argv);
#endif

	// Enable external functions like fetch and listdir.
	jx_eval_enable_external(1);

	random_init();
	debug_config(argv[0]);
	debug_config_file_size(0);//to set debug file size to "don't delete anything"

	s = getenv("MAKEFLOW_BATCH_QUEUE_TYPE");
	if(s) {
		batch_queue_type = batch_queue_type_from_string(s);
		if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
			fprintf(stderr, "makeflow: unknown batch queue type: %s (from $MAKEFLOW_BATCH_QUEUE_TYPE)\n", s);
			return 1;
		}
	}

	s = getenv("WORK_QUEUE_MANAGER_MODE") ? getenv("WORK_QUEUE_MANAGER_MODE") : getenv("WORK_QUEUE_MASTER_MODE");
	if(s) {
		option_manager_mode = s;
	}

	s = getenv("WORK_QUEUE_NAME");
	if(s) {
		project = xxstrdup(s);
	}
	s = getenv("WORK_QUEUE_FAST_ABORT_MULTIPLIER");
	if(s) {
		option_fast_abort_multiplier = atof(s);
	}

	enum {
		LONG_OPT_AUTH = UCHAR_MAX+1,
		LONG_OPT_ARGV,
		LONG_OPT_CACHE,
		LONG_OPT_CACHE_MODE,
		LONG_OPT_DEBUG_ROTATE_MAX,
		LONG_OPT_DISABLE_BATCH_CACHE,
		LONG_OPT_DOT_CONDENSE,
		LONG_OPT_HOOK_EXAMPLE,
		LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME,
		LONG_OPT_FAIL_DIR,
		LONG_OPT_GC_SIZE,
		LONG_OPT_IGNORE_MEM,
		LONG_OPT_LOCAL_CORES,
		LONG_OPT_LOCAL_MEMORY,
		LONG_OPT_LOCAL_DISK,
		LONG_OPT_BATCH_MEM_TYPE,
		LONG_OPT_MONITOR,
		LONG_OPT_MONITOR_EXE,
		LONG_OPT_MONITOR_INTERVAL,
		LONG_OPT_MONITOR_LOG_NAME,
		LONG_OPT_MONITOR_MEASURE_DIR,
		LONG_OPT_MONITOR_OPENED_FILES,
		LONG_OPT_MONITOR_TIME_SERIES,
		LONG_OPT_MOUNTS,
		LONG_OPT_SAFE_SUBMIT,
		LONG_OPT_SANDBOX,
		LONG_OPT_STORAGE_TYPE,
		LONG_OPT_STORAGE_LIMIT,
		LONG_OPT_STORAGE_PRINT,
		LONG_OPT_PASSWORD,
		LONG_OPT_TICKETS,
		LONG_OPT_VC3,
		LONG_OPT_VC3_EXE,
		LONG_OPT_VC3_LOG,
		LONG_OPT_VC3_OPT,
		LONG_OPT_VERBOSE_PARSING,
		LONG_OPT_LOG_VERBOSE_MODE,
		LONG_OPT_WORKING_DIR,
		LONG_OPT_PREFERRED_CONNECTION,
		LONG_OPT_WAIT_FOR_WORKERS,
		LONG_OPT_WRAPPER,
		LONG_OPT_WRAPPER_INPUT,
		LONG_OPT_WRAPPER_OUTPUT,
		LONG_OPT_DOCKER,
		LONG_OPT_DOCKER_OPT,
		LONG_OPT_DOCKER_TAR,
		LONG_OPT_AMAZON_CONFIG,
		LONG_OPT_LAMBDA_CONFIG,
		LONG_OPT_AMAZON_BATCH_IMG,
		LONG_OPT_AMAZON_BATCH_CFG,
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
		LONG_OPT_ENVREPLACE,
		LONG_OPT_SINGULARITY,
		LONG_OPT_SINGULARITY_OPT,
		LONG_OPT_SHARED_FS,
		LONG_OPT_SSL_KEY,
		LONG_OPT_SSL_CERT,
		LONG_OPT_ARCHIVE,
		LONG_OPT_ARCHIVE_S3,
		LONG_OPT_ARCHIVE_S3_NO_CHECK,
		LONG_OPT_S3_HOSTNAME,
		LONG_OPT_S3_KEYID,
		LONG_OPT_S3_SECRETKEY,
		LONG_OPT_ARCHIVE_DIR,
		LONG_OPT_ARCHIVE_READ,
		LONG_OPT_ARCHIVE_WRITE,
		LONG_OPT_MESOS_MANAGER,
		LONG_OPT_MESOS_PATH,
		LONG_OPT_MESOS_PRELOAD,
		LONG_OPT_SEND_ENVIRONMENT,
		LONG_OPT_K8S_IMG,
		LONG_OPT_VERBOSE_JOBNAMES,
		LONG_OPT_KEEP_WRAPPER_STDOUT,
		LONG_OPT_MPI_CORES,
		LONG_OPT_MPI_MEMORY,
		LONG_OPT_TLQ,
		LONG_OPT_FILE_STATUS,
		LONG_OPT_FILE_STATUS_INTERVAL,
		LONG_OPT_DISABLE_HEARTBEAT
	};

	static const struct option long_options_run[] = {
		{"advertise", no_argument, 0, 'a'},
		{"allocation", required_argument, 0, LONG_OPT_ALLOCATION_MODE},
		{"argv", required_argument, 0, LONG_OPT_ARGV},
		{"auth", required_argument, 0, LONG_OPT_AUTH},
		{"batch-log", required_argument, 0, 'L'},
		{"batch-options", required_argument, 0, 'B'},
		{"batch-type", required_argument, 0, 'T'},
		{"cache", required_argument, 0, LONG_OPT_CACHE},
		{"cache-mode", required_argument, 0, LONG_OPT_CACHE_MODE},
		{"catalog-server", required_argument, 0, 'C'},
		{"clean", optional_argument, 0, 'c'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, LONG_OPT_DEBUG_ROTATE_MAX},
		{"disable-afs-check", no_argument, 0, 'A'},
		{"disable-cache", no_argument, 0, LONG_OPT_DISABLE_BATCH_CACHE},
		{"disable-heartbeat", no_argument, 0, LONG_OPT_DISABLE_HEARTBEAT},
		{"email", required_argument, 0, 'm'},
		{"enable_hook_example", no_argument, 0, LONG_OPT_HOOK_EXAMPLE},
		{"wait-for-files-upto", required_argument, 0, LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME},
		{"gc", required_argument, 0, 'g'},
		{"gc-size", required_argument, 0, LONG_OPT_GC_SIZE},
		{"gc-count", required_argument, 0, 'G'},
		{"help", no_argument, 0, 'h'},
		{"ignore-memory-spec", no_argument, 0, LONG_OPT_IGNORE_MEM},
		{"batch-mem-type", required_argument, 0, LONG_OPT_BATCH_MEM_TYPE},
		{"local-cores", required_argument, 0, LONG_OPT_LOCAL_CORES},
		{"local-memory", required_argument, 0, LONG_OPT_LOCAL_MEMORY},
		{"local-disk", required_argument, 0, LONG_OPT_LOCAL_DISK},
		{"makeflow-log", required_argument, 0, 'l'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
		{"monitor", required_argument, 0, LONG_OPT_MONITOR},
		{"monitor-exe", required_argument, 0, LONG_OPT_MONITOR_EXE},
		{"monitor-interval", required_argument, 0, LONG_OPT_MONITOR_INTERVAL},
		{"monitor-log-name", required_argument, 0, LONG_OPT_MONITOR_LOG_NAME},
		{"monitor-log-fmt", required_argument, 0, LONG_OPT_MONITOR_LOG_NAME},
		{"monitor-measure-dir", no_argument, 0, LONG_OPT_MONITOR_MEASURE_DIR},
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
		{"do-not-save-failed-output", no_argument, 0, LONG_OPT_FAIL_DIR},
		{"safe-submit-mode", no_argument, 0, LONG_OPT_SAFE_SUBMIT},
		{"sandbox", no_argument, 0, LONG_OPT_SANDBOX},
		{"send-environment", no_argument, 0, LONG_OPT_SEND_ENVIRONMENT},
		{"shared-fs", required_argument, 0, LONG_OPT_SHARED_FS},
		{"show-output", no_argument, 0, 'O'}, // Deprecated
		{"ssl-key", required_argument, 0, LONG_OPT_SSL_KEY},
		{"ssl-cert", required_argument, 0, LONG_OPT_SSL_CERT},
		{"storage-type", required_argument, 0, LONG_OPT_STORAGE_TYPE},
		{"storage-limit", required_argument, 0, LONG_OPT_STORAGE_LIMIT},
		{"storage-print", required_argument, 0, LONG_OPT_STORAGE_PRINT},
		{"submission-timeout", required_argument, 0, 'S'},
		{"summary-log", required_argument, 0, 'f'},
		{"tickets", required_argument, 0, LONG_OPT_TICKETS}, // Deprecated ?
		{"vc3-builder", no_argument, 0, LONG_OPT_VC3},
		{"vc3-exe", required_argument, 0, LONG_OPT_VC3_EXE},
		{"vc3-log", required_argument, 0, LONG_OPT_VC3_LOG},
		{"vc3-options", required_argument, 0, LONG_OPT_VC3_OPT},
		{"version", no_argument, 0, 'v'},
		{"log-verbose", no_argument, 0, LONG_OPT_LOG_VERBOSE_MODE},
		{"working-dir", required_argument, 0, LONG_OPT_WORKING_DIR},
		{"skip-file-check", no_argument, 0, LONG_OPT_SKIP_FILE_CHECK},
		{"umbrella-binary", required_argument, 0, LONG_OPT_UMBRELLA_BINARY},
		{"umbrella-log-prefix", required_argument, 0, LONG_OPT_UMBRELLA_LOG_PREFIX},
		{"umbrella-mode", required_argument, 0, LONG_OPT_UMBRELLA_MODE},
		{"umbrella-spec", required_argument, 0, LONG_OPT_UMBRELLA_SPEC},
		{"work-queue-preferred-connection", required_argument, 0, LONG_OPT_PREFERRED_CONNECTION},
		{"fast-abort", required_argument, 0, 'F'},
		{"keepalive-interval", required_argument, 0, 'u'},
		{"keepalive-timeout", required_argument, 0, 't'},
		{"schedule", required_argument, 0, 'W'},
		{"wait-queue-size", required_argument, 0, LONG_OPT_WAIT_FOR_WORKERS}, // Not advertised
		{"wrapper", required_argument, 0, LONG_OPT_WRAPPER},
		{"wrapper-input", required_argument, 0, LONG_OPT_WRAPPER_INPUT},
		{"wrapper-output", required_argument, 0, LONG_OPT_WRAPPER_OUTPUT},
		{"zero-length-error", no_argument, 0, 'z'},
		{"change-directory", required_argument, 0, 'X'}, // Deprecated Still functional not advertised
		{"docker", required_argument, 0, LONG_OPT_DOCKER},
		{"docker-tar", required_argument, 0, LONG_OPT_DOCKER_TAR},
		{"docker-opt", required_argument, 0, LONG_OPT_DOCKER_OPT},
		{"amazon-config", required_argument, 0, LONG_OPT_AMAZON_CONFIG},
		{"lambda-config", required_argument, 0, LONG_OPT_LAMBDA_CONFIG},
		{"amazon-batch-img",required_argument,0,LONG_OPT_AMAZON_BATCH_IMG},
		{"amazon-batch-config",required_argument,0,LONG_OPT_AMAZON_BATCH_CFG},
		{"json", no_argument, 0, LONG_OPT_JSON},
		{"jx", no_argument, 0, LONG_OPT_JX},
		{"jx-context", required_argument, 0, LONG_OPT_JX_ARGS}, // Deprecated
		{"jx-args", required_argument, 0, LONG_OPT_JX_ARGS},
		{"jx-define", required_argument, 0, LONG_OPT_JX_DEFINE},
		{"enforcement", no_argument, 0, LONG_OPT_ENFORCEMENT},
		{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
		{"env-replace-path", required_argument, 0, LONG_OPT_ENVREPLACE},
		{"singularity", required_argument, 0, LONG_OPT_SINGULARITY},
		{"singularity-opt", required_argument, 0, LONG_OPT_SINGULARITY_OPT},
		{"archive", no_argument, 0, LONG_OPT_ARCHIVE},
		{"archive-s3", optional_argument, 0, LONG_OPT_ARCHIVE_S3},
		{"archive-s3-no-check", optional_argument, 0, LONG_OPT_ARCHIVE_S3_NO_CHECK},
		{"s3-hostname",required_argument,0,LONG_OPT_S3_HOSTNAME},
		{"s3-keyid",required_argument,0,LONG_OPT_S3_KEYID},
		{"s3-secretkey",required_argument,0,LONG_OPT_S3_SECRETKEY},
		{"archive-dir", required_argument, 0, LONG_OPT_ARCHIVE_DIR},
		{"archive-read", no_argument, 0, LONG_OPT_ARCHIVE_READ},
		{"archive-write", no_argument, 0, LONG_OPT_ARCHIVE_WRITE},
		{"mesos-master", required_argument, 0, LONG_OPT_MESOS_MANAGER},
		{"mesos-master",  required_argument, 0, LONG_OPT_MESOS_MANAGER}, //same as mesos-master
		{"mesos-path", required_argument, 0, LONG_OPT_MESOS_PATH},
		{"mesos-preload", required_argument, 0, LONG_OPT_MESOS_PRELOAD},
		{"k8s-image", required_argument, 0, LONG_OPT_K8S_IMG},
		{"verbose-jobnames", no_argument, 0, LONG_OPT_VERBOSE_JOBNAMES},
		{"keep-wrapper-stdout", no_argument, 0, LONG_OPT_KEEP_WRAPPER_STDOUT},
		{"mpi-cores", required_argument,0, LONG_OPT_MPI_CORES},
		{"mpi-memory", required_argument,0, LONG_OPT_MPI_MEMORY},
		{"tlq", required_argument, 0, LONG_OPT_TLQ},
		{"file-status", required_argument, 0, LONG_OPT_FILE_STATUS},
		{"file-status-interval", required_argument, 0, LONG_OPT_FILE_STATUS_INTERVAL},
		{0, 0, 0, 0}
	};

	static const char option_string_run[] = "aAB:c::C:d:Ef:F:g:G:hj:J:l:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:X:zZ:";

	while((c = jx_getopt(argc, argv, option_string_run, long_options_run, NULL)) >= 0) {
		switch (c) {
			case 'a':
				option_manager_mode = "catalog";
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
				option_fast_abort_multiplier = atof(optarg);
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
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_log_dir"), jx_string(optarg));
				break;
			case LONG_OPT_MONITOR_EXE:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_exe"), jx_string(optarg));
				break;
			case LONG_OPT_MONITOR_INTERVAL:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_interval"), jx_integer(atoi(optarg)));
				break;
			case LONG_OPT_MONITOR_MEASURE_DIR:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_measure_dir"), jx_integer(1));
				break;
			case LONG_OPT_MONITOR_TIME_SERIES:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_enable_time_series"), jx_integer(1));
				break;
			case LONG_OPT_MONITOR_OPENED_FILES:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_enable_list_files"), jx_integer(1));
				break;
			case LONG_OPT_MONITOR_LOG_NAME:
				if (makeflow_hook_register(&makeflow_hook_resource_monitor, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("resource_monitor_log_format"), jx_string(optarg));
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
			case LONG_OPT_LAMBDA_CONFIG:
				lambda_config = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_BATCH_IMG:
				amazon_batch_img = xxstrdup(optarg);
				break;
			case LONG_OPT_AMAZON_BATCH_CFG:
				amazon_batch_cfg = xxstrdup(optarg);
				break;
			case 'M':
			case 'N':
				free(project);
				project = xxstrdup(optarg);
				option_manager_mode = "catalog";
				catalog_reporting_on = 1; //set to true
				break;
			case 'o':
				debug_file_name = optarg;
				debug_config_file(debug_file_name);
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
				option_keepalive_timeout = optarg;
				break;
			case 'T':
				batch_queue_type = batch_queue_type_from_string(optarg);
				if(batch_queue_type == BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr, "makeflow: unknown batch queue type: %s\n", optarg);
					return 1;
				}
				break;
			case 'u':
				option_keepalive_interval = optarg;
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case 'W':
				option_scheduler = optarg;
				break;
			case 'X':
				change_dir = optarg;
				break;
			case 'z':
				output_len_check = 1;
				break;
			case 'Z':
				option_port_file = optarg;
				port = 0;
				port_set = 1;	//WQ is going to set the port, so we continue as if already set.
				break;
			case LONG_OPT_PASSWORD:
				if(copy_file_to_buffer(optarg, &option_password, NULL) < 0) {
					fprintf(stderr, "makeflow: couldn't open %s: %s\n", optarg, strerror(errno));
					return 1;
				}
				break;
			case LONG_OPT_CACHE_MODE:
				cache_mode = optarg;
				break;
			case LONG_OPT_DISABLE_BATCH_CACHE:
				cache_mode = "task";
				break;
			case LONG_OPT_DISABLE_HEARTBEAT:
				batch_job_disable_heartbeat = 1;
				break;
			case LONG_OPT_HOOK_EXAMPLE:
				if (makeflow_hook_register(&makeflow_hook_example, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				break;
			case LONG_OPT_WAIT_FOR_WORKERS:
				option_wait_queue_size = optarg;
				break;
			case LONG_OPT_WORKING_DIR:
				free(working_dir);
				working_dir = xxstrdup(optarg);
				break;
			case LONG_OPT_PREFERRED_CONNECTION:
				free(option_preferred_connection);
				option_preferred_connection = xxstrdup(optarg);
				break;
			case LONG_OPT_DEBUG_ROTATE_MAX:
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case LONG_OPT_LOG_VERBOSE_MODE:
				log_verbose_mode = 1;
				break;
			case LONG_OPT_WRAPPER:
				if (makeflow_hook_register(&makeflow_hook_basic_wrapper, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				if(!jx_lookup(hook_args, "wrapper_command"))
					jx_insert(hook_args, jx_string("wrapper_command"),jx_array(NULL));
				jx_array_append(jx_lookup(hook_args, "wrapper_command"), jx_string(optarg));
				break;
			case LONG_OPT_WRAPPER_INPUT:
				if (makeflow_hook_register(&makeflow_hook_basic_wrapper, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				if(!jx_lookup(hook_args, "wrapper_input"))
					jx_insert(hook_args, jx_string("wrapper_input"),jx_array(NULL));
				jx_array_append(jx_lookup(hook_args, "wrapper_input"), jx_string(optarg));
				break;
			case LONG_OPT_WRAPPER_OUTPUT:
				if (makeflow_hook_register(&makeflow_hook_basic_wrapper, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				if(!jx_lookup(hook_args, "wrapper_output"))
					jx_insert(hook_args, jx_string("wrapper_output"),jx_array(NULL));
				jx_array_append(jx_lookup(hook_args, "wrapper_output"), jx_string(optarg));
				break;
			case LONG_OPT_SHARED_FS:
				if (optarg[0] != '/') fatal("Shared fs must be specified as an absolute path");
				if (makeflow_hook_register(&makeflow_hook_shared_fs, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				if(!jx_lookup(hook_args, "shared_fs_list"))
					jx_insert(hook_args, jx_string("shared_fs_list"),jx_array(NULL));
				jx_array_append(jx_lookup(hook_args, "shared_fs_list"), jx_string(optarg));
				break;
			case LONG_OPT_SSL_KEY:
				ssl_key_file = optarg;
				break;
			case LONG_OPT_SSL_CERT:
				ssl_cert_file = optarg;
				break;
			case LONG_OPT_STORAGE_TYPE:
				if (makeflow_hook_register(&makeflow_hook_storage_allocation, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("storage_allocation_type"), jx_integer(atoi(optarg)));
				break;
			case LONG_OPT_STORAGE_LIMIT:
				if (makeflow_hook_register(&makeflow_hook_storage_allocation, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("storage_allocation_limit"), jx_integer(string_metric_parse(optarg)));
				break;
			case LONG_OPT_STORAGE_PRINT:
				if (makeflow_hook_register(&makeflow_hook_storage_allocation, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("storage_allocation_print"), jx_string(optarg));
				break;
			case LONG_OPT_DOCKER:
				if (makeflow_hook_register(&makeflow_hook_docker, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("docker_container_image"), jx_string(optarg));
				break;
			case LONG_OPT_SKIP_FILE_CHECK:
				skip_file_check = 1;
				break;
			case LONG_OPT_DOCKER_TAR:
				if (makeflow_hook_register(&makeflow_hook_docker, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("docker_container_tar"), jx_string(optarg));
				break;
			case LONG_OPT_DOCKER_OPT:
				if (makeflow_hook_register(&makeflow_hook_docker, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("docker_container_opt"), jx_string(optarg));
				break;
			case LONG_OPT_SINGULARITY:
				if (makeflow_hook_register(&makeflow_hook_singularity, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("singularity_container_image"), jx_string(optarg));
				break;
			case LONG_OPT_SINGULARITY_OPT:
				jx_insert(hook_args, jx_string("singularity_container_options"), jx_string(optarg));
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
				break;
			case LONG_OPT_JSON:
				dag_syntax = DAG_SYNTAX_JSON;
				break;
			case LONG_OPT_JX:
				dag_syntax = DAG_SYNTAX_JX;
				break;
			case LONG_OPT_JX_ARGS:
				dag_syntax = DAG_SYNTAX_JX;
				jx_args = jx_parse_cmd_args(jx_args, optarg);
				if (!jx_args) {
					fatal("Failed to parse in JX Args File.\n");
				}
				break;
			case LONG_OPT_JX_DEFINE:
				dag_syntax = DAG_SYNTAX_JX;
				if (!jx_parse_cmd_define(jx_args, optarg)) {
					fatal("Failed to parse in JX Define.\n");
				}
				break;
			case LONG_OPT_UMBRELLA_BINARY:
				if (makeflow_hook_register(&makeflow_hook_umbrella, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("umbrella_binary"), jx_string(optarg));
				break;
			case LONG_OPT_UMBRELLA_LOG_PREFIX:
				if (makeflow_hook_register(&makeflow_hook_umbrella, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("umbrella_log_prefix"), jx_string(optarg));
				break;
			case LONG_OPT_UMBRELLA_MODE:
				if (makeflow_hook_register(&makeflow_hook_umbrella, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("umbrella_mode"), jx_string(optarg));
				break;
			case LONG_OPT_UMBRELLA_SPEC:
				if (makeflow_hook_register(&makeflow_hook_umbrella, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("umbrella_spec"), jx_string(optarg));
				break;
			case LONG_OPT_MESOS_MANAGER:
				mesos_manager = xxstrdup(optarg);
				break;
			case LONG_OPT_MESOS_PATH:
				mesos_path = xxstrdup(optarg);
				break;
			case LONG_OPT_MESOS_PRELOAD:
				mesos_preload = xxstrdup(optarg);
				break;
			case LONG_OPT_K8S_IMG:
				k8s_image = xxstrdup(optarg);
				break;
#ifdef HAS_CURL
			case LONG_OPT_S3_HOSTNAME:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
                    goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("s3_hostname"), jx_string(xxstrdup(optarg)));
				break;
			case LONG_OPT_S3_KEYID:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
                    goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("s3_keyid"), jx_string(xxstrdup(optarg)));
				break;
			case LONG_OPT_S3_SECRETKEY:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
                    goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("s3_secretkey"), jx_string(xxstrdup(optarg)));
				break;
			case LONG_OPT_ARCHIVE_S3_NO_CHECK:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("archive_s3_no_check"), jx_boolean(1));
			case LONG_OPT_ARCHIVE_S3:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				if(optarg){
					jx_insert(hook_args, jx_string("archive_s3_arg"), jx_string(xxstrdup(optarg)));
				}
				else{
					jx_insert(hook_args, jx_string("archive_s3_no_arg"), jx_string(""));
				}
			case LONG_OPT_ARCHIVE:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("archive_read"), jx_boolean(1));
				jx_insert(hook_args, jx_string("archive_write"), jx_boolean(1));
				break;
			case LONG_OPT_ARCHIVE_DIR:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("archive_dir"), jx_string(optarg));
				break;
			case LONG_OPT_ARCHIVE_READ:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("archive_read"), jx_boolean(1));
				break;
			case LONG_OPT_ARCHIVE_WRITE:
				if (makeflow_hook_register(&makeflow_hook_archive, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("archive_write"), jx_boolean(1));
				break;
#endif
			case LONG_OPT_SEND_ENVIRONMENT:
				should_send_all_local_environment = 1;
				break;
			case LONG_OPT_ENFORCEMENT:
				if (makeflow_hook_register(&makeflow_hook_enforcement, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				break;
			case LONG_OPT_PARROT_PATH:
				if (makeflow_hook_register(&makeflow_hook_enforcement, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("parrot_path"), jx_string(optarg));
				break;
			case LONG_OPT_ENVREPLACE:
				if (makeflow_hook_register(&makeflow_hook_enforcement, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("env_replace_path"), jx_string(optarg));
				break;
			case LONG_OPT_FAIL_DIR:
				save_failure = 0;
				break;
			case LONG_OPT_IGNORE_MEM:
				ignore_mem_spec = 1;
				break;
			case LONG_OPT_BATCH_MEM_TYPE:
				free(batch_mem_type);
				batch_mem_type = xxstrdup(optarg);
				break;
			case LONG_OPT_SAFE_SUBMIT:
				safe_submit = 1;
				break;
			case LONG_OPT_SANDBOX:
				if (makeflow_hook_register(&makeflow_hook_sandbox, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				break;
			case LONG_OPT_VC3:
				if (makeflow_hook_register(&makeflow_hook_vc3_builder, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				break;
			case LONG_OPT_VC3_EXE:
				if (makeflow_hook_register(&makeflow_hook_vc3_builder, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("vc3_exe"), jx_string(optarg));
				break;
			case LONG_OPT_VC3_LOG:
				if (makeflow_hook_register(&makeflow_hook_vc3_builder, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("vc3_log"), jx_string(optarg));
				break;
			case LONG_OPT_VC3_OPT:
				if (makeflow_hook_register(&makeflow_hook_vc3_builder, &hook_args) == MAKEFLOW_HOOK_FAILURE)
					goto EXIT_WITH_FAILURE;
				jx_insert(hook_args, jx_string("vc3_opt"), jx_string(optarg));
				break;
			case LONG_OPT_ARGV: {
				debug(D_MAKEFLOW, "loading argv from %s", optarg);
				struct jx *j = jx_parse_file(optarg);
				if (!j) {
					fatal("failed to parse JSON argv %s", optarg);
				}
				if (!jx_istype(j, JX_OBJECT)) {
					fatal("argv must be a JX object");
				}
				struct jx *k = jx_string("MAKEFLOW");
				struct jx *v = jx_remove(j, k);
				jx_delete(k);
				if (v && dagfile) {
					fatal("only one dagfile can be specified");
				}
				if (v && !jx_match_string(v, &dagfile)) {
					fatal("dagfile must be a string filename");
				}
				jx_delete(v);
				jx_getopt_push(j);
				jx_delete(j);
				break;
			}
			case LONG_OPT_VERBOSE_JOBNAMES:
				batch_job_verbose_jobnames = 1;
				break;
			case LONG_OPT_KEEP_WRAPPER_STDOUT:
				keep_wrapper_stdout = 1;
				break;
			case LONG_OPT_MPI_CORES:
				mpi_cores = atoi(optarg);
				break;
			case LONG_OPT_MPI_MEMORY:
				mpi_memory = atoi(optarg);
				break;
			case LONG_OPT_TLQ:
				tlq_port = atoi(optarg);
				break;
			case LONG_OPT_FILE_STATUS:
				file_status_on = 1;
				file_status_name = optarg;	
				break;
			case LONG_OPT_FILE_STATUS_INTERVAL:
				file_status_on = 1;
				file_status_interval = atoi(optarg);
				break;			
			default:
				show_help_run(argv[0]);
				return 1;
		}
	}

	cctools_version_debug(D_MAKEFLOW_RUN, argv[0]);

	/* If cleaning anything, assume local execution. This only really matters for nested workflows. */
	if(clean_mode != MAKEFLOW_CLEAN_NONE) {
		batch_queue_type = batch_queue_type_from_string("local");
	}

	/* Perform initial MPI setup prior to creating the batch queue object. */
	if(batch_queue_type==BATCH_QUEUE_TYPE_MPI) {
		batch_job_mpi_setup(debug_file_name ? debug_file_name : "debug", mpi_cores,mpi_memory);
	}

	if(!did_explicit_auth)
		auth_register_all();
	if(chirp_tickets) {
		auth_ticket_load(chirp_tickets);
		free(chirp_tickets);
	} else {
		auth_ticket_load(NULL);
	}

	// REGISTER HOOKS HERE
	/* This is intended for hooks that are on by default. */
	if(save_failure){
		if (makeflow_hook_register(&makeflow_hook_fail_dir, &hook_args) == MAKEFLOW_HOOK_FAILURE)
			goto EXIT_WITH_FAILURE;
	}

	int rc = makeflow_hook_create();
	if(rc != MAKEFLOW_HOOK_SUCCESS)
		goto EXIT_WITH_FAILURE;

	if((argc - optind) == 1) {
		if (dagfile) {
			fatal("only one dagfile can be specified");
		}
		dagfile = xxstrdup(argv[optind]);
	} else if (!dagfile) {
		int rv = access("./Makeflow", R_OK);
		if(rv < 0) {
			fprintf(stderr, "makeflow: No makeflow specified and file \"./Makeflow\" could not be found.\n");
			fprintf(stderr, "makeflow: Run \"%s -h\" for help with options.\n", argv[0]);
			return 1;
		}

		dagfile = xxstrdup("./Makeflow");
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_WORK_QUEUE || batch_queue_type == BATCH_QUEUE_TYPE_VINE ) {
		if(strcmp(option_manager_mode, "catalog") == 0 && project == NULL) {
			fprintf(stderr, "makeflow: Makeflow running in catalog mode. Please use '-N' option to specify the name of this project.\n");
			fprintf(stderr, "makeflow: Run \"makeflow -h\" for help with options.\n");
			return 1;
		}
		// Use Work Queue default port in standalone mode when port is not
		// specified with -p option. In Work Queue catalog mode, Work Queue
		// would choose an arbitrary port when port is not explicitly specified.
		if(!port_set && strcmp(option_manager_mode, "standalone") == 0) {
			port_set = 1;
			port = WORK_QUEUE_DEFAULT_PORT;
		}

		if(port_set) {
			char *value;
			value = string_format("%d", port);
			setenv("WORK_QUEUE_PORT", value, 1);
			setenv("VINE_PORT", value, 1);
			free(value);
		}
	}

	if(!logfilename)
		logfilename = string_format("%s.makeflowlog", dagfile);

	printf("parsing %s...\n",dagfile);
	d = dag_from_file(dagfile, dag_syntax, jx_args);

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
			} else if(batch_queue_type == BATCH_QUEUE_TYPE_VINE) {
				remote_jobs_max = 10 * MAX_REMOTE_JOBS_DEFAULT;
			} else {
				remote_jobs_max = MAX_REMOTE_JOBS_DEFAULT;
			}
		}
		printf("max running remote jobs: %d\n",remote_jobs_max);
	}

	printf("max running local jobs: %d\n",local_jobs_max);

	remote_queue = batch_queue_create(batch_queue_type,ssl_key_file,ssl_cert_file);
	if(!remote_queue) {
		fprintf(stderr, "makeflow: couldn't create batch queue.\n");
		if(port != 0)
			fprintf(stderr, "makeflow: perhaps port %d is already in use?\n", port);
		goto EXIT_WITH_FAILURE;
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
		batch_queue_set_option(remote_queue, "mesos-master", mesos_manager);
		batch_queue_set_option(remote_queue, "mesos-preload", mesos_preload);
	}

	if(batch_queue_type == BATCH_QUEUE_TYPE_K8S) {
		batch_queue_set_option(remote_queue, "k8s-image", k8s_image);
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

	if(!batch_submit_options){
		batch_submit_options = getenv("BATCH_OPTIONS");
		if(batch_submit_options){
			debug(D_MAKEFLOW, "BATCH_OPTIONS pulled from environment: %s", batch_submit_options);
		}
	}

	batch_queue_set_logfile(remote_queue, batchlogfilename);
	batch_queue_set_option(remote_queue, "batch-options", batch_submit_options);
	batch_queue_set_option(remote_queue, "password", option_password);
	batch_queue_set_option(remote_queue, "manager-mode", option_manager_mode);
	batch_queue_set_option(remote_queue, "name", project);
	batch_queue_set_option(remote_queue, "debug", debug_file_name);
	batch_queue_set_option(remote_queue, "priority", priority);
	batch_queue_set_option(remote_queue, "keepalive-interval", option_keepalive_interval);
	batch_queue_set_option(remote_queue, "keepalive-timeout", option_keepalive_timeout);
	batch_queue_set_option(remote_queue, "caching", cache_mode);
	batch_queue_set_option(remote_queue, "wait-queue-size", option_wait_queue_size);
	batch_queue_set_option(remote_queue, "amazon-config", amazon_config);
	batch_queue_set_option(remote_queue, "lambda-config", lambda_config);
	batch_queue_set_option(remote_queue, "working-dir", working_dir);
	batch_queue_set_option(remote_queue, "manager-preferred-connection", option_preferred_connection);
	batch_queue_set_option(remote_queue, "amazon-batch-config",amazon_batch_cfg);
	batch_queue_set_option(remote_queue, "amazon-batch-img", amazon_batch_img);
	batch_queue_set_option(remote_queue, "safe-submit-mode", safe_submit ? "yes" : "no");
	batch_queue_set_option(remote_queue, "ignore-mem-spec", ignore_mem_spec ? "yes" : "no");
	batch_queue_set_option(remote_queue, "mem-type", batch_mem_type);
	batch_queue_set_option(remote_queue, "keep-wrapper-stdout", keep_wrapper_stdout ? "yes" : "no" );
	if(option_scheduler) batch_queue_set_option(remote_queue, "scheduler", option_scheduler );
	batch_queue_set_int_option(remote_queue, "tlq-port", tlq_port);

	char *fa_multiplier = string_format("%f", option_fast_abort_multiplier);
	batch_queue_set_option(remote_queue, "fast-abort", fa_multiplier);
	free(fa_multiplier);

	/* Do not create a local queue for systems where local and remote are the same. */

	if(!batch_queue_supports_feature(remote_queue, "local_job_queue")) {
		local_queue = 0;
	} else {
		local_queue = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL,0,0);
		if(!local_queue) {
			fatal("couldn't create local job queue.");
		}
	}

	/* Remote storage modes do not (yet) support measuring storage for garbage collection. */

	if(makeflow_gc_method == MAKEFLOW_GC_SIZE && !batch_queue_supports_feature(remote_queue, "gc_size")) {
		makeflow_gc_method = MAKEFLOW_GC_ALL;
	}

	makeflow_parse_input_outputs(d);

	if (change_dir)
		chdir(change_dir);

	if(!disable_afs_check && (batch_queue_type==BATCH_QUEUE_TYPE_CONDOR)) {
		char *cwd = path_getcwd();
		if(!strncmp(cwd, "/afs", 4)) {
			fprintf(stderr,"error: The working directory is '%s'\n", cwd);
			fprintf(stderr,"This won't work because Condor is not able to write to files in AFS.\n");
			fprintf(stderr,"Instead, run your workflow from a local disk like /tmp.");
			fprintf(stderr,"Or, use the Work Queue batch system with -T wq.\n");
			free(cwd);
			goto EXIT_WITH_FAILURE;
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

	if(!makeflow_check_batch_consistency(d) && clean_mode == MAKEFLOW_CLEAN_NONE) {
		goto EXIT_WITH_FAILURE;
	}

	rc = makeflow_hook_dag_check(d);
	if(rc == MAKEFLOW_HOOK_FAILURE) {
		goto EXIT_WITH_FAILURE;
	} else if(rc == MAKEFLOW_HOOK_END) {
		goto EXIT_WITH_SUCCESS;
	}
	printf("%s has %d rules.\n",dagfile,d->nodeid_counter);

	setlinebuf(stdout);
	setlinebuf(stderr);

	if(mount_cache) d->cache_dir = mount_cache;

	/* In case when the user uses --cache option to specify the mount cache dir and the log file also has
	 * a cache dir logged, these two dirs must be the same. Otherwise exit.
	 */
	if(makeflow_log_recover(d, logfilename, log_verbose_mode, remote_queue, clean_mode )) {
		goto EXIT_WITH_FAILURE;
	}

	if(skip_file_check) {
		printf("skipping file checks.");
	} else {
		if(!makeflow_check_files(d)) {
			goto EXIT_WITH_FAILURE;
		}
	}

	/* This check must happen after makeflow_log_recover which may load the cache_dir info into d->cache_dir.
	 * This check must happen before makeflow_mount_install to guarantee that the program ends before any mount is copied if any target is invliad.
	 */
	if(use_mountfile) {
		if(makeflow_mount_check_target(d)) {
			goto EXIT_WITH_FAILURE;
		}
	}

	if(use_mountfile && !clean_mode) {
		if(makeflow_mounts_install(d)) {
			fprintf(stderr, "Failed to install the dependencies specified in the mountfile!\n");
			goto EXIT_WITH_FAILURE;
		}
	}

	struct dag_file *f = dag_file_lookup_or_create(d, batchlogfilename);
	makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXPECT);

	if(batch_queue_supports_feature(remote_queue, "batch_log_transactions")) {
		const char *transactions = batch_queue_get_option(remote_queue, "batch_log_transactions_name");
		f = dag_file_lookup_or_create(d, transactions);
		makeflow_log_file_state_change(d, f, DAG_FILE_STATE_EXPECT);
	}

	if(clean_mode != MAKEFLOW_CLEAN_NONE) {
		rc = makeflow_hook_dag_clean(d);
		if(rc != MAKEFLOW_HOOK_SUCCESS){
			debug(D_ERROR, "Failed to clean up makeflow hooks!\n");
			goto EXIT_WITH_FAILURE;
		}
		printf("cleaning filesystem...\n");
		if(makeflow_clean(d, remote_queue, clean_mode)) {
			debug(D_ERROR, "Failed to clean up makeflow!\n");
			goto EXIT_WITH_FAILURE;
		}

		if(clean_mode == MAKEFLOW_CLEAN_ALL) {
			unlink(logfilename);
			unlink(batchlogfilename);
		}

		goto EXIT_WITH_SUCCESS;
	}

	printf("starting workflow....\n");
	rc = makeflow_hook_dag_start(d);
	if(rc != MAKEFLOW_HOOK_SUCCESS){
		debug(D_ERROR, "Failed DAG start hooks");
		goto EXIT_WITH_FAILURE;
	}

	port = batch_queue_port(remote_queue);
	if(option_port_file)
		opts_write_port_file(option_port_file, port);
	if(port > 0)
		printf("listening for workers on port %d.\n", port);

	signal(SIGINT, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGTERM, handle_abort);

	makeflow_log_started_event(d);

	runtime = timestamp_get();

	if(tlq_port && debug_file_name) {
		debug(D_TLQ, "looking up makeflow TLQ URL");
		time_t config_stoptime = time(0) + 10;
		char *local_tlq_url = tlq_config_url(tlq_port, debug_file_name, config_stoptime);
		if(!local_tlq_url) debug(D_TLQ, "error looking up makeflow TLQ URL");
		else debug(D_TLQ, "set makeflow TLQ URL: %s", local_tlq_url);
	}
	else if(tlq_port && !debug_file_name) debug(D_TLQ, "cannot lookup makeflow TLQ URL: debug log not set");

	makeflow_run(d);

	if(makeflow_failed_flag == 0 && makeflow_nodes_local_waiting_count(d) > 0) {
		debug(D_ERROR, "There are local jobs that could not be run. Usually this means that makeflow did not have enough local resources to run them.");
		goto EXIT_WITH_FAILURE;
	}

	if(makeflow_hook_dag_end(d) != MAKEFLOW_HOOK_SUCCESS){
		goto EXIT_WITH_FAILURE;
	}

EXIT_WITH_SUCCESS:
	/* Makeflow fails by default if we goto EXIT_WITH_FAILURE.
		This indicates we have correctly initialized. */
	makeflow_failed_flag = 0;

EXIT_WITH_FAILURE:
	time_completed = timestamp_get();
	runtime = time_completed - runtime;

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

	if(write_summary_to || email_summary_to)
		makeflow_summary_create(d, write_summary_to, email_summary_to, runtime, time_completed, argc, argv, dagfile, remote_queue, makeflow_abort_flag, makeflow_failed_flag );

	int exit_value;
	if(makeflow_abort_flag) {
		makeflow_hook_dag_abort(d);
		makeflow_log_aborted_event(d);
		fprintf(stderr, "workflow was aborted.\n");
		exit_value = EXIT_FAILURE;
	} else if(makeflow_failed_flag) {
		makeflow_hook_dag_fail(d);
		makeflow_log_failed_event(d);
		fprintf(stderr, "workflow failed.\n");
		exit_value = EXIT_FAILURE;
	} else {
		makeflow_hook_dag_success(d);
		makeflow_log_completed_event(d);
		printf("nothing left to do.\n");
		exit_value = EXIT_SUCCESS;
	}

	/* delete all files. We do this here are some of these files may be created in clean mode. */
	{
		struct dag_node *n;
		uint64_t key;
		itable_firstkey(d->node_table);
		while(itable_nextkey(d->node_table, &key, (void **) &n)) {
			if(n->workflow_args) {
				debug(D_MAKEFLOW_RUN, "deleting tmp file: %s\n", dag_node_nested_workflow_filename(n, "args"));
				unlink(dag_node_nested_workflow_filename(n, "args"));
			}
		}
	}

	makeflow_hook_destroy(d);

	/* Batch queues are removed after hooks are destroyed to allow for file clean up on related files. */
	batch_queue_delete(remote_queue);
	if(local_queue)
		batch_queue_delete(local_queue);

	makeflow_log_close(d);

	/* clean up allocated objects to satisfy valgrind */
	if(logfilename) free(logfilename);
	if(batchlogfilename) free(batchlogfilename);
	jx_delete(jx_args);
	jx_delete(base_hook_args);

        
	exit(exit_value);
        
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
