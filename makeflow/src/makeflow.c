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

#include "dag.h"
#include "dag_visitors.h"
#include "makeflow_summary.h"
#include "makeflow_log.h"
#include "makeflow_gc.h"
#include "parser.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

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

#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%06.6d"

#define CONTAINER_SH "docker.wrapper.sh"

#define MAX_REMOTE_JOBS_DEFAULT 100

typedef enum {
	CONTAINER_MODE_NONE,
	CONTAINER_MODE_DOCKER,
	// CONTAINER_MODE_ROCKET etc
} container_mode_t;

static sig_atomic_t makeflow_abort_flag = 0;
static int makeflow_failed_flag = 0;
static int makeflow_submit_timeout = 3600;
static int makeflow_retry_flag = 0;
static int makeflow_retry_max = MAX_REMOTE_JOBS_DEFAULT;

static makeflow_gc_method_t makeflow_gc_method = MAKEFLOW_GC_NONE;
static int makeflow_gc_param = -1;
static int makeflow_gc_barrier = 1;
static double makeflow_gc_task_ratio = 0.05;

static batch_queue_type_t batch_queue_type = BATCH_QUEUE_TYPE_LOCAL;
static struct batch_queue *local_queue = 0;
static struct batch_queue *remote_queue = 0;

static int local_jobs_max = 1;
static int remote_jobs_max = 100;

static char *project = NULL;
static int port = 0;
static int output_len_check = 0;

static int cache_mode = 1;

static char *monitor_exe  = "resource_monitor_cctools";

static int monitor_mode = 0;
static int monitor_enable_time_series = 0;
static int monitor_enable_list_files  = 0;

static char *monitor_limits_name = NULL;
static int   monitor_interval = 1;	// in seconds
static char *monitor_log_format = NULL;
static char *monitor_log_dir = NULL;

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

static char *wrapper_command = 0;
static struct list *wrapper_input_files = 0;
static struct list *wrapper_output_files = 0;

static void makeflow_wrapper_add_command( const char *cmd )
{
	if(!wrapper_command) {
		wrapper_command = strdup(cmd);
	} else {
		wrapper_command = string_wrap_command(wrapper_command,cmd);
	}
}

static void makeflow_wrapper_add_input_file( const char *file )
{
 	if(!wrapper_input_files) wrapper_input_files = list_create();
	list_push_tail(wrapper_input_files,dag_file_create(file));
}

static void makeflow_wrapper_add_output_file( const char *file )
{
	if(!wrapper_output_files) wrapper_output_files = list_create();
	list_push_tail(wrapper_output_files,dag_file_create(file));
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
		printf("aborting local job %" PRIu64 "\n", jobid);
		batch_job_remove(local_queue, jobid);
		makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}

	itable_firstkey(d->remote_job_table);
	while(itable_nextkey(d->remote_job_table, &jobid, (void **) &n)) {
		printf("aborting remote job %" PRIu64 "\n", jobid);
		batch_job_remove(remote_queue, jobid);
		makeflow_log_state_change(d, n, DAG_NODE_STATE_ABORTED);
	}
}

/*
Clean a specific file, while emitting an appropriate message.
*/

static void makeflow_file_clean(const char *filename, int silent)
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

/*
For a given dag node, export all variables into the environment.
This is currently only used when cleaning a makeflow recurisvely,
and would be better handled by invoking batch_job_local.
*/

static void makeflow_node_export_variables( struct dag *d, struct dag_node *n )
{
	struct nvpair *nv = dag_node_env_create(d,n);
	nvpair_export(nv);
	nvpair_delete(nv);
}

/*
Clean a node of a dag by cleaning all of its output dependencies,
or by cleaning it recursively if it is a Makeflow itself.
*/

static void makeflow_node_clean(struct dag *d, struct dag_node *n)
{
	struct dag_file *f;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		makeflow_file_clean(f->filename, 0);
		hash_table_remove(d->completed_files, f->filename);
	}

	/* If the node is a Makeflow job, then we should recursively call the
	 * clean operation on it. */
	if(n->nested_job) {
		char *command = xxmalloc(sizeof(char) * (strlen(n->command) + 4));
		sprintf(command, "%s -c", n->command);

		/* XXX this should use the batch job interface for consistency */
		makeflow_node_export_variables(d, n);
		system(command);
		free(command);
	}
}

/*
Clean the entire dag by cleaning all nodes.
*/

static void makeflow_clean(struct dag *d)
{
	struct dag_node *n;
	for(n = d->nodes; n; n = n->next)
		makeflow_node_clean(d, n);
}

static void makeflow_node_force_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n);

/*
Decide whether to rerun a node based on batch and file system status.
*/

void makeflow_node_decide_rerun(struct itable *rerun_table, struct dag *d, struct dag_node *n)
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
			if(!f->created_by) {
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
	makeflow_node_clean(d, n);
	makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);

	// For each parent node, rerun it if input file was garbage collected
	list_first_item(n->source_files);
	while((f1 = list_next_item(n->source_files))) {
		if(!set_lookup(d->collect_table, f1))
			continue;

		p = f1->created_by;
		if(p) {
			makeflow_node_force_rerun(rerun_table, d, p);
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

static char *monitor_log_name(char *dirname, int nodeid)
{
	char *name = string_format(monitor_log_format, nodeid);
	char *path = string_format("%s/%s", dirname, name);
	free(name);

	return path;
}

/*
Prepare a node for monitoring by wrapping the command and attaching the 
appropriate input and output dependencies.
*/

static int makeflow_prepare_for_monitoring(struct dag *d)
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

/*
Wraps a given command with the appropriate resource monitor string.
Returns a newly allocated string that must be freed.
*/

static char *makeflow_node_rmonitor_wrap_command( struct dag_node *n, const char *command )
{
	char *log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
	char *limits_str = dag_node_resources_wrap_as_rmonitor_options(n);
	char *extra_options = string_format("%s -V '%-15s%s'",
			limits_str ? limits_str : "",
			"category:",
			n->category->label);

	log_name_prefix     = monitor_log_name(monitor_log_dir, n->nodeid);

	char * result = resource_monitor_rewrite_command(command,
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

	return result;
}


/*
Replace instances of %% in a string with the string 'replace'.
To escape this behavior, %%%% becomes %%.
(Backslash it not used as the escape, as it would interfere with shell escapes.)
This function works like realloc: the string str must be created by malloc
and may be freed and reallocated.  Therefore, always invoke it like this:
x = replace_percents(x,replace);
*/

static char * replace_percents( char *str, const char *replace )
{
	/* Common case: do nothing if no percents. */
	if(!strchr(str,'%')) return str;

	buffer_t buffer;
	buffer_init(&buffer);

	char *s;
	for(s=str;*s;s++) {
		if(*s=='%' && *(s+1)=='%' ) {
			if( *(s+2)=='%' && *(s+3)=='%') {
				buffer_putlstring(&buffer,"%%",2);
				s+=3;
			} else {
				buffer_putstring(&buffer,replace);
				s++;
			}
		} else {
			buffer_putlstring(&buffer,s,1);
		}
	}

	char *result;
	buffer_dup(&buffer,&result);
	buffer_free(&buffer);

	free(str);

	return result;
}

/*
Given a file, return the string that identifies it appropriately
for the given batch system, combining the local and remote name
and making substitutions according to the node.
*/

static char * makeflow_file_format( struct dag_node *n, struct dag_file *f, struct batch_queue *queue )
{
	const char *remotename = dag_node_get_remote_name(n, f->filename);
	if(!remotename) remotename = f->filename;

	switch (batch_queue_get_type(queue)) {
		case BATCH_QUEUE_TYPE_WORK_QUEUE:
			return string_format("%s=%s,", f->filename, remotename);
		case BATCH_QUEUE_TYPE_CONDOR:
			return string_format("%s,", remotename);
		default:
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

static batch_job_id_t makeflow_node_submit_retry( struct batch_queue *queue, const char *command, const char *input_files, const char *output_files, struct nvpair *envlist )
{
	time_t stoptime = time(0) + makeflow_submit_timeout;
	int waittime = 1;
	batch_job_id_t jobid = 0;

	/* Display the fully elaborated command, just like Make does. */
	printf("submitting job: %s\n", command);

	while(1) {
		jobid = batch_job_submit(queue, command, input_files, output_files, envlist );
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

static void makeflow_create_docker_sh() 
{       
    FILE *wrapper_fn;
	
  	wrapper_fn = fopen(CONTAINER_SH, "w"); 

    if (image_tar == NULL) {

        fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker pull %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", container_image, container_image);

    } else {

        fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker load < %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", image_tar, container_image);

        makeflow_wrapper_add_input_file(image_tar);
    }

  	fclose(wrapper_fn);

	chmod(CONTAINER_SH, 0755);   

    makeflow_wrapper_add_input_file(CONTAINER_SH);
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

	/* Create strings for all the files mentioned by this node. */
	char *input_files = makeflow_file_list_format(n,0,n->source_files,queue);
	char *output_files = makeflow_file_list_format(n,0,n->target_files,queue);

	/* Add the wrapper input and output files to the strings. */
	/* This function may realloc input_files and output_files. */
	input_files = makeflow_file_list_format(n,input_files,wrapper_input_files,queue);
	output_files = makeflow_file_list_format(n,output_files,wrapper_output_files,queue);
	/* Apply the wrapper(s) to the command, if it is (they are) enabled. */
	char *command = string_wrap_command(n->command, wrapper_command);

	/* Wrap the command with the resource monitor, if it is enabled. */
	if(monitor_mode) {
		char *newcommand = makeflow_node_rmonitor_wrap_command(n,command);
		free(command);
		command = newcommand;
	}

	/* Before setting the batch job options (stored in the "BATCH_OPTIONS"
	 * variable), we must save the previous global queue value, and then
	 * restore it after we submit. */
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *batch_options_env    = dag_variable_lookup_string("BATCH_OPTIONS", &s);
	char *batch_submit_options = dag_node_resources_wrap_options(n, batch_options_env, batch_queue_get_type(queue));
	char *old_batch_submit_options = NULL;

	free(batch_options_env);
	if(batch_submit_options) {
		debug(D_MAKEFLOW_RUN, "Batch options: %s\n", batch_submit_options);
		if(batch_queue_get_option(queue, "batch-options"))
			old_batch_submit_options = xxstrdup(batch_queue_get_option(queue, "batch-options"));
		batch_queue_set_option(queue, "batch-options", batch_submit_options);
		free(batch_submit_options);
	}

	/* Generate the environment vars specific to this node. */
	struct nvpair *envlist = dag_node_env_create(d,n);

	/*
	Just before execution, replace double-percents with the nodeid.
	This is used for substituting in the nodeid into a wrapper command or file.
	*/
	char *nodeid = string_format("%d",n->nodeid);
	command = replace_percents(command,nodeid);
	input_files = replace_percents(input_files,nodeid);
	output_files = replace_percents(output_files,nodeid);
	free(nodeid);

	/* Now submit the actual job, retrying failures as needed. */
	n->jobid = makeflow_node_submit_retry(queue,command,input_files,output_files,envlist);

	/* Restore old batch job options. */
	if(old_batch_submit_options) {
		batch_queue_set_option(queue, "batch-options", old_batch_submit_options);
		free(old_batch_submit_options);
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
	free(input_files);
	free(output_files);
	nvpair_delete(envlist);
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
		if(hash_table_lookup(d->completed_files, f->filename)) {
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

static void makeflow_node_complete(struct dag *d, struct dag_node *n, struct batch_job_info *info)
{
	struct dag_file *f;
	int job_failed = 0;

	if(n->state != DAG_NODE_STATE_RUNNING)
		return;

	if(info->exited_normally && info->exit_code == 0) {
		list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))) {
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
		if(monitor_mode && info->exit_code == 147)
		{
			fprintf(stderr, "\nrule %d failed because it exceeded the resources limits.\n", n->nodeid);
			char *log_name_prefix = monitor_log_name(monitor_log_dir, n->nodeid);
			char *summary_name = string_format("%s.summary", log_name_prefix);
			struct rmsummary *s = rmsummary_parse_limits_exceeded(summary_name);

			if(s)
			{
				rmsummary_print(stderr, s, NULL);
				free(s);
				fprintf(stderr, "\n");
			}

			free(log_name_prefix);
			free(summary_name);

			makeflow_failed_flag = 1;
		}
		else if(makeflow_retry_flag || info->exit_code == 101) {
			n->failure_count++;
			if(n->failure_count > makeflow_retry_max) {
				fprintf(stderr, "job %s failed too many times.\n", n->command);
				makeflow_failed_flag = 1;
			} else {
				fprintf(stderr, "will retry failed job %s\n", n->command);
				makeflow_log_state_change(d, n, DAG_NODE_STATE_WAITING);
			}
		}
		else
		{
			makeflow_failed_flag = 1;
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
			debug(D_MAKEFLOW_RUN, "%s: %d\n", f->filename, f->ref_count);
		}

		makeflow_log_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
}

/*
Check the dag for consistency, and emit errors if input dependencies, etc are missing.
*/

static int makeflow_check(struct dag *d)
{
	struct dag_node *n;
	struct dag_file *f;
	int error = 0;

	debug(D_MAKEFLOW_RUN, "checking rules for consistency...\n");

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

			if(f->created_by) {
				continue;
			}

			if(!error) {
				fprintf(stderr, "makeflow: %s does not exist, and is not created by any rule.\n", f->filename);
			}
			error = 1;
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
					makeflow_node_complete(d, n, &info);
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
					makeflow_node_complete(d, n, &info);
			}
		}

		/* Rather than try to garbage collect after each time in this
		 * wait loop, perform garbage collection after a proportional
		 * amount of tasks have passed. */
		makeflow_gc_barrier--;
		if(makeflow_gc_method != MAKEFLOW_GC_NONE && makeflow_gc_barrier == 0) {
			makeflow_gc(d,makeflow_gc_method,makeflow_gc_param);
			makeflow_gc_barrier = MAX(d->nodeid_counter * makeflow_gc_task_ratio, 1);
		}
	}

	if(makeflow_abort_flag) {
		makeflow_abort_all(d);
	} else {
		if(!makeflow_failed_flag && makeflow_gc_method != MAKEFLOW_GC_NONE) {
			makeflow_gc(d,MAKEFLOW_GC_FORCE,0);
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
	printf(" %-30s Clean up: remove logfile and all targets.\n", "-c,--clean");
	printf(" %-30s Change directory: chdir to enable executing the Makefile in other directory.\n", "-X,--change-directory");
	printf(" %-30s Batch system type: (default is local)\n", "-T,--batch-type=<type>");
	printf(" %-30s %s\n\n", "", batch_queue_type_string());
	printf("Other options are:\n");
	printf(" %-30s Advertise the master information to a catalog server.\n", "-a,--advertise");
	printf(" %-30s Disable the check for AFS. (experts only.)\n", "-A,--disable-afs-check");
	printf(" %-30s Add these options to all batch submit files.\n", "-B,--batch-options=<options>");
	printf(" %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT \n", "-C,--catalog-server=<catalog>");
	printf(" %-30s Enable debugging for this subsystem\n", "-d,--debug=<subsystem>");
	printf(" %-30s Write summary of workflow to this file upon success or failure.\n", "-f,--summary-log=<file>");
	printf(" %-30s Work Queue fast abort multiplier.           (default is deactivated)\n", "-F,--wq-fast-abort=<#>");
	printf(" %-30s Show this help screen.\n", "-h,--help");
	printf(" %-30s Max number of local jobs to run at once.    (default is # of cores)\n", "-j,--max-local=<#>");
	printf(" %-30s Max number of remote jobs to run at once.\n", "-J,--max-remote=<#>");
	printf("                                                            (default %d for -Twq, %d otherwise.)\n", 10*MAX_REMOTE_JOBS_DEFAULT, MAX_REMOTE_JOBS_DEFAULT );
	printf(" %-30s Use this file for the makeflow log.         (default is X.makeflowlog)\n", "-l,--makeflow-log=<logfile>");
	printf(" %-30s Use this file for the batch system log.     (default is X.<type>log)\n", "-L,--batch-log=<logfile>");
	printf(" %-30s Send summary of workflow to this email address upon success or failure.\n", "-m,--email=<email>");
	printf(" %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	printf(" %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	printf(" %-30s Rotate debug file once it reaches this size.\n", "   --debug-rotate-max=<bytes>");
	printf(" %-30s Password file for authenticating workers.\n", "   --password");
	printf(" %-30s Port number to use with Work Queue.       (default is %d, 0=arbitrary)\n", "-p,--port=<port>", WORK_QUEUE_DEFAULT_PORT);
	printf(" %-30s Priority. Higher the value, higher the priority.\n", "-P,--priority=<integer>");
	printf(" %-30s Automatically retry failed batch jobs up to %d times.\n", "-R,--retry", makeflow_retry_max);
	printf(" %-30s Automatically retry failed batch jobs up to n times.\n", "-r,--retry-count=<n>");
	printf(" %-30s Wait for output files to be created upto n seconds (e.g., to deal with NFS semantics).", "--wait-for-files-upto=<n>");
	printf(" %-30s Time to retry failed batch job submission.  (default is %ds)\n", "-S,--submission-timeout=<#>", makeflow_submit_timeout);
	printf(" %-30s Work Queue keepalive timeout.               (default is %ds)\n", "-t,--wq-keepalive-timeout=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	printf(" %-30s Work Queue keepalive interval.              (default is %ds)\n", "-u,--wq-keepalive-interval=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	printf(" %-30s Show version string\n", "-v,--version");
	printf(" %-30s Work Queue scheduling algorithm.            (time|files|fcfs)\n", "-W,--wq-schedule=<mode>");
	printf(" %-30s Wrap all commands with this prefix.\n", "--wrapper=<cmd>");
	printf(" %-30s Wrapper command requires this input file.\n", "--wrapper-input=<cmd>");
	printf(" %-30s Wrapper command produces this output file.\n", "--wrapper-output=<cmd>");
	printf(" %-30s Force failure on zero-length output files \n", "-z,--zero-length-error");
	printf(" %-30s Select port at random and write it to this file.\n", "-Z,--port-file=<file>");
	printf(" %-30s Disable Work Queue caching.                 (default is false)\n", "   --disable-wq-cache");
	printf(" %-30s Add node id symbol tags in the makeflow log.        (default is false)\n", "   --log-verbose");
	printf(" %-30s Run each task with a container based on this docker image.\n", "--docker=<image>");
	printf(" %-30s Load docker image from the tar file.\n", "--docker-tar=<tar file>");

	printf("\n*Monitor Options:\n\n");
	printf(" %-30s Enable the resource monitor, and write the monitor logs to <dir>.\n", "-M,--monitor=<dir>");
	printf(" %-30s Use <file> as value-pairs for resource limits.\n", "--monitor-limits=<file>");
	printf(" %-30s Set monitor interval to <#> seconds.        (default is 1 second)\n", "--monitor-interval=<#>");
	printf(" %-30s Enable monitor time series.                 (default is disabled)\n", "--monitor-with-time-series");
	printf(" %-30s Enable monitoring of openened files.        (default is disabled)\n", "--monitor-with-opened-files");
	printf(" %-30s Format for monitor logs.                    (default %s)\n", "--monitor-log-fmt=<fmt>", DEFAULT_MONITOR_LOG_FORMAT);
}

int main(int argc, char *argv[])
{
	int c;
	random_init();
	debug_config(argv[0]);

	cctools_version_debug((long) D_MAKEFLOW_RUN, argv[0]);
	const char *dagfile;
	char *change_dir = NULL;
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
	timestamp_t runtime = 0;
	int skip_afs_check = 0;
	timestamp_t time_completed = 0;
	const char *work_queue_keepalive_interval = NULL;
	const char *work_queue_keepalive_timeout = NULL;
	const char *work_queue_master_mode = "standalone";
	const char *work_queue_port_file = NULL;
	const char *priority = NULL;
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
		LONG_OPT_MONITOR_INTERVAL,
		LONG_OPT_MONITOR_LIMITS,
		LONG_OPT_MONITOR_LOG_NAME,
		LONG_OPT_MONITOR_OPENED_FILES,
		LONG_OPT_MONITOR_TIME_SERIES,
		LONG_OPT_PASSWORD,
		LONG_OPT_TICKETS,
		LONG_OPT_VERBOSE_PARSING,
		LONG_OPT_LOG_VERBOSE_MODE,
		LONG_OPT_WORKING_DIR,
		LONG_OPT_WQ_WAIT_FOR_WORKERS,
		LONG_OPT_WRAPPER,
		LONG_OPT_WRAPPER_INPUT,
		LONG_OPT_WRAPPER_OUTPUT,
        LONG_OPT_DOCKER,
        LONG_OPT_DOCKER_TAR
	};

	static struct option long_options_run[] = {
		{"advertise", no_argument, 0, 'a'},
		{"auth", required_argument, 0, LONG_OPT_AUTH},
		{"batch-log", required_argument, 0, 'L'},
		{"batch-options", required_argument, 0, 'B'},
		{"batch-type", required_argument, 0, 'T'},
		{"catalog-server", required_argument, 0, 'C'},
		{"clean", no_argument, 0, 'c'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, LONG_OPT_DEBUG_ROTATE_MAX},
		{"disable-afs-check", no_argument, 0, 'A'},
		{"disable-cache", no_argument, 0, LONG_OPT_DISABLE_BATCH_CACHE},
		{"email", required_argument, 0, 'm'},
		{"wait-for-files-upto", required_argument, 0, LONG_OPT_FILE_CREATION_PATIENCE_WAIT_TIME},
		{"help", no_argument, 0, 'h'},
		{"makeflow-log", required_argument, 0, 'l'},
		{"max-local", required_argument, 0, 'j'},
		{"max-remote", required_argument, 0, 'J'},
		{"monitor", required_argument, 0, 'M'},
		{"monitor-interval", required_argument, 0, LONG_OPT_MONITOR_INTERVAL},
		{"monitor-limits", required_argument,   0, LONG_OPT_MONITOR_LIMITS},
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
		{0, 0, 0, 0}
	};

	static const char option_string_run[] = "aAB:cC:d:Ef:F:g:G:hj:J:l:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:X:zZ:";

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
				clean_mode = 1;
				break;
			case 'C':
				if(!work_queue_catalog_parse(optarg, &catalog_host, &catalog_port)) {
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
					makeflow_gc_method = MAKEFLOW_GC_REF_COUNT;
					if(makeflow_gc_param < 0)
						makeflow_gc_param = 16;	/* Try to collect at most 16 files. */
				} else if(strcasecmp(optarg, "on_demand") == 0) {
					makeflow_gc_method = MAKEFLOW_GC_ON_DEMAND;
					if(makeflow_gc_param < 0)
						makeflow_gc_param = 1 << 14;	/* Inode threshold of 2^14. */
				} else {
					fprintf(stderr, "makeflow: invalid garbage collection method: %s\n", optarg);
					exit(1);
				}
				break;
			case 'G':
				makeflow_gc_param = atoi(optarg);
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
			case LONG_OPT_DEBUG_ROTATE_MAX:
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case LONG_OPT_LOG_VERBOSE_MODE:
				log_verbose_mode = 1;
				break;
			case LONG_OPT_WRAPPER:
				makeflow_wrapper_add_command(optarg);
				break;
			case LONG_OPT_WRAPPER_INPUT:
				makeflow_wrapper_add_input_file(optarg);
				break;
			case LONG_OPT_WRAPPER_OUTPUT:
				makeflow_wrapper_add_output_file(optarg);
				break;
			case LONG_OPT_DOCKER:
				container_mode = CONTAINER_MODE_DOCKER; 
				container_image = xxstrdup(optarg);
				break;
            case LONG_OPT_DOCKER_TAR:
                image_tar = xxstrdup(optarg);
                break;
			default:
				show_help_run(argv[0]);
				return 1;
			case 'X':
				change_dir = optarg;
				break;
		}
	}

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

		// In clean mode, delete all existing log files
		if(clean_mode) {
			BUFFER_STACK_ABORT(B, PATH_MAX);
			buffer_putfstring(&B, "%s.condorlog", dagfile);
			unlink(buffer_tostring(&B));
			buffer_rewind(&B, 0);
			buffer_putfstring(&B, "%s.wqlog", dagfile);
			unlink(buffer_tostring(&B));
			buffer_rewind(&B, 0);
			buffer_putfstring(&B, "%s.batchlog", dagfile);
			unlink(buffer_tostring(&B));
		}
	}

	if(monitor_mode) {
		if(!monitor_log_dir)
			fatal("Monitor mode was enabled, but a log output directory was not specified (use -M<dir>)");

		monitor_exe = resource_monitor_copy_to_wd(NULL);

		if(monitor_interval < 1)
			fatal("Monitoring interval should be non-negative.");

		if(!monitor_log_format)
			monitor_log_format = DEFAULT_MONITOR_LOG_FORMAT;
	}

	printf("parsing %s...\n",dagfile);
	struct dag *d = dag_from_file(dagfile);
	if(!d) {
		fatal("makeflow: couldn't load %s: %s\n", dagfile, strerror(errno));
	}

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

	if(monitor_mode && !makeflow_prepare_for_monitoring(d)) {
		fatal("Could not prepare for monitoring.\n");
	}

	remote_queue = batch_queue_create(batch_queue_type);
	if(!remote_queue) {
		fprintf(stderr, "makeflow: couldn't create batch queue.\n");
		if(port != 0)
			fprintf(stderr, "makeflow: perhaps port %d is already in use?\n", port);
		exit(EXIT_FAILURE);
	}

	batch_queue_set_logfile(remote_queue, batchlogfilename);
	batch_queue_set_option(remote_queue, "batch-options", batch_submit_options);
	batch_queue_set_option(remote_queue, "skip-afs-check", skip_afs_check ? "yes" : "no");
	batch_queue_set_option(remote_queue, "password", work_queue_password);
	batch_queue_set_option(remote_queue, "master-mode", work_queue_master_mode);
	batch_queue_set_option(remote_queue, "name", project);
	batch_queue_set_option(remote_queue, "priority", priority);
	batch_queue_set_option(remote_queue, "estimate-capacity", "yes"); // capacity estimation is on by default
	batch_queue_set_option(remote_queue, "keepalive-interval", work_queue_keepalive_interval);
	batch_queue_set_option(remote_queue, "keepalive-timeout", work_queue_keepalive_timeout);
	batch_queue_set_option(remote_queue, "caching", cache_mode ? "yes" : "no");
	batch_queue_set_option(remote_queue, "wait-queue-size", wq_wait_queue_size);
	batch_queue_set_option(remote_queue, "working-dir", working_dir);

	/* Do not create a local queue for systems where local and remote are the same. */

	if(batch_queue_type == BATCH_QUEUE_TYPE_CHIRP ||
	   batch_queue_type == BATCH_QUEUE_TYPE_HADOOP ||
	   batch_queue_type == BATCH_QUEUE_TYPE_LOCAL) {
		local_queue = 0;
	} else {
		local_queue = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL);
		if(!local_queue) {
			fatal("couldn't create local job queue.");
		}
	}

	/* Remote storage modes do not (yet) support measuring storage for garbage collection. */
	
	if(batch_queue_type==BATCH_QUEUE_TYPE_CHIRP || batch_queue_type==BATCH_QUEUE_TYPE_HADOOP) {
		if(makeflow_gc_method == MAKEFLOW_GC_ON_DEMAND) {
			makeflow_gc_method = MAKEFLOW_GC_REF_COUNT;
		}
	}

	if(makeflow_gc_method != MAKEFLOW_GC_NONE)
		makeflow_gc_prepare(d);

	makeflow_prepare_nested_jobs(d);

	if (change_dir)
		chdir(change_dir);

	if(clean_mode) {
		printf("cleaning filesystem...\n");
		makeflow_clean(d);
		unlink(logfilename);
		unlink(batchlogfilename);
		exit(0);
	}

	printf("checking %s for consistency...\n",dagfile);
	if(!makeflow_check(d)) {
		exit(EXIT_FAILURE);
	}

	printf("%s has %d rules.\n",dagfile,d->nodeid_counter);

	setlinebuf(stdout);
	setlinebuf(stderr);

	makeflow_log_recover(d, logfilename, log_verbose_mode );

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
    /* XXX if docker mode is on 
     * 1) create a global script for running docker container
     * 2) add this script to the global wrapper list
     * 3) reformat each task command
     */
        
        makeflow_create_docker_sh();
        char *global_cmd = string_format("sh %s", CONTAINER_SH);        
        makeflow_wrapper_add_command(global_cmd);

	    struct dag_node *n;
       	for(n = d->nodes; n; n = n->next) 
            n->command = string_format("sh -c \"%s\"", n->command);
    }

	makeflow_run(d);
	time_completed = timestamp_get();
	runtime = time_completed - runtime;

	if(local_queue)
		batch_queue_delete(local_queue);
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
