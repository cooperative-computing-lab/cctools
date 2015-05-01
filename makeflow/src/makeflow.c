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
#include "random.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "work_queue.h"
#include "work_queue_catalog.h"
#include "xxmalloc.h"

#include "dag.h"
#include "dag_log.h"
#include "dag_gc.h"
#include "dag_visitors.h"
#include "makeflow_summary.h"
#include "parser.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <ctype.h>
#include <errno.h>
#include <signal.h>
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

#define MONITOR_ENV_VAR "CCTOOLS_RESOURCE_MONITOR"
#define DEFAULT_MONITOR_LOG_FORMAT "resource-rule-%06.6d"
#define DEFAULT_MONITOR_INTERVAL   1

sig_atomic_t dag_abort_flag = 0;
int dag_failed_flag = 0;
static int dag_submit_timeout = 3600;
static int dag_retry_flag = 0;
static int dag_retry_max = 100;

static dag_gc_method_t dag_gc_method = DAG_GC_NONE;
static int dag_gc_param = -1;
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

#define CONTAINER_SH_PREFIX "docker.wrapper"
#define CONTAINER_TMP_SH_PREFIX "docker.tmp"

typedef enum {
	CONTAINER_MODE_NONE,
	CONTAINER_MODE_DOCKER,
	// CONTAINER_MODE_ROCKET etc
} container_mode_t;

static container_mode_t container_mode = CONTAINER_MODE_NONE;
static char *container_image = NULL;

/* wait upto this many seconds for an output file of a succesfull task
 * to appear on the local filesystem (e.g, to deal with NFS
 * semantics. . */
static int file_creation_patience_wait_time = 0;

/* Write a verbose transaction log with SYMBOL tags.
 * SYMBOLs are category labels (SYMBOLs should be deprecated
 * once weaver/pbui tools are updated.) */
static int log_verbose_mode = 0;

static char *monitor_limits_name = NULL;
static int monitor_interval = 1;	// in seconds
static char *monitor_log_format = NULL;
static char *monitor_log_dir = NULL;

static char *wrapper_command = 0;
static struct list *wrapper_input_files = 0;
static struct list *wrapper_output_files = 0;

void dag_node_state_change(struct dag *d, struct dag_node *n, int newstate)
{
	dag_log_state_change(d,n,newstate);
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

	if(batch_fs_unlink(remote_queue, filename) == 0) {
		if(!silent)
			printf("deleted path %s\n", filename);
	} else if(errno == ENOENT) {
		// say nothing
	} else if(!silent) {
		fprintf(stderr, "couldn't delete %s: %s\n", filename, strerror(errno));
	}
}

static void dag_node_export_variables( struct dag *d, struct dag_node *n );

void dag_node_clean(struct dag *d, struct dag_node *n)
{
	struct dag_file *f;
	list_first_item(n->target_files);
	while((f = list_next_item(n->target_files))) {
		file_clean(f->filename, 0);
		hash_table_remove(d->completed_files, f->filename);
	}

	/* If the node is a Makeflow job, then we should recursively call the
	 * clean operation on it. */
	if(n->nested_job) {
		char *command = xxmalloc(sizeof(char) * (strlen(n->command) + 4));
		sprintf(command, "%s -c", n->command);

		/* XXX this should use the batch job interface for consistency */
		dag_node_export_variables(d, n);
		system(command);
		free(command);
	}
}

void dag_clean(struct dag *d)
{
	struct dag_node *n;
	for(n = d->nodes; n; n = n->next)
		dag_node_clean(d, n);
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

		p = f1->created_by;
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

/*
Creates an nvpair containing the explicit environment
strings for this given node.  Each element of the list
is a string of the form name=value.
If nothing has been set, this function may return null.
*/

struct nvpair * dag_node_env_create( struct dag *d, struct dag_node *n )
{
	struct dag_variable_lookup_set s = { d, n->category, n, NULL };
	char *key;

	struct nvpair *nv = 0;

	set_first_element(d->export_vars);
	while((key = set_next_element(d->export_vars))) {
		char *value = dag_variable_lookup_string(key, &s);
		if(value) {
			if(!nv) nv = nvpair_create();
			nvpair_insert_string(nv,key,value);
			debug(D_MAKEFLOW_RUN, "export %s=%s", key, value);
		}
	}

	return nv;
}

/*
For a given dag node, export all variables into the environment.
This is currently only used when cleaning a makeflow recurisvely,
and would be better handled by invoking batch_job_local.
*/

static void dag_node_export_variables( struct dag *d, struct dag_node *n )
{
	struct nvpair *nv = dag_node_env_create(d,n);
	nvpair_export(nv);
	nvpair_delete(nv);
}

/*
Wraps a given command with the appropriate resource monitor string.
Returns a newly allocated string that must be freed.
*/

char *dag_node_rmonitor_wrap_command( struct dag_node *n, const char *command )
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

char * dag_file_format( struct dag_node *n, struct dag_file *f, struct batch_queue *queue )
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

char * dag_file_list_format( struct dag_node *node, char *file_str, struct list *file_list, struct batch_queue *queue )
{
	struct dag_file *file;

	if(!file_str) file_str = strdup("");

	if(!file_list) return file_str;

	list_first_item(file_list);
	while((file=list_next_item(file_list))) {
		char *f = dag_file_format(node,file,queue);
		file_str = string_combine(file_str,f);
		free(f);
	}

	return file_str;
}

/*
Submit one fully formed job, retrying failures up to the dag_submit_timeout.
This is necessary because busy batch systems occasionally do not accept a job submission.
*/

batch_job_id_t dag_node_submit_retry( struct batch_queue *queue, const char *command, const char *input_files, const char *output_files, struct nvpair *envlist )
{
	time_t stoptime = time(0) + dag_submit_timeout;
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

		if(dag_abort_flag) break;

		if(time(0) > stoptime) {
			fprintf(stderr, "unable to submit job after %d seconds!\n", dag_submit_timeout);
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

void dag_node_submit(struct dag *d, struct dag_node *n)
{
	struct batch_queue *queue;

	if(n->local_job && local_queue) {
		queue = local_queue;
	} else {
		queue = remote_queue;
	}

	/*
	XXX this code has several problems:
	1 - It should be abstracted away in a function.
	2 - It should use string_format() to generate strings of appropriate size.
	3 - It should not modify the global list wrapper_input_files.
	*/

	if (container_mode == CONTAINER_MODE_DOCKER) {

	  	FILE *wrapper_fn;
		
		char wrapper_fn_name[4096];
		sprintf(wrapper_fn_name, "%s.%d", CONTAINER_SH_PREFIX, n->nodeid);
	
	  	wrapper_fn = fopen(wrapper_fn_name, "w"); 

		char tmp_sh_name[4096];
		sprintf(tmp_sh_name, "%s.%d", CONTAINER_TMP_SH_PREFIX, n->nodeid); 
 
	  	fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
echo \"#!/bin/sh\" > %s\n\
echo \"$@\" >> %s\n\
chmod 755 %s\n\
flock /tmp/lockfile /usr/bin/docker pull %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir \
%s $default_dir/%s", tmp_sh_name, tmp_sh_name, tmp_sh_name, container_image, container_image, tmp_sh_name);
 
	  	fclose(wrapper_fn);

		chmod(wrapper_fn_name, 0755);

		/* XXX this is badly incorrect: it is adding files to the global wrapper list on each job submission. */
		/* We must either do that once at startup, or do it every time to the local variable input_files. */

		if(!wrapper_input_files) wrapper_input_files = list_create();
		list_push_tail(wrapper_input_files,dag_file_create(wrapper_fn_name)); 

		char wrap_cmd[4096]; 
		sprintf(wrap_cmd, "./%s.%%%%", CONTAINER_SH_PREFIX);
		
		if(!wrapper_command) wrapper_command = strdup(wrap_cmd);
		else wrapper_command = string_wrap_command(wrapper_command,optarg);

	}

	/* Create strings for all the files mentioned by this node. */
	char *input_files = dag_file_list_format(n,0,n->source_files,queue);
	char *output_files = dag_file_list_format(n,0,n->target_files,queue);

	/* Add the wrapper input and output files to the strings. */
	/* This function may realloc input_files and output_files. */
	input_files = dag_file_list_format(n,input_files,wrapper_input_files,queue);
	output_files = dag_file_list_format(n,output_files,wrapper_output_files,queue);

	/* Apply the wrapper(s) to the command, if it is (they are) enabled. */
	char * command = string_wrap_command(n->command,wrapper_command);

	/* Wrap the command with the resource monitor, if it is enabled. */
	if(monitor_mode) {
		char *newcommand = dag_node_rmonitor_wrap_command(n,command);
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
	n->jobid = dag_node_submit_retry(queue,command,input_files,output_files,envlist);

	/* Restore old batch job options. */
	if(old_batch_submit_options) {
		batch_queue_set_option(queue, "batch-options", old_batch_submit_options);
		free(old_batch_submit_options);
	}

	/* Update all of the necessary data structures. */
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

	free(command);
	free(input_files);
	free(output_files);
	nvpair_delete(envlist);
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

int dag_node_check_file_was_created(struct dag_node *n, struct dag_file *f) {
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
			if(!dag_node_check_file_was_created(n, f))
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
		dag_node_state_change(d, n, DAG_NODE_STATE_FAILED);
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
			debug(D_MAKEFLOW_RUN, "%s: %d\n", f->filename, f->ref_count);
		}

		dag_node_state_change(d, n, DAG_NODE_STATE_COMPLETE);
	}
}

int dag_check(struct dag *d)
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
				printf("job %"PRIbjid" completed\n",jobid);
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
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
				debug(D_MAKEFLOW_RUN, "Job %" PRIbjid " has returned.\n", jobid);
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
			dag_gc(d,dag_gc_method,dag_gc_param);
			dag_gc_barrier = MAX(d->nodeid_counter * dag_gc_task_ratio, 1);
		}
	}

	if(dag_abort_flag) {
		dag_abort_all(d);
	} else {
		if(!dag_failed_flag && dag_gc_method != DAG_GC_NONE) {
			dag_gc(d,DAG_GC_FORCE,0);
		}
	}
}

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
	dag_abort_flag = 1;
}

static void show_help_run(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, "Frequently used options:\n\n");
	fprintf(stdout, " %-30s Clean up: remove logfile and all targets.\n", "-c,--clean");
	fprintf(stdout, " %-30s Change directory: chdir to enable executing the Makefile in other directory.\n", "-X,--change-directory");
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
	fprintf(stdout, " %-30s Use this file for the makeflow log.         (default is X.makeflowlog)\n", "-l,--makeflow-log=<logfile>");
	fprintf(stdout, " %-30s Use this file for the batch system log.     (default is X.<type>log)\n", "-L,--batch-log=<logfile>");
	fprintf(stdout, " %-30s Send summary of workflow to this email address upon success or failure.\n", "-m,--email=<email>");
	fprintf(stdout, " %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate debug file once it reaches this size.\n", "   --debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Password file for authenticating workers.\n", "   --password");
	fprintf(stdout, " %-30s Port number to use with Work Queue.       (default is %d, 0=arbitrary)\n", "-p,--port=<port>", WORK_QUEUE_DEFAULT_PORT);
	fprintf(stdout, " %-30s Priority. Higher the value, higher the priority.\n", "-P,--priority=<integer>");
	fprintf(stdout, " %-30s Automatically retry failed batch jobs up to %d times.\n", "-R,--retry", dag_retry_max);
	fprintf(stdout, " %-30s Automatically retry failed batch jobs up to n times.\n", "-r,--retry-count=<n>");
	fprintf(stdout, " %-30s Wait for output files to be created upto n seconds (e.g., to deal with NFS semantics).", "--wait-for-files-upto=<n>");
	fprintf(stdout, " %-30s Time to retry failed batch job submission.  (default is %ds)\n", "-S,--submission-timeout=<#>", dag_submit_timeout);
	fprintf(stdout, " %-30s Work Queue keepalive timeout.               (default is %ds)\n", "-t,--wq-keepalive-timeout=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	fprintf(stdout, " %-30s Work Queue keepalive interval.              (default is %ds)\n", "-u,--wq-keepalive-interval=<#>", WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	fprintf(stdout, " %-30s Show version string\n", "-v,--version");
	fprintf(stdout, " %-30s Work Queue scheduling algorithm.            (time|files|fcfs)\n", "-W,--wq-schedule=<mode>");
	fprintf(stdout, " %-30s Wrap all commands with this prefix.\n", "--wrapper=<cmd>");
	fprintf(stdout, " %-30s Wrapper command requires this input file.\n", "--wrapper-input=<cmd>");
	fprintf(stdout, " %-30s Wrapper command produces this output file.\n", "--wrapper-output=<cmd>");
	fprintf(stdout, " %-30s Force failure on zero-length output files \n", "-z,--zero-length-error");
	fprintf(stdout, " %-30s Select port at random and write it to this file.\n", "-Z,--port-file=<file>");
	fprintf(stdout, " %-30s Disable Work Queue caching.                 (default is false)\n", "   --disable-wq-cache");
	fprintf(stdout, " %-30s Add node id symbol tags in the makeflow log.        (default is false)\n", "   --log-verbose");
	fprintf(stdout, " %-30s Run each task with a container based on this docker image.\n", "--docker=<image>");

	fprintf(stdout, "\n*Monitor Options:\n\n");
	fprintf(stdout, " %-30s Enable the resource monitor, and write the monitor logs to <dir>.\n", "-M,--monitor=<dir>");
	fprintf(stdout, " %-30s Use <file> as value-pairs for resource limits.\n", "--monitor-limits=<file>");
	fprintf(stdout, " %-30s Set monitor interval to <#> seconds.        (default is 1 second)\n", "--monitor-interval=<#>");
	fprintf(stdout, " %-30s Enable monitor time series.                 (default is disabled)\n", "--monitor-with-time-series");
	fprintf(stdout, " %-30s Enable monitoring of openened files.        (default is disabled)\n", "--monitor-with-opened-files");
	fprintf(stdout, " %-30s Format for monitor logs.                    (default %s)\n", "--monitor-log-fmt=<fmt>", DEFAULT_MONITOR_LOG_FORMAT);
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
        LONG_OPT_DOCKER
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
				if(!wrapper_command) {
					wrapper_command = strdup(optarg);
				} else {
					wrapper_command = string_wrap_command(wrapper_command,optarg);
				}
				break;
			case LONG_OPT_WRAPPER_INPUT:
				if(!wrapper_input_files) wrapper_input_files = list_create();
				list_push_tail(wrapper_input_files,dag_file_create(optarg));
				break;
			case LONG_OPT_WRAPPER_OUTPUT:
				if(!wrapper_output_files) wrapper_output_files = list_create();
				list_push_tail(wrapper_output_files,dag_file_create(optarg));
				break;
			case LONG_OPT_DOCKER:
				container_mode = CONTAINER_MODE_DOCKER; 
				container_image = xxstrdup(optarg);
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
		if(dag_gc_method == DAG_GC_ON_DEMAND) {
			dag_gc_method = DAG_GC_REF_COUNT;
		}
	}

	if(dag_gc_method != DAG_GC_NONE)
		dag_gc_prepare(d);

	dag_prepare_nested_jobs(d);

	if (change_dir)
		chdir(change_dir);

	if(clean_mode) {
		printf("cleaning filesystem...\n");
		dag_clean(d);
		unlink(logfilename);
		unlink(batchlogfilename);
		exit(0);
	}

	printf("checking %s for consistency...\n",dagfile);
	if(!dag_check(d)) {
		exit(EXIT_FAILURE);
	}

	printf("%s has %d rules.\n",dagfile,d->nodeid_counter);

	setlinebuf(stdout);
	setlinebuf(stderr);

	dag_log_recover(d, logfilename, log_verbose_mode );

	printf("starting workflow....\n");

	port = batch_queue_port(remote_queue);
	if(work_queue_port_file)
		opts_write_port_file(work_queue_port_file, port);
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

	if(write_summary_to || email_summary_to)
		makeflow_summary_create(d, write_summary_to, email_summary_to, runtime, time_completed, argc, argv, dagfile, remote_queue );

	/* XXX better to write created files to log, then delete those listed in log. */

	if (container_mode == CONTAINER_MODE_DOCKER) {
		char *cmd = string_format("rm %s.* %s.*",CONTAINER_SH_PREFIX,CONTAINER_TMP_SH_PREFIX);
		system(cmd);
		free(cmd);
	}

	if(dag_abort_flag) {
		fprintf(d->logfile, "# ABORTED\t%" PRIu64 "\n", timestamp_get());
		fprintf(stderr, "workflow was aborted.\n");
		exit(EXIT_FAILURE);
	} else if(dag_failed_flag) {
		fprintf(d->logfile, "# FAILED\t%" PRIu64 "\n", timestamp_get());
		fprintf(stderr, "workflow failed.\n");
		exit(EXIT_FAILURE);
	} else {
		fprintf(d->logfile, "# COMPLETED\t%" PRIu64 "\n", timestamp_get());
        
      	printf("nothing left to do.\n");
		exit(EXIT_SUCCESS);
	}

    

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
