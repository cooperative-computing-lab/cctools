/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager.h"
#include "vine_task.h"
#include "vine_worker_info.h"
#include "vine_file.h"
#include "vine_mount.h"

#include "list.h"
#include "rmsummary.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "rmonitor.h"
#include "macros.h"

#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include "random.h"

struct vine_task *vine_task_create(const char *command_line)
{
	struct vine_task *t = malloc(sizeof(*t));
	if(!t) {
		fprintf(stderr, "Error: failed to allocate memory for task.\n");
		return NULL;
	}
	memset(t, 0, sizeof(*t));

	/* REMEMBER: Any memory allocation done in this function should have a
	 * corresponding copy in vine_task_clone. Otherwise we get
	 * double-free segfaults. */

	/* For clarity, put initialization in same order as structure. */

	if(command_line) t->command_line = xxstrdup(command_line);
	t->category = xxstrdup("default");

	t->input_mounts = list_create();
	t->output_mounts = list_create();
	t->env_list = list_create();
	t->feature_list = list_create();

	t->resource_request   = CATEGORY_ALLOCATION_FIRST;
	t->worker_selection_algorithm = VINE_SCHEDULE_UNSET;

	t->state = VINE_TASK_READY;

	t->result = VINE_RESULT_UNKNOWN;
	t->exit_code = -1;

	/* In the absence of additional information, a task consumes an entire worker. */
	t->resources_requested = rmsummary_create(-1);
	t->resources_measured  = rmsummary_create(-1);
	t->resources_allocated = rmsummary_create(-1);

	return t;
}

void vine_task_clean( struct vine_task *t, int full_clean )
{
	t->time_when_commit_start = 0;
	t->time_when_commit_end   = 0;
	t->time_when_retrieval    = 0;
	t->time_when_done = 0;

	t->time_workers_execute_last = 0;
	t->time_workers_execute_last_start = 0;
	t->time_workers_execute_last_end = 0;

	t->bytes_sent = 0;
	t->bytes_received = 0;
	t->bytes_transferred = 0;

	free(t->output);
	t->output = NULL;

	free(t->hostname);
	t->hostname = NULL;

	free(t->addrport);
	t->addrport = NULL;

	if(full_clean) {
		t->resource_request = CATEGORY_ALLOCATION_FIRST;
		t->try_count = 0;
		t->exhausted_attempts = 0;
		t->workers_slow = 0;

		t->time_workers_execute_all = 0;
		t->time_workers_execute_exhaustion = 0;
		t->time_workers_execute_failure = 0;

		rmsummary_delete(t->resources_measured);
		rmsummary_delete(t->resources_allocated);
		t->resources_measured  = rmsummary_create(-1);
		t->resources_allocated = rmsummary_create(-1);
	}

	/* If result is never updated, then it is mark as a failure. */
	t->result = VINE_RESULT_UNKNOWN;
	t->state = VINE_TASK_READY;
}

static struct list *vine_task_mount_list_clone(struct list *list)
{
	struct list *new = list_create();
	struct vine_mount *old_mount, *new_mount;
	
	LIST_ITERATE(list,old_mount) {
		new_mount = vine_mount_clone(old_mount);
		list_push_tail(new, new_mount);
	}
	return new;
}

static struct list *vine_task_string_list_clone(struct list *string_list)
{
	struct list *new = list_create();
	char *var;

	LIST_ITERATE(string_list,var) {
		list_push_tail(new, xxstrdup(var));
	}

	return new;
}


struct vine_task *vine_task_clone(const struct vine_task *task)
{
	if(!task) return 0;

	struct vine_task *new = vine_task_create(task->command_line);

	/* Static features of task are copied. */
	if(task->coprocess) vine_task_set_coprocess(new,task->tag);
	if(task->tag) vine_task_set_tag(new, task->tag);
	if(task->category) vine_task_set_category(new, task->category);

	if(task->monitor_output_directory) {
		vine_task_set_monitor_output(new, task->monitor_output_directory);
	}

	if(task->monitor_snapshot_file) {
		vine_task_set_snapshot_file(new, task->monitor_snapshot_file);
	}

	new->input_mounts  = vine_task_mount_list_clone(task->input_mounts);
	new->output_mounts = vine_task_mount_list_clone(task->output_mounts);
	new->env_list     = vine_task_string_list_clone(task->env_list);
	new->feature_list = vine_task_string_list_clone(task->feature_list);

	/* Scheduling features of task are copied. */
	new->resource_request = task->resource_request;
	vine_task_set_scheduler(new, task->worker_selection_algorithm);
	vine_task_set_priority(new, task->priority);
	vine_task_set_retries(new, task->max_retries);
	vine_task_set_time_min(new, task->min_running_time);

	/* Internal state of task is cleared from vine_task_create */

	/* Results of task are cleared from vine_task_create. */

	/* Metrics of task are cleared from vine_task_create. */

	/* Resource requests are copied. */

	if(task->resources_requested) {
		new->resources_requested = rmsummary_copy(task->resources_requested, 0);
	}

	return new;
}


void vine_task_set_command( struct vine_task *t, const char *cmd )
{
	if(t->command_line) free(t->command_line);
	t->command_line = xxstrdup(cmd);
}

static void delete_feature(struct vine_task *t, const char *name)
{
	struct list_cursor *c = list_cursor_create(t->feature_list);

	char *feature;
	for(list_seek(c, 0); list_get(c, (void **) &feature); list_next(c)) {
		if(name && feature && (strcmp(name, feature) == 0)) {
			list_drop(c);
		}
	}

	list_cursor_destroy(c);
}

void vine_task_set_coprocess( struct vine_task *t, const char *coprocess )
{
	if(t->coprocess) {
		delete_feature(t, t->coprocess);
		free(t->coprocess);
		t->coprocess = NULL;
	}

	if(coprocess) {
		t->coprocess = xxstrdup(coprocess);
		vine_task_add_feature(t, t->coprocess);
	}
}

void vine_task_set_env_var( struct vine_task *t, const char *name, const char *value )
{
	if(value) {
		list_push_tail(t->env_list,string_format("%s=%s",name,value));
	} else {
		/* Specifications without = indicate variables to me unset. */
		list_push_tail(t->env_list,string_format("%s",name));
	}
}

void vine_task_set_retries( struct vine_task *t, int64_t max_retries ) {
	if(max_retries < 1) {
		t->max_retries = 0;
	}
	else {
		t->max_retries = max_retries;
	}
}

void vine_task_set_memory( struct vine_task *t, int64_t memory )
{
	if(memory < 0)
	{
		t->resources_requested->memory = -1;
	}
	else
	{
		t->resources_requested->memory = memory;
	}
}

void vine_task_set_disk( struct vine_task *t, int64_t disk )
{
	if(disk < 0)
	{
		t->resources_requested->disk = -1;
	}
	else
	{
		t->resources_requested->disk = disk;
	}
}

void vine_task_set_cores( struct vine_task *t, int cores )
{
	if(cores < 0)
	{
		t->resources_requested->cores = -1;
	}
	else
	{
		t->resources_requested->cores = cores;
	}
}

void vine_task_set_gpus( struct vine_task *t, int gpus )
{
	if(gpus < 0)
	{
		t->resources_requested->gpus = -1;
	}
	else
	{
		t->resources_requested->gpus = gpus;
	}
}

void vine_task_set_time_end( struct vine_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->end = -1;
	}
	else
	{
		t->resources_requested->end = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void vine_task_set_time_start( struct vine_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->start = -1;
	}
	else
	{
		t->resources_requested->start = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void vine_task_set_time_max( struct vine_task *t, int64_t seconds )
{
	if(seconds < 1)
	{
		t->resources_requested->wall_time = -1;
	}
	else
	{
		t->resources_requested->wall_time = DIV_INT_ROUND_UP(seconds, ONE_SECOND);
	}
}

void vine_task_set_time_min( struct vine_task *t, int64_t seconds )
{
	if(seconds < 1)
	{
		t->min_running_time = -1;
	}
	else
	{
		t->min_running_time = seconds;
	}
}

void vine_task_set_resources(struct vine_task *t, const struct rmsummary *rm) {
	if(!rm)
		return;

	vine_task_set_cores(t,        rm->cores);
	vine_task_set_memory(t,       rm->memory);
	vine_task_set_disk(t,         rm->disk);
	vine_task_set_gpus(t,         rm->gpus);
	vine_task_set_time_max(t, rm->wall_time);
	vine_task_set_time_min(t, t->min_running_time);
	vine_task_set_time_end(t,     rm->end);
}

void vine_task_set_tag(struct vine_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xxstrdup(tag);
}

void vine_task_set_category(struct vine_task *t, const char *category)
{
	if(t->category)
		free(t->category);

	t->category = xxstrdup(category ? category : "default");
}

void vine_task_add_feature(struct vine_task *t, const char *name)
{
	if(!name) {
		return;
	}

	list_push_tail(t->feature_list, xxstrdup(name));
}

/*
Make sure that the various files added to the task do not conflict.
Emit warnings if inconsistencies are detected, but keep going otherwise.
*/

void vine_task_check_consistency( struct vine_task *t )
{
	struct hash_table *table = hash_table_create(0,0);
	struct vine_mount *m;

	/* Cannot have multiple input files mapped to the same remote name. */

	LIST_ITERATE(t->input_mounts,m) {
		if(hash_table_lookup(table,m->remote_name)) {
			fprintf(stderr,"warning: task %d has more than one input file named %s\n",t->task_id,m->remote_name);
		} else {
			hash_table_insert(table,m->remote_name,m->remote_name);
		}
	}

	hash_table_clear(table,0);

	/* Cannot have multiple output files bring back the same file. */

	LIST_ITERATE(t->output_mounts,m) {
		if(m->file->type==VINE_FILE && hash_table_lookup(table,m->file->source)) {
			fprintf(stderr,"warning: task %d has more than one output file named %s\n",t->task_id,m->file->source);
		} else {
			hash_table_insert(table,m->remote_name,m->file->source);
		}
	}

	hash_table_clear(table,0);
	hash_table_delete(table);
}

void vine_task_add_input( struct vine_task *t, struct vine_file *f, const char *remote_name, vine_mount_flags_t flags )
{
	if(!t || !f || !f->source || !remote_name) {
		fatal("%s: invalid null argument.",__func__);
	}

	if(remote_name[0] == '/') {
		fatal("%s: invalid remote name %s: cannot start with a slash.",__func__,remote_name);
	}

	struct vine_mount *m = vine_mount_create(f,remote_name,flags,0);

	list_push_tail(t->input_mounts, m);
}

void vine_task_add_output( struct vine_task *t, struct vine_file *f, const char *remote_name, vine_mount_flags_t flags )
{
	if(!t || !f || !f->source || !remote_name) {
		fatal("%s: invalid null argument.",__func__);
	}

	if(remote_name[0] == '/') {
		fatal("%s: invalid remote name %s: cannot start with a slash.",__func__,remote_name);
	}

	struct vine_mount *m = vine_mount_create(f,remote_name,flags,0);

	list_push_tail(t->output_mounts, m);
}

void vine_task_add_input_file(struct vine_task *t, const char *local_name, const char *remote_name, vine_mount_flags_t flags)
{
	struct vine_file *f = vine_file_local(local_name, 0);
	vine_task_add_input(t,f,remote_name,flags);
}

void vine_task_add_output_file(struct vine_task *t, const char *local_name, const char *remote_name, vine_mount_flags_t flags)
{
	struct vine_file *f = vine_file_local(local_name, 0);
	vine_task_add_output(t,f,remote_name,flags);
}

void vine_task_add_input_url(struct vine_task *t, const char *file_url, const char *remote_name, vine_mount_flags_t flags)
{
	struct vine_file *f = vine_file_url(file_url, 0);
	vine_task_add_input(t,f,remote_name,flags);
}

void vine_task_add_empty_dir( struct vine_task *t, const char *remote_name )
{
	struct vine_file *f = vine_file_empty_dir();
	vine_task_add_input(t,f,remote_name,0);
}

void vine_task_add_input_buffer(struct vine_task *t, const char *data, int length, const char *remote_name, vine_mount_flags_t flags)
{
	struct vine_file *f = vine_file_buffer(data,length,0);
	vine_task_add_input(t,f,remote_name,flags);
}

void vine_task_add_input_mini_task(struct vine_task *t, struct vine_task *mini_task, const char *remote_name, vine_mount_flags_t flags)
{
	/* XXX mini task must have a single output file */
	struct vine_file *f = vine_file_mini_task(mini_task, 0);
	vine_task_add_input(t,f,remote_name,flags);
}

void vine_task_add_environment(struct vine_task *t, struct vine_file *environment_file) {

	assert(environment_file);

	char *env_name = string_format("__vine_env_%s", environment_file->cached_name);

	vine_task_add_input(t, environment_file, env_name, 0);

	char *new_cmd = string_format("%s/bin/run_in_env %s", env_name, t->command_line);
	vine_task_set_command(t, new_cmd);

	free(env_name);
}


void vine_task_set_snapshot_file(struct vine_task *t, struct vine_file *monitor_snapshot_file) {

	assert(monitor_snapshot_file);

	t->monitor_snapshot_file = monitor_snapshot_file;
	vine_task_add_input(t, monitor_snapshot_file, RESOURCE_MONITOR_REMOTE_NAME_EVENTS, 0);
}

void vine_task_set_scheduler(struct vine_task *t, vine_schedule_t algorithm)
{
	t->worker_selection_algorithm = algorithm;
}

void vine_task_set_priority( struct vine_task *t, double priority )
{
	t->priority = priority;
}

void vine_task_set_monitor_output(struct vine_task *t, const char *monitor_output_directory) {

	if(!monitor_output_directory) {
		fatal("Error: no monitor_output_file was specified.");
	}

	if(t->monitor_output_directory) {
		free(t->monitor_output_directory);
	}

	t->monitor_output_directory = xxstrdup(monitor_output_directory);
}

int vine_task_set_result(struct vine_task *t, vine_result_t new_result)
{
	if(new_result & ~(0x7)) {
		/* Upper bits are set, so this is not related to old-style result for
		 * inputs, outputs, or stdout, so we simply make an update. */
		t->result = new_result;
	} else if(t->result != VINE_RESULT_UNKNOWN && t->result & ~(0x7)) {
		/* Ignore new result, since we only update for input, output, or
		 * stdout missing when no other result exists. This is because
		 * missing inputs/outputs are anyway expected with other kind of
		 * errors. */
	} else if(new_result == VINE_RESULT_INPUT_MISSING) {
		/* input missing always appears by itself, so yet again we simply make an update. */
		t->result = new_result;
	} else if(new_result == VINE_RESULT_OUTPUT_MISSING) {
		/* output missing clobbers stdout missing. */
		t->result = new_result;
	} else {
		/* we only get here for stdout missing. */
		t->result = new_result;
	}

	return t->result;
}

void vine_task_delete(struct vine_task *t)
{
	if(!t) return;

	free(t->command_line);
	free(t->coprocess);
	free(t->tag);
	free(t->category);

	free(t->monitor_output_directory);

	list_clear(t->input_mounts,(void*)vine_mount_delete);
	list_delete(t->input_mounts);

	list_clear(t->output_mounts,(void*)vine_mount_delete);
	list_delete(t->output_mounts);

	list_clear(t->env_list,(void*)free);
	list_delete(t->env_list);

	list_clear(t->feature_list,(void*)free);
	list_delete(t->feature_list);

	free(t->output);
	free(t->addrport);
	free(t->hostname);

	rmsummary_delete(t->resources_requested);
	rmsummary_delete(t->resources_measured);
	rmsummary_delete(t->resources_allocated);

	free(t);
}

const char * vine_task_get_command( struct vine_task *t )
{
	return t->command_line;
}

const char * vine_task_get_tag( struct vine_task *t )
{
	return t->tag;
}

const char * vine_task_get_category( struct vine_task *t )
{
	return t->category;
}


int vine_task_get_id( struct vine_task *t )
{
	return t->task_id;
}

const char * vine_task_get_stdout( struct vine_task *t )
{
	return t->output;
}

int vine_task_get_exit_code( struct vine_task *t )
{
	return t->exit_code;
}

vine_result_t vine_task_get_result( struct vine_task *t )
{
	return t->result;
}

const char * vine_task_get_addrport( struct vine_task *t )
{
	return t->addrport;
}

const char * vine_task_get_hostname( struct vine_task *t )
{
	return t->hostname;
}

#define METRIC(x) if(!strcmp(name,#x)) return t->x;
int64_t vine_task_get_metric( struct vine_task *t, const char *name )
{
	METRIC(time_when_submitted);
	METRIC(time_when_done);
	METRIC(time_when_commit_start);
	METRIC(time_when_commit_end);
	METRIC(time_when_retrieval);
	METRIC(time_workers_execute_last);
	METRIC(time_workers_execute_all);
	METRIC(time_workers_execute_exhaustion);
	METRIC(time_workers_execute_failure);
	METRIC(bytes_received);
	METRIC(bytes_sent);
	METRIC(bytes_transferred);
	return 0;
}

#define RESOURCES(x) if(!strcmp(name,#x)) return t->resources_ ## x;
const struct rmsummary *vine_task_get_resources( struct vine_task *t, const char *name )
{
	RESOURCES(measured);
	RESOURCES(requested);
	RESOURCES(allocated);
	return 0;
}

const char *vine_task_state_to_string( vine_task_state_t task_state )
{
	const char *str;

	switch(task_state) {
		case VINE_TASK_READY:
			str = "WAITING";
			break;
		case VINE_TASK_RUNNING:
			str = "RUNNING";
			break;
		case VINE_TASK_WAITING_RETRIEVAL:
			str = "WAITING_RETRIEVAL";
			break;
		case VINE_TASK_RETRIEVED:
			str = "RETRIEVED";
			break;
		case VINE_TASK_DONE:
			str = "DONE";
			break;
		case VINE_TASK_CANCELED:
			str = "CANCELED";
			break;
		case VINE_TASK_UNKNOWN:
		default:
			str = "UNKNOWN";
			break;
	}

	return str;
}

static void priority_add_to_jx(struct jx *j, double priority)
{
	int decimals = 2;
	int factor   = pow(10, decimals);

	int dpart = ((int) (priority * factor)) - ((int) priority) * factor;

	char *str;

	if(dpart == 0)
		str = string_format("%d", (int) priority);
	else
		str = string_format("%.2g", priority);

	jx_insert_string(j, "priority", str);

	free(str);
}

/*
Converts a task into JX format for the purpose of performance
and status reporting, without file details.
*/

struct jx * vine_task_to_jx( struct vine_manager *q, struct vine_task *t )
{
	struct jx *j = jx_object(0);

	jx_insert_integer(j,"task_id",t->task_id);
	jx_insert_string(j,"state",vine_task_state_to_string(t->state));
	if(t->tag) jx_insert_string(j,"tag",t->tag);
	if(t->category) jx_insert_string(j,"category",t->category);
	jx_insert_string(j,"command",t->command_line);
	if(t->coprocess) jx_insert_string(j,"coprocess",t->coprocess);
	if(t->worker) {
		jx_insert_string(j, "addrport", t->worker->addrport);
		jx_insert_string(j,"host",t->worker->hostname);

		jx_insert_integer(j,"cores",t->resources_allocated->cores);
		jx_insert_integer(j,"gpus",t->resources_allocated->gpus);
		jx_insert_integer(j,"memory",t->resources_allocated->memory);
		jx_insert_integer(j,"disk",t->resources_allocated->disk);
	} else {
		const struct rmsummary *min = vine_manager_task_resources_min(q, t);
		const struct rmsummary *max = vine_manager_task_resources_max(q, t);

		struct rmsummary *limits = rmsummary_create(-1);
		rmsummary_merge_override(limits, max);
		rmsummary_merge_max(limits, min);

		jx_insert_integer(j,"cores",limits->cores);
		jx_insert_integer(j,"gpus",limits->gpus);
		jx_insert_integer(j,"memory",limits->memory);
		jx_insert_integer(j,"disk",limits->disk);

		rmsummary_delete(limits);
	}

	jx_insert_integer(j, "time_when_submitted", t->time_when_submitted);
	jx_insert_integer(j, "time_when_commit_start", t->time_when_commit_start);
	jx_insert_integer(j, "time_when_commit_end", t->time_when_commit_end);
	jx_insert_integer(j, "current_time", timestamp_get());

	priority_add_to_jx(j, t->priority);

	return j;
}

/*
Converts a task into a JSON string for the purposes of provenance.
This function must include all of the functional inputs to a task
that affect its outputs (command, environment, sandbox) but not
performance and resource details that do not affect the output.
*/

char * vine_task_to_json(struct vine_task *t)
{
	char * buffer;
	char * file_buffer;

	struct vine_mount *m;

	buffer = string_format("{\ncmd = \"%s\"\n", t->command_line);

	if(t->input_mounts){
		buffer = string_combine(buffer, "inputs = ");
		LIST_ITERATE(t->input_mounts,m) {
			file_buffer = string_format("{ name: \"%s\", content: \"%s\"}, ", m->remote_name, m->file->cached_name);
			buffer = string_combine(buffer, file_buffer);
			free(file_buffer);
		}
		buffer = string_combine(buffer, "\n");
	}

	if(t->output_mounts){
		buffer = string_combine(buffer, "outputs = ");
		LIST_ITERATE(t->output_mounts,m) {
			file_buffer = string_format("{ name: \"%s\" }, ", m->remote_name);
			buffer = string_combine(buffer, file_buffer);
			free(file_buffer);
		}
		buffer = string_combine(buffer, "\n");
	}
	return buffer;
}

