/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_manager.h"
#include "ds_task.h"
#include "ds_worker_info.h"
#include "ds_file.h"

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

struct ds_task *ds_task_create(const char *command_line)
{
	struct ds_task *t = malloc(sizeof(*t));
	if(!t) {
		fprintf(stderr, "Error: failed to allocate memory for task.\n");
		return NULL;
	}
	memset(t, 0, sizeof(*t));

	/* REMEMBER: Any memory allocation done in this function should have a
	 * corresponding copy in ds_task_clone. Otherwise we get
	 * double-free segfaults. */

	/* For clarity, put initialization in same order as structure. */

	if(command_line) t->command_line = xxstrdup(command_line);
	t->category = xxstrdup("default");

	t->input_files = list_create();
	t->output_files = list_create();
	t->env_list = list_create();
	t->feature_list = list_create();

	t->resource_request   = CATEGORY_ALLOCATION_FIRST;
	t->worker_selection_algorithm = DS_SCHEDULE_UNSET;

	t->state = DS_TASK_READY;

	t->result = DS_RESULT_UNKNOWN;
	t->exit_code = -1;

	/* In the absence of additional information, a task consumes an entire worker. */
	t->resources_requested = rmsummary_create(-1);
	t->resources_measured  = rmsummary_create(-1);
	t->resources_allocated = rmsummary_create(-1);

	return t;
}

void ds_task_clean( struct ds_task *t, int full_clean )
{
	t->time_when_commit_start = 0;
	t->time_when_commit_end   = 0;
	t->time_when_retrieval    = 0;
	t->time_workers_execute_last = 0;

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
		t->fast_abort_count = 0;

		t->time_workers_execute_all = 0;
		t->time_workers_execute_exhaustion = 0;
		t->time_workers_execute_failure = 0;

		rmsummary_delete(t->resources_measured);
		rmsummary_delete(t->resources_allocated);
		t->resources_measured  = rmsummary_create(-1);
		t->resources_allocated = rmsummary_create(-1);
	}

	/* If result is never updated, then it is mark as a failure. */
	t->result = DS_RESULT_UNKNOWN;
	t->state = DS_TASK_READY;
}

static struct list *ds_task_file_list_clone(struct list *list)
{
	struct list *new = list_create();
	struct ds_file *old_file, *new_file;

	LIST_ITERATE(list,old_file) {
		new_file = ds_file_clone(old_file);
		list_push_tail(new, new_file);
	}
	return new;
}

static struct list *ds_task_string_list_clone(struct list *string_list)
{
	struct list *new = list_create();
	char *var;

	LIST_ITERATE(string_list,var) {
		list_push_tail(new, xxstrdup(var));
	}

	return new;
}


struct ds_task *ds_task_clone(const struct ds_task *task)
{
	struct ds_task *new = ds_task_create(task->command_line);

	/* Static features of task are copied. */
	if(task->coprocess) ds_task_specify_coprocess(new,task->tag);
	if(task->tag) ds_task_specify_tag(new, task->tag);
	if(task->category) ds_task_specify_category(new, task->category);

	if(task->monitor_output_directory) {
		ds_task_specify_monitor_output(new, task->monitor_output_directory);
	}

	if(task->monitor_snapshot_file) {
		ds_task_specify_snapshot_file(new, task->monitor_snapshot_file);
	}

	new->input_files  = ds_task_file_list_clone(task->input_files);
	new->output_files = ds_task_file_list_clone(task->output_files);
	new->env_list     = ds_task_string_list_clone(task->env_list);
	new->feature_list = ds_task_string_list_clone(task->feature_list);

	/* Scheduling features of task are copied. */
	new->resource_request = task->resource_request;
	ds_task_specify_algorithm(new, task->worker_selection_algorithm);
	ds_task_specify_priority(new, task->priority);
	ds_task_specify_max_retries(new, task->max_retries);
	ds_task_specify_running_time_min(new, task->min_running_time);

	/* Internal state of task is cleared from ds_task_create */

	/* Results of task are cleared from ds_task_create. */

	/* Metrics of task are cleared from ds_task_create. */

	/* Resource requests are copied. */

	if(task->resources_requested) {
		new->resources_requested = rmsummary_copy(task->resources_requested, 0);
	}

	return new;
}


void ds_task_specify_command( struct ds_task *t, const char *cmd )
{
	if(t->command_line) free(t->command_line);
	t->command_line = xxstrdup(cmd);
}

static void delete_feature(struct ds_task *t, const char *name)
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

void ds_task_specify_coprocess( struct ds_task *t, const char *coprocess )
{
	if(t->coprocess) {
		delete_feature(t, t->coprocess);
		free(t->coprocess);
		t->coprocess = NULL;
	}

	if(coprocess) {
		t->coprocess = string_format("ds_worker_coprocess:%s", coprocess);
		ds_task_specify_feature(t, t->coprocess);
	}
}

void ds_task_specify_env( struct ds_task *t, const char *name, const char *value )
{
	if(value) {
		list_push_tail(t->env_list,string_format("%s=%s",name,value));
	} else {
		/* Specifications without = indicate variables to me unset. */
		list_push_tail(t->env_list,string_format("%s",name));
	}
}

void ds_task_specify_max_retries( struct ds_task *t, int64_t max_retries ) {
	if(max_retries < 1) {
		t->max_retries = 0;
	}
	else {
		t->max_retries = max_retries;
	}
}

void ds_task_specify_memory( struct ds_task *t, int64_t memory )
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

void ds_task_specify_disk( struct ds_task *t, int64_t disk )
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

void ds_task_specify_cores( struct ds_task *t, int cores )
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

void ds_task_specify_gpus( struct ds_task *t, int gpus )
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

void ds_task_specify_end_time( struct ds_task *t, int64_t useconds )
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

void ds_task_specify_start_time_min( struct ds_task *t, int64_t useconds )
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

void ds_task_specify_running_time( struct ds_task *t, int64_t useconds )
{
	if(useconds < 1)
	{
		t->resources_requested->wall_time = -1;
	}
	else
	{
		t->resources_requested->wall_time = DIV_INT_ROUND_UP(useconds, ONE_SECOND);
	}
}

void ds_task_specify_running_time_max( struct ds_task *t, int64_t seconds )
{
	ds_task_specify_running_time(t, seconds);
}

void ds_task_specify_running_time_min( struct ds_task *t, int64_t seconds )
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

void ds_task_specify_resources(struct ds_task *t, const struct rmsummary *rm) {
	if(!rm)
		return;

	ds_task_specify_cores(t,        rm->cores);
	ds_task_specify_memory(t,       rm->memory);
	ds_task_specify_disk(t,         rm->disk);
	ds_task_specify_gpus(t,         rm->gpus);
	ds_task_specify_running_time(t, rm->wall_time);
	ds_task_specify_running_time_max(t, rm->wall_time);
	ds_task_specify_running_time_min(t, t->min_running_time);
	ds_task_specify_end_time(t,     rm->end);
}

void ds_task_specify_tag(struct ds_task *t, const char *tag)
{
	if(t->tag)
		free(t->tag);
	t->tag = xxstrdup(tag);
}

void ds_task_specify_category(struct ds_task *t, const char *category)
{
	if(t->category)
		free(t->category);

	t->category = xxstrdup(category ? category : "default");
}

void ds_task_specify_feature(struct ds_task *t, const char *name)
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

void ds_task_check_consistency( struct ds_task *t )
{
	struct hash_table *table = hash_table_create(0,0);
	struct ds_file *f;

	/* Cannot have multiple input files mapped to the same remote name. */

	LIST_ITERATE(t->input_files,f) {
		if(hash_table_lookup(table,f->remote_name)) {
			fprintf(stderr,"warning: task %d has more than one input file named %s\n",t->taskid,f->remote_name);
		} else {
			hash_table_insert(table,f->remote_name,f->remote_name);
		}
	}

	hash_table_clear(table,0);

	/* Cannot have multiple output files bring back the same file. */

	LIST_ITERATE(t->output_files,f) {
		if(f->type==DS_FILE && hash_table_lookup(table,f->source)) {
			fprintf(stderr,"warning: task %d has more than one output file named %s\n",t->taskid,f->source);
		} else {
			hash_table_insert(table,f->remote_name,f->source);
		}
	}

	hash_table_clear(table,0);
	hash_table_delete(table);
}

static void ds_task_add_input( struct ds_task *t, struct ds_file *f )
{
	if(!t || !f || !f->source || !f->remote_name) {
		fatal("%s: invalid null argument.",__func__);
	}

	if(f->remote_name[0] == '/') {
		fatal("%s: invalid remote name %s: cannot start with a slash.",__func__,f->remote_name);
	}

	list_push_tail(t->input_files, f);
}

static void ds_task_add_output( struct ds_task *t, struct ds_file *f )
{
	if(!t || !f || !f->source || !f->remote_name) {
		fatal("%s: invalid null argument.",__func__);
	}

	if(f->remote_name[0] == '/') {
		fatal("%s: invalid remote name %s: cannot start with a slash.",__func__,f->remote_name);
	}

	list_push_tail(t->output_files, f);
}

void ds_task_specify_input_file(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *f = ds_file_create(local_name, remote_name, 0, 0, DS_FILE, flags);
	ds_task_add_input(t,f);
}

void ds_task_specify_output_file(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *f = ds_file_create(local_name, remote_name, 0, 0, DS_FILE, flags);
	ds_task_add_output(t,f);
}

void ds_task_specify_input_url(struct ds_task *t, const char *file_url, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *f = ds_file_create(file_url, remote_name, 0, 0, DS_URL, flags);
	ds_task_add_input(t,f);
}

void ds_task_specify_empty_dir( struct ds_task *t, const char *remote_name )
{
	struct ds_file *f = ds_file_create("unused", remote_name, 0, 0, DS_EMPTY_DIR, 0);
	ds_task_add_input(t,f);
}

void ds_task_specify_input_piece(struct ds_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, ds_file_flags_t flags)
{
	if(end_byte < start_byte) {
		fatal("%s: end byte lower than start byte for %s.\n",__func__,remote_name);
	}

	struct ds_file *f = ds_file_create(local_name, remote_name, 0, 0, DS_FILE_PIECE, flags);

	f->offset = start_byte;
	f->piece_length = end_byte - start_byte + 1;

	ds_task_add_input(t,f);
}

void ds_task_specify_input_buffer(struct ds_task *t, const char *data, int length, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *f = ds_file_create("unnamed", remote_name, data, length, DS_BUFFER, flags);
	ds_task_add_input(t,f);
}

void ds_task_specify_output_buffer(struct ds_task *t, const char *buffer_name, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *f = ds_file_create(buffer_name, remote_name, 0, 0, DS_BUFFER, flags);
	ds_task_add_output(t,f);
}

void ds_task_specify_input_command(struct ds_task *t, const char *cmd, const char *remote_name, ds_file_flags_t flags)
{
	if(strstr(cmd, "%%") == NULL) {
		fatal("%s: command to transfer file does not contain %%%% specifier: %s", __func__, cmd);
	}

	struct ds_file *f = ds_file_create(cmd, remote_name, 0, 0, DS_COMMAND, flags);
	ds_task_add_input(t,f);
}

void ds_task_specify_snapshot_file(struct ds_task *t, const char *monitor_snapshot_file) {

	assert(monitor_snapshot_file);

	free(t->monitor_snapshot_file);
	t->monitor_snapshot_file = xxstrdup(monitor_snapshot_file);

	ds_task_specify_input_file(t, monitor_snapshot_file, RESOURCE_MONITOR_REMOTE_NAME_EVENTS, DS_CACHE);
}

void ds_task_specify_algorithm(struct ds_task *t, ds_schedule_t algorithm)
{
	t->worker_selection_algorithm = algorithm;
}

void ds_task_specify_priority( struct ds_task *t, double priority )
{
	t->priority = priority;
}

void ds_task_specify_monitor_output(struct ds_task *t, const char *monitor_output_directory) {

	if(!monitor_output_directory) {
		fatal("Error: no monitor_output_file was specified.");
	}

	if(t->monitor_output_directory) {
		free(t->monitor_output_directory);
	}

	t->monitor_output_directory = xxstrdup(monitor_output_directory);
}

int ds_task_update_result(struct ds_task *t, ds_result_t new_result)
{
	if(new_result & ~(0x7)) {
		/* Upper bits are set, so this is not related to old-style result for
		 * inputs, outputs, or stdout, so we simply make an update. */
		t->result = new_result;
	} else if(t->result != DS_RESULT_UNKNOWN && t->result & ~(0x7)) {
		/* Ignore new result, since we only update for input, output, or
		 * stdout missing when no other result exists. This is because
		 * missing inputs/outputs are anyway expected with other kind of
		 * errors. */
	} else if(new_result == DS_RESULT_INPUT_MISSING) {
		/* input missing always appears by itself, so yet again we simply make an update. */
		t->result = new_result;
	} else if(new_result == DS_RESULT_OUTPUT_MISSING) {
		/* output missing clobbers stdout missing. */
		t->result = new_result;
	} else {
		/* we only get here for stdout missing. */
		t->result = new_result;
	}

	return t->result;
}

void ds_task_delete(struct ds_task *t)
{
	if(!t) return;

	free(t->command_line);
	free(t->coprocess);
	free(t->tag);
	free(t->category);

	free(t->monitor_output_directory);
	free(t->monitor_snapshot_file);

	list_clear(t->input_files,(void*)ds_file_delete);
	list_delete(t->input_files);

	list_clear(t->output_files,(void*)ds_file_delete);
	list_delete(t->output_files);

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

static struct ds_file * find_output_buffer( struct ds_task *t, const char *name )
{
	struct ds_file *f;

	LIST_ITERATE(t->output_files,f) {
		if(f->type==DS_BUFFER && !strcmp(f->source,name)) {
			return f;
		}
	}

	return 0;
}

const char * ds_task_get_output_buffer( struct ds_task *t, const char *buffer_name )
{
	struct ds_file *f = find_output_buffer(t,buffer_name);
	if(f) {
		return f->data;
	} else {
		return 0;
	}
}

int ds_task_get_output_buffer_length( struct ds_task *t, const char *buffer_name )
{
	struct ds_file *f = find_output_buffer(t,buffer_name);
	if(f) {
		return f->length;
	} else {
		return 0;
	}
}

const char * ds_task_get_command( struct ds_task *t )
{
	return t->command_line;
}

const char * ds_task_get_tag( struct ds_task *t )
{
	return t->tag;
}

int ds_task_get_taskid( struct ds_task *t )
{
	return t->taskid;
}

const char * ds_task_get_output( struct ds_task *t )
{
	return t->output;
}

int ds_task_get_exit_code( struct ds_task *t )
{
	return t->exit_code;
}

ds_result_t ds_task_get_result( struct ds_task *t )
{
	return t->result;
}

const char * ds_task_get_addrport( struct ds_task *t )
{
	return t->addrport;
}

const char * ds_task_get_hostname( struct ds_task *t )
{
	return t->hostname;
}

#define METRIC(x) if(!strcmp(name,#x)) return t->x;

int64_t ds_task_get_metric( struct ds_task *t, const char *name )
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

const char *ds_task_state_string( ds_task_state_t task_state )
{
	const char *str;

	switch(task_state) {
		case DS_TASK_READY:
			str = "WAITING";
			break;
		case DS_TASK_RUNNING:
			str = "RUNNING";
			break;
		case DS_TASK_WAITING_RETRIEVAL:
			str = "WAITING_RETRIEVAL";
			break;
		case DS_TASK_RETRIEVED:
			str = "RETRIEVED";
			break;
		case DS_TASK_DONE:
			str = "DONE";
			break;
		case DS_TASK_CANCELED:
			str = "CANCELED";
			break;
		case DS_TASK_UNKNOWN:
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

struct jx * ds_task_to_jx( struct ds_manager *q, struct ds_task *t )
{
	struct jx *j = jx_object(0);

	jx_insert_integer(j,"taskid",t->taskid);
	jx_insert_string(j,"state",ds_task_state_string(t->state));
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
		const struct rmsummary *min = ds_manager_task_min_resources(q, t);
		const struct rmsummary *max = ds_manager_task_max_resources(q, t);

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
