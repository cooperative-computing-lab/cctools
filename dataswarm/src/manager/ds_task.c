/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_manager.h"
#include "ds_task.h"
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

	if(command_line) t->command_line = xxstrdup(command_line);

	t->worker_selection_algorithm = DS_SCHEDULE_UNSET;
	t->input_files = list_create();
	t->output_files = list_create();
	t->env_list = list_create();
	t->exit_code = -1;

	t->result = DS_RESULT_UNKNOWN;

	t->resource_request   = CATEGORY_ALLOCATION_FIRST;

	/* In the absence of additional information, a task consumes an entire worker. */
	t->resources_requested = rmsummary_create(-1);
	t->resources_measured  = rmsummary_create(-1);
	t->resources_allocated = rmsummary_create(-1);

	t->category = xxstrdup("default");

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

	free(t->host);
	t->host = NULL;

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
}

static struct list *ds_task_file_list_clone(struct list *list)
{
	struct list *new = list_create();
	struct ds_file *old_file, *new_file;

	list_first_item(list);
	while ((old_file = list_next_item(list))) {
		new_file = ds_file_clone(old_file);
		list_push_tail(new, new_file);
	}
	return new;
}

static struct list *ds_task_env_list_clone(struct list *env_list)
{
	struct list *new = list_create();
	char *var;
	list_first_item(env_list);
	while((var=list_next_item(env_list))) {
		list_push_tail(new, xxstrdup(var));
	}

	return new;
}


struct ds_task *ds_task_clone(const struct ds_task *task)
{
	struct ds_task *new = ds_task_create(task->command_line);

	if(task->tag) {
		ds_task_specify_tag(new, task->tag);
	}

	if(task->category) {
		ds_task_specify_category(new, task->category);
	}

	ds_task_specify_algorithm(new, task->worker_selection_algorithm);
	ds_task_specify_priority(new, task->priority);
	ds_task_specify_max_retries(new, task->max_retries);
	ds_task_specify_running_time_min(new, task->min_running_time);


	if(task->monitor_output_directory) {
		ds_task_specify_monitor_output(new, task->monitor_output_directory);
	}

	if(task->monitor_snapshot_file) {
		ds_task_specify_snapshot_file(new, task->monitor_snapshot_file);
	}

	new->input_files  = ds_task_file_list_clone(task->input_files);
	new->output_files = ds_task_file_list_clone(task->output_files);
	new->env_list     = ds_task_env_list_clone(task->env_list);

	if(task->features) {
		new->features = list_create();
		char *req;
		list_first_item(task->features);
		while((req = list_next_item(task->features))) {
			list_push_tail(new->features, xxstrdup(req));
		}
	}

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
	if(!t->features) {
		return;
	}

	struct list_cursor *c = list_cursor_create(t->features);

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

void ds_task_specify_environment_variable( struct ds_task *t, const char *name, const char *value )
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

	if(!t->features) {
		t->features = list_create();
	}

	list_push_tail(t->features, xxstrdup(name));
}

int ds_task_specify_url(struct ds_task *t, const char *file_url, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags)
{
	struct list *files;
	struct ds_file *tf;

	if(!t || !file_url || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, url, and remote name not allowed in specify_url.\n");
		return 0;
	}
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == DS_INPUT) {
		files = t->input_files;

		//check if two different urls map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(file_url, tf->source)) {
				fprintf(stderr, "Error: input url %s conflicts with another input pointing to same remote name (%s).\n", file_url, remote_name);
				return 0;
			}
		}
		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: input url %s conflicts with an output pointing to same remote name (%s).\n", file_url, remote_name);
				return 0;
			}
		}
	} else {
		fprintf(stderr, "Error: ds_specify_url does not yet support output files.\n");
	  	return 0;
	}

	tf = ds_file_create(file_url, remote_name, DS_URL, flags);
	if(!tf) return 0;

	// length of source data is not known yet.
	tf->length = 0;

	list_push_tail(files, tf);

	return 1;
}

int ds_task_specify_file(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags)
{
	struct list *files;
	struct ds_file *tf;

	if(!t || !local_name || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, local name, and remote name not allowed in specify_file.\n");
		return 0;
	}

	// @param remote_name is the path of the file as on the worker machine. In
	// the Data Swarm framework, workers are prohibited from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}


	if(type == DS_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(local_name, tf->source)){
				fprintf(stderr, "Error: input file %s conflicts with another input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: input file %s conflicts with an output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	} else {
		files = t->output_files;

		//check if two different different remote names map to the same local name for outputs.
		list_first_item(files);
		while((tf = (struct ds_file*)list_next_item(files))) {
			if(!strcmp(local_name, tf->source) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: output file %s conflicts with another output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: output file %s conflicts with an input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	}

	tf = ds_file_create(local_name, remote_name, DS_FILE, flags);
	if(!tf) return 0;

	list_push_tail(files, tf);
	return 1;
}

int ds_task_specify_directory(struct ds_task *t, const char *local_name, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags, int recursive) {
	struct list *files;
	struct ds_file *tf;

	if(!t || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task and remote name not allowed in specify_directory.\n");
		return 0;
	}

	// @param remote_name is the path of the file as on the worker machine. In
	// the Data Swarm framework, workers are prohibited from writing to paths
	// outside of their workspaces. When a task is specified, the workspace of
	// the worker(the worker on which the task will be executed) is unlikely to
	// be known. Thus @param remote_name should not be an absolute path.
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == DS_OUTPUT || recursive) {
		return ds_task_specify_file(t, local_name, remote_name, type, flags);
	}

	files = t->input_files;

	list_first_item(files);
	while((tf = (struct ds_file*)list_next_item(files))) {
		if(!strcmp(remote_name, tf->remote_name))
		{	return 0;	}
	}

	//KNOWN HACK: Every file passes through make_cached_name() which expects the
	//source field to be set. So we simply set the source to remote name if
	//local name is null. This doesn't affect the behavior of the file transfers.
	const char *source = local_name ? local_name : remote_name;

	tf = ds_file_create(source, remote_name, DS_DIRECTORY, flags);
	if(!tf) return 0;

	list_push_tail(files, tf);
	return 1;

}

int ds_task_specify_file_piece(struct ds_task *t, const char *local_name, const char *remote_name, off_t start_byte, off_t end_byte, ds_file_type_t type, ds_file_flags_t flags)
{
	struct list *files;
	struct ds_file *tf;
	if(!t || !local_name || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task, local name, and remote name not allowed in specify_file_piece.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// ds_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(end_byte < start_byte) {
		fprintf(stderr, "Error: End byte lower than start byte for %s.\n", remote_name);
		return 0;
	}

	if(type == DS_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(local_name, tf->source)){
				fprintf(stderr, "Error: piece of input file %s conflicts with another input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: piece of input file %s conflicts with an output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	} else {
		files = t->output_files;

		//check if two different different remote names map to the same local name for outputs.
		list_first_item(files);
		while((tf = (struct ds_file*)list_next_item(files))) {
			if(!strcmp(local_name, tf->source) && strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: piece of output file %s conflicts with another output pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}

		//check if there is an input file with the same remote name.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)){
				fprintf(stderr, "Error: piece of output file %s conflicts with an input pointing to same remote name (%s).\n", local_name, remote_name);
				return 0;
			}
		}
	}

	tf = ds_file_create(local_name, remote_name, DS_FILE_PIECE, flags);
	if(!tf) return 0;

	tf->offset = start_byte;
	tf->piece_length = end_byte - start_byte + 1;

	list_push_tail(files, tf);
	return 1;
}

int ds_task_specify_buffer(struct ds_task *t, const char *data, int length, const char *remote_name, ds_file_flags_t flags)
{
	struct ds_file *tf;
	if(!t || !remote_name) {
		fprintf(stderr, "Error: Null arguments for task and remote name not allowed in specify_buffer.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// ds_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	list_first_item(t->input_files);
	while((tf = (struct ds_file*)list_next_item(t->input_files))) {
		if(!strcmp(remote_name, tf->remote_name)) {
			fprintf(stderr, "Error: buffer conflicts with another input pointing to same remote name (%s).\n", remote_name);
			return 0;
		}
	}

	list_first_item(t->output_files);
	while((tf = (struct ds_file*)list_next_item(t->input_files))) {
		if(!strcmp(remote_name, tf->remote_name)) {
			fprintf(stderr, "Error: buffer conflicts with an output pointing to same remote name (%s).\n", remote_name);
			return 0;
		}
	}

	tf = ds_file_create(NULL, remote_name, DS_BUFFER, flags);
	if(!tf) return 0;

	tf->source = malloc(length);
	if(!tf->source) {
		fprintf(stderr, "Error: failed to allocate memory for buffer with remote name %s and length %d bytes.\n", remote_name, length);
		return 0;
	}

	tf->length  = length;

	memcpy(tf->source, data, length);
	list_push_tail(t->input_files, tf);

	return 1;
}

int ds_task_specify_file_command(struct ds_task *t, const char *cmd, const char *remote_name, ds_file_type_t type, ds_file_flags_t flags)
{
	struct list *files;
	struct ds_file *tf;
	if(!t || !remote_name || !cmd) {
		fprintf(stderr, "Error: Null arguments for task, remote name, and command not allowed in specify_file_command.\n");
		return 0;
	}

	// @param remote_name should not be an absolute path. @see
	// ds_task_specify_file
	if(remote_name[0] == '/') {
		fatal("Error: Remote name %s is an absolute path.\n", remote_name);
	}

	if(type == DS_INPUT) {
		files = t->input_files;

		//check if two different local names map to the same remote name for inputs.
		list_first_item(t->input_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name) && strcmp(cmd, tf->source)){
				fprintf(stderr, "Error: input file command %s conflicts with another input pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}

		//check if there is an output file with the same remote name.
		list_first_item(t->output_files);
		while((tf = (struct ds_file*)list_next_item(t->input_files))) {
			if(!strcmp(remote_name, tf->remote_name)) {
				fprintf(stderr, "Error: input file command %s conflicts with an output pointing to same remote name (%s).\n", cmd, remote_name);
				return 0;
			}
		}
	} else {
		fprintf(stderr, "Error: ds_specify_file_command does not yet support output files.\n");
	  	return 0;
	}

	if(strstr(cmd, "%%") == NULL) {
		fatal("command to transfer file does not contain %%%% specifier: %s", cmd);
	}

	tf = ds_file_create(cmd, remote_name, DS_REMOTECMD, flags);
	if(!tf) return 0;

	// length of source data is not known yet.
	tf->length = 0;

	list_push_tail(files, tf);

	return 1;
}

int ds_task_specify_snapshot_file(struct ds_task *t, const char *monitor_snapshot_file) {

	assert(monitor_snapshot_file);

	free(t->monitor_snapshot_file);
	t->monitor_snapshot_file = xxstrdup(monitor_snapshot_file);

	return ds_task_specify_file(t, monitor_snapshot_file, RESOURCE_MONITOR_REMOTE_NAME_EVENTS, DS_INPUT, DS_CACHE);

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
	struct ds_file *tf;
	if(t) {

		free(t->command_line);
		free(t->coprocess);
		free(t->tag);
		free(t->category);
		free(t->output);

		if(t->input_files) {
			while((tf = list_pop_tail(t->input_files))) {
				ds_file_delete(tf);
			}
			list_delete(t->input_files);
		}
		if(t->output_files) {
			while((tf = list_pop_tail(t->output_files))) {
				ds_file_delete(tf);
			}
			list_delete(t->output_files);
		}
		if(t->env_list) {
			char *var;
			while((var=list_pop_tail(t->env_list))) {
				free(var);
			}
			list_delete(t->env_list);
		}

		if(t->features) {
			char *feature;
			while((feature=list_pop_tail(t->features))) {
				free(feature);
			}
			list_delete(t->features);
		}

		free(t->hostname);
		free(t->host);

		rmsummary_delete(t->resources_requested);
		rmsummary_delete(t->resources_measured);
		rmsummary_delete(t->resources_allocated);

		free(t->monitor_output_directory);
		free(t->monitor_snapshot_file);
		free(t);
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
