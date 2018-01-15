/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_task.h"
#include "stringtools.h"
#include "xxmalloc.h"

struct batch_task *batch_task_create(struct batch_queue *queue)
{
	struct batch_task *t = malloc(sizeof(struct batch_task));

	t->taskid = 0;
	t->jobid = 0;

	t->queue = queue;

	t->command = NULL;

	t->input_files = list_create();
	t->output_files = list_create();

	t->resources = NULL;

	t->envlist = NULL;

	return t;
}

void batch_task_delete(struct batch_task *t)
{
	if(t->command)
		free(t->command);

	struct batch_file *f;
	list_first_item(t->input_files);
	while((f = list_next_item(t->input_files))){
		batch_file_delete(f);
	}
	list_delete(t->input_files);

	list_first_item(t->output_files);
	while((f = list_next_item(t->output_files))){
		batch_file_delete(f);
	}
	list_delete(t->output_files);

	rmsummary_delete(t->resources);

	jx_delete(t->envlist);

	free(t);
}

struct batch_file * batch_task_add_input_file(struct batch_task *task, char * host_name, char * exe_name)
{
	struct batch_file *f = batch_file_create(task->queue, host_name, exe_name);
    list_push_tail(task->input_files, f);

	return f;

}

struct batch_file * batch_task_add_output_file(struct batch_task *task, char * host_name, char * exe_name)
{
	struct batch_file *f = batch_file_create(task->queue, host_name, exe_name);
    list_push_tail(task->output_files, f);

	return f;
}

void batch_task_wrap_command(struct batch_task *t, char *command)
{
    if(!command) return; 

    char *id = string_format("%d",t->taskid);
    char *wrap_tmp = string_replace_percents(command, id);

    free(id);

    char *result = string_wrap_command(t->command, wrap_tmp);
    free(wrap_tmp);

	free(t->command);
	t->command = result;
}

/* vim: set noexpandtab tabstop=4: */
