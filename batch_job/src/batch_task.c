/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_task.h"
#include "stringtools.h"
#include "xxmalloc.h"

/** Creates new batch_task and initializes file lists. */
struct batch_task *batch_task_create(struct batch_queue *queue)
{
	struct batch_task *t = calloc(1,sizeof(*t));

	t->queue = queue;

	t->input_files = list_create();
	t->output_files = list_create();

	t->info = batch_job_info_create();

	return t;
}

/** Deletes task struct and frees contained data. */
void batch_task_delete(struct batch_task *t)
{
	if (!t)
		return;

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

	batch_job_info_delete(t->info);

	free(t);
}

/** Creates new batch_file and adds to inputs. */
struct batch_file * batch_task_add_input_file(struct batch_task *task, const char * outer_name, const char * inner_name)
{
	struct batch_file *f = batch_file_create(task->queue, outer_name, inner_name);
    list_push_tail(task->input_files, f);

	return f;

}

/** Creates new batch_file and adds to outputs. */
struct batch_file * batch_task_add_output_file(struct batch_task *task, const char * outer_name, const char * inner_name)
{
	struct batch_file *f = batch_file_create(task->queue, outer_name, inner_name);
    list_push_tail(task->output_files, f);

	return f;
}

/** Free previous command and strdup passed command. */
void batch_task_set_command(struct batch_task *t, const char *command)
{
	free(t->command);
	t->command = xxstrdup(command);
}

/** Wraps the specified command using string_wrap_command.
 See stringtools for a more detailed example of its use.
 Frees the previously set command after wrapping.
*/
void batch_task_wrap_command(struct batch_task *t, const char *command)
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

/** Sets the resources of batch_task.
 Uses rmsummary_copy to create a deep copy of resources.
*/
void batch_task_set_resources(struct batch_task *t, const struct rmsummary *resources)
{
	rmsummary_delete(t->resources);
	t->resources = rmsummary_copy(resources);
}

/** Sets the envlist of batch_task.
 Uses jx_copy to create a deep copy.
*/
void batch_task_set_envlist(struct batch_task *t, struct jx *envlist)
{
	jx_delete(t->envlist);
	t->envlist = jx_copy(envlist);
}

/** Sets the batch_job_info of batch_task.
 Manually copies data into struct.
 Does not free in current code, but as this become standard
 in batch_job interface we should.
*/
void batch_task_set_info(struct batch_task *t, struct batch_job_info *info)
{
	t->info->submitted = info->submitted;
	t->info->started   = info->started  ;
	t->info->finished  = info->finished ;
	t->info->exited_normally = info->exited_normally;
	t->info->exit_code = info->exit_code;
	t->info->exit_signal = info->exit_signal;
	t->info->disk_allocation_exhausted = info->disk_allocation_exhausted;
}

/* vim: set noexpandtab tabstop=4: */
