/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "batch_task.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "path.h"
#include "debug.h"

struct batch_task_wrapper {
	struct list *pre;
	struct list *post;
	struct list *cmd;
};

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
struct batch_file * batch_task_add_input_file(struct batch_task *task, char * name_on_submission, char * name_on_execution)
{
	struct batch_file *f = batch_file_create(task->queue, name_on_submission, name_on_execution);
	list_push_tail(task->input_files, f);

	return f;

}

/** Creates new batch_file and adds to outputs. */
struct batch_file * batch_task_add_output_file(struct batch_task *task, char * name_on_submission, char * name_on_execution)
{
	struct batch_file *f = batch_file_create(task->queue, name_on_submission, name_on_execution);
	list_push_tail(task->output_files, f);

	return f;
}

/** Free previous command and strdup passed command. */
void batch_task_set_command(struct batch_task *t, char *command)
{
	free(t->command);
	t->command = xxstrdup(command);
}

/** Wraps the specified command using string_wrap_command.
 See stringtools for a more detailed example of its use.
 Frees the previously set command after wrapping.
*/
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

/** Sets the resources of batch_task.
 Uses rmsummary_copy to create a deep copy of resources.
*/
void batch_task_set_resources(struct batch_task *t, struct rmsummary *resources)
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

struct batch_task_wrapper *batch_task_wrapper_create(void) {
	return xxcalloc(1, sizeof(struct batch_task_wrapper));
}

void batch_task_wrapper_delete(struct batch_task_wrapper *w) {
	if (!w) return;
	list_free(w->pre);
	list_delete(w->pre);
	list_free(w->post);
	list_delete(w->post);
	list_free(w->cmd);
	list_delete(w->cmd);
	free(w);
}

void batch_task_wrapper_pre(struct batch_task_wrapper *w, const char *cmd) {
	assert(w);
	assert(cmd);
	assert(!w->cmd);

	if (!w->pre) {
		w->pre = list_create();
		assert(w->pre);
	}
	list_push_tail(w->pre, string_escape_shell(cmd));
}

void batch_task_wrapper_argv(struct batch_task_wrapper *w, char *const argv[]) {
	assert(w);
	assert(argv);
	assert(!w->cmd);

	w->cmd = list_create();
	assert(w->cmd);
	for (unsigned i = 0; argv[i]; i++) {
		int rc = list_push_tail(w->cmd, string_escape_shell(argv[i]));
		assert(rc == 1);
	}
}

void batch_task_wrapper_cmd(struct batch_task_wrapper *w, char *const argv[]) {
	assert(w);
	assert(argv);
	assert(!w->cmd);

	w->cmd = list_create();
	assert(w->cmd);
	for (unsigned i = 0; argv[i]; i++) {
		int rc = list_push_tail(w->cmd, string_quote_shell(argv[i]));
		assert(rc == 1);
	}
}

void batch_task_wrapper_post(struct batch_task_wrapper *w, const char *cmd) {
	assert(w);
	assert(cmd);
	assert(!w->cmd);

	if (!w->post) {
		w->post = list_create();
		assert(w->post);
	}
	list_push_tail(w->post, string_escape_shell(cmd));
}

char *batch_task_wrapper_write(struct batch_task_wrapper *w, struct batch_task *task, const char *prefix) {
	assert(w);
	assert(prefix);

	char *name = string_format("%sXXXXXX", prefix);
	int wrapper_fd = mkstemp(name);
	if (wrapper_fd == -1)
			fatal("failed to create wrapper: %s", strerror(errno));

	batch_task_add_input_file(task, name, NULL);

	if (fchmod(wrapper_fd, 0700) == -1)
			fatal("failed to make wrapper executable: %s", strerror(errno));

	FILE *wrapper = fdopen(wrapper_fd, "w");
	if (!wrapper)
			fatal("failed to open wrapper: %s", strerror(errno));

	fprintf(wrapper, "#!/bin/sh\n");
	fprintf(wrapper, "set -e\n");

	if (w->post) {
		// function name unlikely to collide with user's stuff
		fprintf(wrapper, "CLEANUP_76tnb43rr7 () {\n");
		list_first_item(w->post);
		for (const char *c; (c = list_next_item(w->post));) {
			fprintf(wrapper, "eval %s\n", c);
		}
		fprintf(wrapper, "}\n");
		fprintf(wrapper, "trap CLEANUP_76tnb43rr7 EXIT INT TERM\n");
	}

	if (w->pre) {
		list_first_item(w->pre);
		for (const char *c; (c = list_next_item(w->pre));) {
			fprintf(wrapper, "eval %s\n", c);
		}
	}

	if (w->cmd) {
		list_first_item(w->cmd);
		for (const char *c; (c = list_next_item(w->cmd));) {
			fprintf(wrapper, " %s", c);
		}
		fprintf(wrapper, "\n");
	}

	fclose(wrapper);
	return name;
}

/* vim: set noexpandtab tabstop=4: */
