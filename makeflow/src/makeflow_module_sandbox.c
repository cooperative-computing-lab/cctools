/*
 Copyright (C) 2022 The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#include "dag_file.h"
#include "makeflow_log.h"
#include "makeflow_hook.h"

#include "batch_job.h"
#include "batch_wrapper.h"

#include "debug.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

static int makeflow_module_sandbox_register(struct makeflow_hook *h, struct list *hooks, struct jx **args)
{
	*args = jx_object(NULL);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int makeflow_module_sandbox_node_submit( void * instance_struct, struct dag_node *node, struct batch_job *task){
	struct batch_wrapper *wrapper = batch_wrapper_create();
	char *wrap_name = string_format("./task_%d_sandbox", task->taskid);
	batch_wrapper_prefix(wrapper, wrap_name);

	/* Save the directory we were originally working in. */
	batch_wrapper_pre(wrapper, "export CUR_WORK_DIR=$(pwd)");

	/* Create sandbox. This should probably have a hex or random tail to be unique. */
	char *cmd = string_format("export SANDBOX=$(mktemp -d %s_XXXXXX)", wrap_name);
	batch_wrapper_pre(wrapper, cmd);
	free(cmd);
	free(wrap_name);

	struct batch_file *f;
	list_first_item(task->input_files);
	while((f = list_next_item(task->input_files))){
		/* Skip if absolute path. */
		if(f->inner_name[0] == '/') continue;

		/* Add a cp for each file. Not linking as wq may already have done this. Not moving as it may be local. */
		cmd = string_format("mkdir -p $(dirname $SANDBOX/%s) && cp -r %s $SANDBOX/%s", f->inner_name, f->inner_name, f->inner_name);
		batch_wrapper_pre(wrapper, cmd);
		free(cmd);
	}
	/* Enter into sandbox_dir. */
	batch_wrapper_pre(wrapper, "cd $SANDBOX");

	/* Execute the previous levels commmand. */
	batch_wrapper_cmd(wrapper, task->command);

	/* Once the command is finished go back to working dir. */
	batch_wrapper_post(wrapper, "cd $CUR_WORK_DIR");

	list_first_item(task->output_files);
	while((f = list_next_item(task->output_files))){
		/* Skip if absolute path. */
		if(f->inner_name[0] == '/') continue;

		/* Copy out results to expected location. OR TRUE so that lack of one file does not
           prevent other files from being sent back.*/
		cmd = string_format("mkdir -p $(dirname %s) && cp -r $SANDBOX/%s %s || true", f->inner_name, f->inner_name, f->inner_name);
		batch_wrapper_post(wrapper, cmd);
		free(cmd);
	}

	/* Remove and fully wipe out sandbox. */
	batch_wrapper_post(wrapper, "rm -rf $SANDBOX");

	cmd = batch_wrapper_write(wrapper, task);
	if(cmd){
		batch_job_set_command(task, cmd);
		struct dag_file *df = makeflow_hook_add_input_file(node->d, task, cmd, cmd, DAG_FILE_TYPE_TEMP);
		debug(D_MAKEFLOW_HOOK, "Wrapper written to %s", df->filename);
		makeflow_log_file_state_change(node->d, df, DAG_FILE_STATE_EXISTS);
	} else {
		debug(D_MAKEFLOW_HOOK, "Failed to create wrapper: errno %d, %s", errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cmd);

	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_sandbox = {
	.module_name = "Sandbox",
	.register_hook = makeflow_module_sandbox_register,
	.node_submit = makeflow_module_sandbox_node_submit,
};


