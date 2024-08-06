/*
 Copyright (C) 2022 The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "debug.h"
#include "batch_wrapper.h"

#include "dag.h"
#include "dag_file.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_hook.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>

struct vc3_definition {
	char *exe;
	char *opt;
	char *log;
};

struct vc3_definition *vc3_definition_create()
{
	struct vc3_definition *v = malloc(sizeof(*v));
	v->exe = NULL;
	v->opt = NULL;
	v->log = NULL;

	return v;
}

static int create( void ** instance_struct, struct jx *hook_args )
{
	struct vc3_definition *v = vc3_definition_create();
	*instance_struct = v;

	if(jx_lookup_string(hook_args, "vc3_exe")){
		v->exe = xxstrdup(jx_lookup_string(hook_args, "vc3_exe"));	
	} else {
		v->exe = xxstrdup("./vc3-builder");
	}
	debug(D_MAKEFLOW_HOOK, "VC3 Builder exe: %s", v->exe);

	if(jx_lookup_string(hook_args, "vc3_opt")){
		v->opt = xxstrdup(jx_lookup_string(hook_args, "vc3_opt"));	
		debug(D_MAKEFLOW_HOOK, "VC3 Builder opt: %s", v->opt);
	} else {
		v->opt = xxstrdup("");
	}

	if(jx_lookup_string(hook_args, "vc3_log")){
		v->log = xxstrdup(jx_lookup_string(hook_args, "vc3_log"));	
		debug(D_MAKEFLOW_HOOK, "VC3 Builder log: %s", v->log);
	} else {
		v->log = xxstrdup("./vc3_log");
	}


	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d)
{
	struct vc3_definition *v = (struct vc3_definition*)instance_struct;
	free(v->exe);
	free(v->opt);
	free(v->log);
	free(v);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_job *t){
	struct vc3_definition *v = (struct vc3_definition*)instance_struct;
	struct batch_wrapper *wrapper = batch_wrapper_create();
	batch_wrapper_prefix(wrapper, "./vc3_builder_");

	char * executable = NULL;
	// If the queue supports remote_renaming add as remote rename.
	if (batch_queue_supports_feature(makeflow_get_queue(n), "remote_rename")) {
		executable = string_format("./%s", path_basename(v->exe));
	} else {
		// Else just use executable in path
		executable = string_format("%s", v->exe);
	}

	/* Assumes a /disk dir in the image. */
	char *log = string_format("%s_%d", v->log, t->taskid);
	char *task_cmd = string_escape_shell(t->command);
	char *cmd = string_format("%s --home $PWD %s -- %s > %s", executable, v->opt, task_cmd, log);
	makeflow_hook_add_input_file(n->d, t, v->exe, executable, DAG_FILE_TYPE_GLOBAL);
	makeflow_hook_add_output_file(n->d, t, log, log, DAG_FILE_TYPE_INTERMEDIATE);
	free(log);
	free(executable);
	free(task_cmd);
	batch_wrapper_cmd(wrapper, cmd);
	free(cmd);

	cmd = batch_wrapper_write(wrapper, t);
	if(cmd){
		batch_job_set_command(t, cmd);
		struct dag_file *df = makeflow_hook_add_input_file(n->d, t, cmd, cmd, DAG_FILE_TYPE_TEMP);
		debug(D_MAKEFLOW_HOOK, "Wrapper written to %s", df->filename);
		makeflow_log_file_state_change(n->d, df, DAG_FILE_STATE_EXISTS);
	} else {
		debug(D_MAKEFLOW_HOOK, "Failed to create wrapper: errno %d, %s", errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cmd);

	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_vc3_builder = {
	.module_name = "VC3 Builder",
	.create = create,
	.destroy = destroy,

	.node_submit = node_submit,
};


