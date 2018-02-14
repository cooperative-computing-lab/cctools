/*
 Copyright (C) 2018- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

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

#define CONTAINER_SINGULARITY_SH "./singularity.wrapper.sh"

char *singularity_image = NULL;
char *singularity_opt   = NULL;

static int create( struct jx *hook_args )
{
	if(jx_lookup_string(hook_args, "singularity_container_image")){
		singularity_image = xxstrdup(jx_lookup_string(hook_args, "singularity_container_image"));	
	}

	if(jx_lookup_string(hook_args, "singularity_container_options")){
		singularity_opt = xxstrdup(jx_lookup_string(hook_args, "singularity_container_options"));	
	} else {
		singularity_opt = xxstrdup("");
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( struct dag *d)
{
	free(singularity_image);
	free(singularity_opt);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit(struct dag_node *n, struct batch_task *t){
	struct batch_wrapper *wrapper = batch_wrapper_create();
	batch_wrapper_prefix(wrapper, CONTAINER_SINGULARITY_SH);

	/* Assumes a /disk dir in the image. */
	char *cmd = string_format("singularity exec --home $(pwd) %s %s %s", singularity_opt, singularity_image, t->command);
	batch_wrapper_cmd(wrapper, cmd);
	free(cmd);

	cmd = batch_wrapper_write(wrapper, t);
	if(cmd){
		batch_task_set_command(t, cmd);
		struct dag_file *df = makeflow_hook_add_input_file(n->d, t, cmd, cmd, DAG_FILE_TYPE_TEMP);
		debug(D_MAKEFLOW_HOOK, "Wrapper written to %s", df->filename);
		makeflow_log_file_state_change(n->d, df, DAG_FILE_STATE_EXISTS);
	} else {
		debug(D_MAKEFLOW_HOOK, "Failed to create wrapper: errno %d, %s", errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cmd);

	makeflow_hook_add_input_file(n->d, t, singularity_image, NULL, DAG_FILE_TYPE_GLOBAL);

	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_singularity = {
	.module_name = "Singularity",
	.create = create,
	.destroy = destroy,

	.node_submit = node_submit,
};


