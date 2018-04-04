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

struct singularity_instance {
	char *image;
	char *opt;
};

struct singularity_instance *singularity_instance_create()
{
	struct singularity_instance *s = malloc(sizeof(*s));
	s->image = NULL;
	s->opt = NULL;

	return s;
}

static int create( void ** instance_struct, struct jx *hook_args )
{
	struct singularity_instance *s = singularity_instance_create();
	*instance_struct = s;

	if(jx_lookup_string(hook_args, "singularity_container_image")){
		s->image = xxstrdup(jx_lookup_string(hook_args, "singularity_container_image"));	
	}

	if(jx_lookup_string(hook_args, "singularity_container_options")){
		s->opt = xxstrdup(jx_lookup_string(hook_args, "singularity_container_options"));	
	} else {
		s->opt = xxstrdup("");
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d)
{
	struct singularity_instance *s = (struct singularity_instance*)instance_struct;
	free(s->image);
	free(s->opt);
	free(s);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_task *t){
	struct singularity_instance *s = (struct singularity_instance*)instance_struct;
	struct batch_wrapper *wrapper = batch_wrapper_create();
	batch_wrapper_prefix(wrapper, CONTAINER_SINGULARITY_SH);

	/* Assumes a /disk dir in the image. */
	char *task_cmd = string_escape_shell(t->command);
	char *cmd = string_format("singularity exec --home $(pwd) %s %s sh -c %s", s->opt, s->image, task_cmd);
	free(task_cmd);
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

	makeflow_hook_add_input_file(n->d, t, s->image, NULL, DAG_FILE_TYPE_GLOBAL);

	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_singularity = {
	.module_name = "Singularity",
	.create = create,
	.destroy = destroy,

	.node_submit = node_submit,
};


