/*
 Copyright (C) 2022 The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "batch_job.h"
#include "batch_wrapper.h"
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "dag_file.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"
#include "makeflow_hook.h"

#define CONTAINER_DOCKER_SH "./docker.wrapper.sh_"

struct docker_instance {
	char *image;
	char *tar;
	char *opt;
};

struct docker_instance *docker_instance_create()
{
	struct docker_instance *d = malloc(sizeof(*d));
	d->image = NULL;
	d->tar = NULL;
	d->opt = NULL;

	return d;
}


static int create( void ** instance_struct, struct jx *hook_args )
{
	struct docker_instance *d = docker_instance_create();
	*instance_struct = d;

	if(jx_lookup_string(hook_args, "docker_container_image")){
		d->image = xxstrdup(jx_lookup_string(hook_args, "docker_container_image"));	
	} else {
		debug(D_ERROR|D_MAKEFLOW_HOOK, "Docker hook requires container image name to be specified");
		return MAKEFLOW_HOOK_FAILURE;
	}

	if(jx_lookup_string(hook_args, "docker_container_tar")){
		d->tar = xxstrdup(jx_lookup_string(hook_args, "docker_container_tar"));	
	}

	if(jx_lookup_string(hook_args, "docker_container_opt")){
		d->opt = xxstrdup(jx_lookup_string(hook_args, "docker_container_opt"));	
	} else {
		d->opt = xxstrdup("");
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d )
{
	struct docker_instance *dock = (struct docker_instance*)instance_struct;
	if(dock){
		free(dock->image);
		free(dock->tar);
		free(dock->opt);
		free(dock);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check( void * instance_struct, struct dag *d){
	char *cwd = path_getcwd();
	if(!strncmp(cwd, "/afs", 4)) {
		fprintf(stderr,"error: The working directory is '%s'\n", cwd);
		fprintf(stderr,"This won't work because Docker cannot mount an AFS directory.\n");
		fprintf(stderr,"Instead, run your workflow from a local disk like /tmp.");
		fprintf(stderr,"Or, use the Work Queue batch system with -T wq.\n");
		free(cwd);
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cwd);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_job *t){
	struct docker_instance *d = (struct docker_instance*)instance_struct;
	
	struct batch_wrapper *wrapper = batch_wrapper_create();
	batch_wrapper_prefix(wrapper, CONTAINER_DOCKER_SH);

	/* Save the directory we were originally working in. */
	batch_wrapper_pre(wrapper, "export CUR_WORK_DIR=$(pwd)");
	batch_wrapper_pre(wrapper, "export DEFAULT_DIR=/root/worker");

	if (d->tar == NULL) {
		char *pull = string_format("flock /tmp/lockfile /usr/bin/docker pull %s", d->image);
		batch_wrapper_pre(wrapper, pull);
		free(pull);
	} else {
		char *load = string_format("flock /tmp/lockfile /usr/bin/docker load < %s", d->tar);
		batch_wrapper_pre(wrapper, load);
		free(load);
		makeflow_hook_add_input_file(n->d, t, d->tar, NULL, DAG_FILE_TYPE_GLOBAL);
	}

	char *cmd = string_format("docker run --rm -v $CUR_WORK_DIR:$DEFAULT_DIR -w $DEFAULT_DIR %s %s %s", 
			d->opt, d->image, t->command);
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
	
struct makeflow_hook makeflow_hook_docker = {
	.module_name = "Docker",
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,

	.node_submit = node_submit,
};


