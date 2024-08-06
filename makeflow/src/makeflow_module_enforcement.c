/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"
#include "xxmalloc.h"
#include "copy_stream.h"
#include "debug.h"
#include "envtools.h"

#include "list.h"
#include "dag.h"
#include "makeflow_hook.h"
#include "makeflow_log.h"

#include "batch_queue.h"
#include "batch_file.h"
#include "batch_job.h"
#include "batch_wrapper.h"

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

struct parrot_enforce_instance {
	char *parrot_path;
	char *local_parrot_path;
	char *env_replace;
	char *local_env_replace;
	char *enforce_prefix;
	char *mountlist_prefix;
	char *tmp_prefix;
};

struct parrot_enforce_instance *parrot_enforce_instance_create()
{   
	struct parrot_enforce_instance *p = malloc(sizeof(*p));
	p->parrot_path = NULL;
	p->local_parrot_path = NULL;
	p->env_replace = NULL;
	p->local_env_replace = NULL;
	p->enforce_prefix = NULL;
	p->mountlist_prefix = NULL;
	p->tmp_prefix = NULL;
	
	return p;
}

static int register_hook(struct makeflow_hook *h, struct list *hooks, struct jx **args)
{
	struct makeflow_hook *hook;
	list_first_item(hooks);
	while((hook = list_next_item(hooks))){
		if(hook->module_name){
			if(!strcmp(hook->module_name, h->module_name)){
				return MAKEFLOW_HOOK_SKIP;
			} else if(!strcmp(hook->module_name, "Umbrella")){
				debug(D_MAKEFLOW_HOOK, "Module %s is incompatible with Umbrella.\n", h->module_name);
				return MAKEFLOW_HOOK_FAILURE;
			}
		}
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int create( void ** instance_struct, struct jx *hook_args ){
	struct parrot_enforce_instance *p = parrot_enforce_instance_create();
	*instance_struct = p;

	if(jx_lookup_string(hook_args, "parrot_path")){
		p->parrot_path = xxstrdup(jx_lookup_string(hook_args, "parrot_path"));
	} else {
        char parrot_path[1024];
        if(!find_executable("parrot_run", "PATH", parrot_path, 1024)) {
		    debug(D_NOTICE, "parrot_path must be set for parrot enforcement");
		    return MAKEFLOW_HOOK_FAILURE;
        } else {
            p->parrot_path = xxstrdup(parrot_path);
        }
	}
	debug(D_MAKEFLOW_HOOK, "setting Parrot binary path to %s\n", p->parrot_path);

	if(jx_lookup_string(hook_args, "env_replace_path")){
		p->env_replace = xxstrdup(jx_lookup_string(hook_args, "env_replace_path"));
	} else {
        char env_replace[1024];
        if(!find_executable("env_replace", "PATH", env_replace, 1024)) {
		    debug(D_NOTICE, "env_replace must be set for parrot enforcement");
		    return MAKEFLOW_HOOK_FAILURE;
        } else {
            p->env_replace = xxstrdup(env_replace);
        }
	}
	debug(D_MAKEFLOW_HOOK, "setting env_replace binary path to %s\n", p->env_replace);

	p->local_parrot_path = xxstrdup("parrot_run");
	p->local_env_replace = xxstrdup("env_replace");
	p->enforce_prefix = xxstrdup("./enforce");
	p->mountlist_prefix = xxstrdup("mount_");
	p->tmp_prefix = xxstrdup("tmp_");

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d ){
	struct parrot_enforce_instance *p = (struct parrot_enforce_instance*)instance_struct;
	if(p) {
		free(p->parrot_path);
		free(p->local_parrot_path);
		free(p->env_replace);
		free(p->local_env_replace);
		free(p->enforce_prefix);
		free(p->mountlist_prefix);
		free(p->tmp_prefix);
		free(p);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check( void * instance_struct, struct dag *d ){
	struct parrot_enforce_instance *p = (struct parrot_enforce_instance*)instance_struct;

	struct stat st;
	int host_parrot = open(p->parrot_path, O_RDONLY);
	if (host_parrot == -1) {
		debug(D_NOTICE, "could not open parrot at `%s': %s", p->parrot_path, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	fstat(host_parrot, &st);
	if (!(st.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH))) {
		debug(D_NOTICE, "%s is not executable", p->parrot_path);
		return MAKEFLOW_HOOK_FAILURE;
	}
	int local_parrot = open(p->local_parrot_path, O_WRONLY|O_CREAT, S_IRWXU);
	if (local_parrot == -1) {
		debug(D_NOTICE, "could not create local copy of parrot: %s", strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	} else {
		fchmod(local_parrot, 0755);
		if (copy_fd_to_fd(host_parrot, local_parrot) != st.st_size) {
			debug(D_NOTICE, "could not copy parrot: %s -> %s", p->parrot_path, p->local_parrot_path );
			return MAKEFLOW_HOOK_FAILURE;
		}
        dag_file_lookup_or_create(d, p->local_parrot_path);
	}
	close(local_parrot);
	close(host_parrot);

	int host_env_replace = open(p->env_replace, O_RDONLY);
	if (host_env_replace == -1) {
		debug(D_NOTICE, "could not open env_replace at `%s': %s", p->env_replace, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	fstat(host_env_replace, &st);
	if (!(st.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH))) {
		debug(D_NOTICE, "%s is not executable", p->env_replace);
		return MAKEFLOW_HOOK_FAILURE;
	}
	int local_env_replace = open(p->local_env_replace, O_WRONLY|O_CREAT, S_IRWXU);
	if (local_env_replace == -1) {
		debug(D_NOTICE, "could not create local copy of env_replace: %s", strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	} else {
		fchmod(local_env_replace, 0755);
		if (copy_fd_to_fd(host_env_replace, local_env_replace) != st.st_size) {
			debug(D_NOTICE, "could not copy env_replace: %s -> %s", p->env_replace, p->local_env_replace );
			return MAKEFLOW_HOOK_FAILURE;
		}
        dag_file_lookup_or_create(d, p->local_env_replace);
	}
	close(local_env_replace);
	close(host_env_replace);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_job *t)
{
	struct parrot_enforce_instance *p = (struct parrot_enforce_instance*)instance_struct;
	struct batch_wrapper *enforce = batch_wrapper_create();
	batch_wrapper_prefix(enforce, p->enforce_prefix);

	struct dag_file *df = NULL;
	makeflow_hook_add_input_file(n->d, t, p->local_parrot_path, p->local_parrot_path, DAG_FILE_TYPE_GLOBAL);
	makeflow_hook_add_input_file(n->d, t, p->local_env_replace, p->local_env_replace, DAG_FILE_TYPE_GLOBAL);

	char *mountlist_path = string_format("%s%d", p->mountlist_prefix, n->nodeid);
	char *tmp_path = string_format("%s%d", p->tmp_prefix, n->nodeid);

	/* make an invalid mountfile to send */
	FILE *mountlist = fopen(mountlist_path, "w");
	if (mountlist == NULL) {
		fatal("could not create `%s': %s", mountlist_path, strerror(errno));
	}

	fprintf(mountlist, "/\t\trx\n");
	fprintf(mountlist, "/dev/null\trwx\n");
	fprintf(mountlist, "/dev/zero\trwx\n");
	fprintf(mountlist, "/dev/full\trwx\n");
	fprintf(mountlist, "/dev/random\trwx\n");
	fprintf(mountlist, "/dev/urandom\trwx\n");
	fprintf(mountlist, "/home\t\tDENY\n");

	/* We have some X related exceptions in case someone needs to
	 * do some troubleshooting/configuration graphically
	 */
	fprintf(mountlist, "$HOME/.Xauthority\trwx\n");
	fprintf(mountlist, "/tmp/.X11-unix\trwx\n");

	struct batch_file *f = NULL;

	list_first_item(t->input_files);
	while((f=list_next_item(t->input_files))) {
		fprintf(mountlist, "$PWD/%s\trwx\n", f->inner_name);
	}
	list_first_item(t->output_files);
	while((f=list_next_item(t->output_files))) {
		fprintf(mountlist, "$PWD/%s\trwx\n", f->inner_name);
	}
	fclose(mountlist);

	df = makeflow_hook_add_input_file(n->d, t, mountlist_path, mountlist_path, DAG_FILE_TYPE_TEMP);
	makeflow_log_file_state_change(n->d, df, DAG_FILE_STATE_EXISTS);

	/* and generate a wrapper script with the current nodeid */
	char *prefix = string_format("export MOUNTFILE='%s'", mountlist_path);
	batch_wrapper_pre(enforce, prefix);
	free(prefix);

	char *env_replace_cmd = string_format("./%s $PWD/$MOUNTFILE $PWD/mount_tmp_file", p->local_env_replace);
	batch_wrapper_pre(enforce, env_replace_cmd);
    free(env_replace_cmd);
	batch_wrapper_pre(enforce, "mv $PWD/mount_tmp_file $PWD/$MOUNTFILE");

	char *tmp_create = string_format("mkdir -p \"$PWD/%s\"", tmp_path);
	batch_wrapper_pre(enforce, tmp_create);
	free(tmp_create);

	char *tmp_set = string_format("export \"TMPDIR=$PWD/%s\"", tmp_path);
	batch_wrapper_pre(enforce, tmp_set);
	free(tmp_set);

	char *cmd = string_format("./%s -m \"$PWD/$MOUNTFILE\" -- %s", p->local_parrot_path, t->command);
	batch_wrapper_cmd(enforce, cmd);
	free(cmd);

	char *tmp_delete = string_format("rm -rf \"$PWD/%s\"", tmp_path);
	batch_wrapper_post(enforce, tmp_delete);
	free(tmp_delete);

	free(mountlist_path);
	free(tmp_path);

	cmd = batch_wrapper_write(enforce, t);
	if(cmd){
		batch_job_set_command(t, cmd);
		df = makeflow_hook_add_input_file(n->d, t, cmd, cmd, DAG_FILE_TYPE_TEMP);
		debug(D_MAKEFLOW_HOOK, "Wrapper written to %s", df->filename);
		makeflow_log_file_state_change(n->d, df, DAG_FILE_STATE_EXISTS);
	} else {
		debug(D_MAKEFLOW_HOOK, "Failed to create wrapper: errno %d, %s", errno, strerror(errno));
		return MAKEFLOW_HOOK_FAILURE;
	}
	free(cmd);

	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_enforcement = {
	.module_name = "Parrot Enforcement",
	.register_hook = register_hook,
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,

	.node_submit = node_submit,
};
