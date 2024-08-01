/*
 Copyright (C) 2022 The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */


#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "debug.h"
#include "list.h"
#include "xxmalloc.h"
#include "path.h"
#include "stringtools.h"

#include "batch_queue.h"
#include "batch_job.h"
#include "batch_wrapper.h"

#include "dag.h"
#include "dag_node.h"

#include "makeflow_hook.h"
#include "makeflow_log.h"

struct umbrella_instance {
	char *spec;
	char *binary;
	char *log_prefix;
	char *mode;
};

struct umbrella_instance *umbrella_instance_create()
{
	struct umbrella_instance *u = malloc(sizeof(*u));
	u->spec = NULL;
	u->binary = NULL;
	u->log_prefix = NULL;
	u->mode = NULL;

	return u;
}

/* Umbrella could feasibly have multiple invocations at
 * different levels, but this is not currently implemented.
 * The complexity of having passing multiple umbrella
 * instances, how they interact, and how we specify specs
 * properly is not being solved here. Only one instance
 * is currently allowed.
 *
 * Additionally, it was previously decided it was 
 * incompatible with Parrot Enforcement. This should be 
 * re-assessed at a later time.
 */

static int register_hook(struct makeflow_hook *h, struct list *hooks, struct jx **args)
{
	struct makeflow_hook *hook;
	list_first_item(hooks);
	while((hook = list_next_item(hooks))){
		if(hook->module_name){
			if(!strcmp(hook->module_name, h->module_name)){
				return MAKEFLOW_HOOK_SKIP;
			} else if(!strcmp(hook->module_name, "Parrot Enforcement")){
				debug(D_MAKEFLOW_HOOK, "Module %s is incompatible with Parrot Enforcement.\n", h->module_name);
				return MAKEFLOW_HOOK_FAILURE;
			}
		}
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int create( void ** instance_struct, struct jx *hook_args ){
	struct umbrella_instance *u = umbrella_instance_create();
	*instance_struct = u;

	if(jx_lookup_string(hook_args, "umbrella_spec")){
		u->spec = xxstrdup(jx_lookup_string(hook_args, "umbrella_spec"));	
		debug(D_MAKEFLOW_HOOK, "setting umbrella spec to %s\n", u->spec);
	}

	if(jx_lookup_string(hook_args, "umbrella_binary")){
		u->binary = xxstrdup(jx_lookup_string(hook_args, "umbrella_binary"));	
		debug(D_MAKEFLOW_HOOK, "setting umbrella binary to %s\n", u->binary);
	}
 
	if(jx_lookup_string(hook_args, "umbrella_log_prefix")) {
		u->log_prefix = string_format("%s.%%", (jx_lookup_string(hook_args, "umbrella_log_prefix")));	
		debug(D_MAKEFLOW_HOOK, "setting umbrella log_prefix to %s\n", u->log_prefix);
	}

	if(jx_lookup_string(hook_args, "umbrella_mode")) {
		u->mode = xxstrdup(jx_lookup_string(hook_args, "umbrella_mode"));	
	} else { 
		u->mode = xxstrdup("local");
	}
	debug(D_MAKEFLOW_HOOK, "setting umbrella mode to %s\n", u->mode);

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d ){
	struct umbrella_instance *u = (struct umbrella_instance*)instance_struct;
	if(u) {
		free(u->spec);
		free(u->binary);
		free(u->log_prefix);
		free(u->mode);
		free(u);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check( void * instance_struct, struct dag *d ){
	struct umbrella_instance *u = (struct umbrella_instance*)instance_struct;
	struct stat st;
	if(u->spec){
		if(stat(u->spec, &st) == -1) {
			debug(D_NOTICE, "stat on %s failed: %s\n", u->spec, strerror(errno));
			return MAKEFLOW_HOOK_FAILURE;
		}
		if((st.st_mode & S_IFMT) != S_IFREG) {
			debug(D_NOTICE, "umbrella spec should specify a regular file\n");
			return MAKEFLOW_HOOK_FAILURE;
		}
	} else {
		debug(D_NOTICE, "no general umbrella spec specified.\n");
		return MAKEFLOW_HOOK_FAILURE;
	}

	if(u->binary){
		if(stat(u->binary, &st) == -1) {
			debug(D_NOTICE, "stat on %s failed: %s\n", u->binary, strerror(errno));
			return MAKEFLOW_HOOK_FAILURE;
		}
		if((st.st_mode & S_IFMT) != S_IFREG) {
			debug(D_NOTICE, "Umbrella binary should specify a regular file\n");
			return MAKEFLOW_HOOK_FAILURE;
		}
	} else {
		debug(D_MAKEFLOW_HOOK, "umbrella binary is not set, therefore an umbrella binary should be available on an execution node if umbrella is used to deliver the execution environment.\n");
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_start( void * instance_struct, struct dag *d ){
	struct umbrella_instance *u = (struct umbrella_instance*)instance_struct;
	/* If no log_prefix is set use the makeflow name as a starter. */
	if(!u->log_prefix){
		u->log_prefix = string_format("%s.umbrella.log.", d->filename);
		debug(D_MAKEFLOW_HOOK, "setting wrapper_umbrella->log_prefix to %s\n", u->log_prefix);
	}

	/* This loop exists to pull specs that are specific to each node. */
	struct dag_node *cur;
	cur = d->nodes;
	while(cur) {
		struct dag_variable_lookup_set s = {d, cur->category, cur, NULL};
		char *spec = NULL;
		spec = dag_variable_lookup_string("SPEC", &s);
		if(spec) {
			debug(D_MAKEFLOW_RUN, "setting dag_node->umbrella_spec (rule %d) from the makefile ...\n", cur->nodeid);
			dag_node_set_umbrella_spec(cur, xxstrdup(spec));
		}
		free(spec);
		cur = cur->next;
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

// the caller should free the result.
char *makeflow_umbrella_print_files(struct list *files, bool is_output) {
	struct batch_file *f;
	char *result = xxstrdup("");

	// construct the --output or --inputs option of umbrella based on files
	list_first_item(files);
	while((f = list_next_item(files))){
		result = string_combine(result, f->outer_name);
		result = string_combine(result, "=");
		result = string_combine(result, f->inner_name);

		if(is_output) result = string_combine(result, ":f,");
		else result = string_combine(result, ",");
	}

	return result;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_job *t)
{
	struct umbrella_instance *u = (struct umbrella_instance*)instance_struct;
    struct batch_wrapper *wrapper = batch_wrapper_create();
    batch_wrapper_prefix(wrapper, "./umbrella");

	if(n->umbrella_spec){
		makeflow_hook_add_input_file(n->d, t, n->umbrella_spec, path_basename(n->umbrella_spec), DAG_FILE_TYPE_GLOBAL);
	} else {
		makeflow_hook_add_input_file(n->d, t, u->spec, path_basename(u->spec), DAG_FILE_TYPE_GLOBAL);
	}

	char *umbrella_input_opt = makeflow_umbrella_print_files(t->input_files, false);
	debug(D_MAKEFLOW_HOOK, "umbrella input opt: %s\n", umbrella_input_opt);

	char *umbrella_output_opt = makeflow_umbrella_print_files(t->output_files, true);
	debug(D_MAKEFLOW_HOOK, "umbrella output opt: %s\n", umbrella_output_opt);

	// Binary is added after inputs are specified to prevent umbrella being passed into itself
	// Not always breaking, but allows umbrella executable to be located at absolute path outside of docker
	if(u->binary) makeflow_hook_add_input_file(n->d, t, u->binary, path_basename(u->binary), DAG_FILE_TYPE_GLOBAL);

	char *log = string_format("%s%d", u->log_prefix, n->nodeid);
	struct dag_file *log_file = makeflow_hook_add_output_file(n->d, t, log, NULL, DAG_FILE_TYPE_INTERMEDIATE);
	free(log);

	char *local_binary = NULL;
	/* If no binary is specified always assume umbrella is in path. */
	if (!u->binary) {
		local_binary = xxstrdup("umbrella");
	/* If remote rename isn't allowed pass binary as specified locally. */
	} else if (!batch_queue_supports_feature(makeflow_get_queue(n), "remote_rename")) {
		local_binary = xxstrdup(u->binary);
	/* If we have the binary and can remotely rename, use ./binary (usually umbrella). */
	} else {
		local_binary = string_format("./%s",path_basename(u->binary));
	}

	char *local_spec = NULL;
	/* If the node has a specific spec listed use this. */
	if(n->umbrella_spec){
		/* Use the basename if we can remote rename. */
		if (!batch_queue_supports_feature(makeflow_get_queue(n), "remote_rename")) {
			local_spec = xxstrdup(n->umbrella_spec);
		} else {
			local_spec = xxstrdup(path_basename(n->umbrella_spec));
		}
	/* If no specific spec use generic. */
	} else {
		/* Use the basename if we can remote rename. */
		if (!batch_queue_supports_feature(makeflow_get_queue(n), "remote_rename")) {
			local_spec = xxstrdup(u->spec);
		} else {
			local_spec = xxstrdup(path_basename(u->spec));
		}
	}

	char *cmd = string_format("%s --spec \"%s\" --localdir ./umbrella_test --inputs \"%s\" --output \"%s\" --sandbox_mode \"%s\" --log \"%s\" run \"%s\"", 
		local_binary, local_spec, umbrella_input_opt, umbrella_output_opt, u->mode, log_file->filename, t->command);
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

	free(local_binary);
	free(local_spec);
	free(umbrella_input_opt);
	free(umbrella_output_opt);
	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_umbrella = {
	.module_name = "Umbrella",
	.register_hook = register_hook,
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,
	.dag_start = dag_start,

	.node_submit = node_submit,
};



