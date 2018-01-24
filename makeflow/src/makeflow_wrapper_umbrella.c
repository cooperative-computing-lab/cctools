/*
 * Copyright (C) 2016- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "batch_job.h"
#include "debug.h"
#include "dag.h"
#include "xxmalloc.h"
#include "makeflow_hook.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_umbrella.h"
#include "path.h"
#include "stringtools.h"

struct makeflow_wrapper_umbrella *makeflow_wrapper_umbrella_create() {
	struct makeflow_wrapper_umbrella *w = malloc(sizeof(struct makeflow_wrapper_umbrella));
	if(!w) {
		fatal("malloc failed: %s\n", strerror(errno));
	}

	w->wrapper = makeflow_wrapper_create();
	w->spec = NULL;
	w->binary = NULL;
	w->log_prefix = NULL;
	w->mode = NULL;
	return w;
}

void makeflow_wrapper_umbrella_set_spec(struct makeflow_wrapper_umbrella *w, const char *spec) {
	struct stat st;
	if(lstat(spec, &st) == -1) {
		fatal("lstat(`%s`) failed: %s\n", spec, strerror(errno));
	}
	if((st.st_mode & S_IFMT) != S_IFREG) {
		fatal("the --umbrella-spec option of makeflow should specify a regular file\n");
	}
	w->spec = spec;
	debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->spec to %s\n", spec);
}

void makeflow_wrapper_umbrella_set_binary(struct makeflow_wrapper_umbrella *w, const char *binary) {
	struct stat st;
	if(lstat(binary, &st) == -1) {
		fatal("lstat(`%s`) failed: %s\n", binary, strerror(errno));
	}
	if((st.st_mode & S_IFMT) != S_IFREG) {
		fatal("the --umbrella-binary option of makeflow should binaryify a regular file\n");
	}
	w->binary = binary;
	debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->binary to %s\n", binary);
}

void makeflow_wrapper_umbrella_set_log_prefix(struct makeflow_wrapper_umbrella *w, const char *log_prefix) {
	if(log_prefix && *log_prefix) {
		w->log_prefix = log_prefix;
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->log_prefix to %s\n", w->log_prefix);
	}
}

void makeflow_wrapper_umbrella_set_mode(struct makeflow_wrapper_umbrella *w, const char *mode) {
	if(mode && *mode) {
		w->mode = mode;
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->mode to %s\n", w->mode);
	}
}

void makeflow_wrapper_umbrella_preparation(struct makeflow_wrapper_umbrella *w, struct dag *d) {
	if(!w->binary) {
		debug(D_MAKEFLOW_RUN, "the --umbrella-binary option is not set, therefore an umbrella binary should be available on an execution node if umbrella is used to deliver the execution environment.\n");
	}

	// set wrapper_umbrella->log_prefix to the default value
	if(!w->log_prefix) {
		w->log_prefix = string_format("%s.umbrella.log", d->filename);
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->log_prefix to %s\n", w->log_prefix);
	}

	if(!w->mode) {
		w->mode = "local";
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->mode to %s\n", w->mode);
	}
}

// the caller should free the result.
char *makeflow_umbrella_print_files(struct list *files, bool is_output) {
	struct batch_file *f;
	char *result = "";

	// construct the --output or --inputs option of umbrella based on files
	list_first_item(files);
	while((f = list_next_item(files))){
		result = string_combine(result, f->inner_name);
		result = string_combine(result, "=");
		result = string_combine(result, f->inner_name);

		if(is_output) result = string_combine(result, ":f,");
		else result = string_combine(result, ",");
	}

	return result;
}

void makeflow_wrap_umbrella(struct batch_task *task, struct dag_node *n, struct makeflow_wrapper_umbrella *w, struct batch_queue *queue)
{
	if(n->umbrella_spec){
		makeflow_hook_add_input_file(n->d, task, n->umbrella_spec, path_basename(n->umbrella_spec));
	} else {
		makeflow_hook_add_input_file(n->d, task, w->spec, path_basename(w->spec));
	}

	if(w->binary) makeflow_hook_add_input_file(n->d, task, w->binary, path_basename(w->binary));

	debug(D_MAKEFLOW_HOOK, "input_files: %s\n", batch_files_to_string(queue, task->input_files));
	char *umbrella_input_opt = makeflow_umbrella_print_files(task->input_files, false);
	debug(D_MAKEFLOW_HOOK, "umbrella input opt: %s\n", umbrella_input_opt);

	debug(D_MAKEFLOW_HOOK, "output_files: %s\n", batch_files_to_string(queue, task->output_files));
	char *umbrella_output_opt = makeflow_umbrella_print_files(task->output_files, true);
	debug(D_MAKEFLOW_HOOK, "umbrella output opt: %s\n", umbrella_output_opt);

	struct dag_file *log_file = makeflow_hook_add_output_file(n->d, task, w->log_prefix, NULL);

	char *local_binary = NULL;
	/* If no binary is specified always assume umbrella is in path. */
	if (!w->binary) {
		local_binary = xxstrdup("umbrella");
	/* If remote rename isn't allowed pass binary as specified locally. */
	} else if (!batch_queue_supports_feature(queue, "remote_rename")) {
		local_binary = xxstrdup(w->binary);
	/* If we have the binary and can remotely rename, use ./binary (usually umbrella). */
	} else {
		local_binary = string_format("./%s",path_basename(w->binary));
	}

	char *local_spec = NULL;
	/* If the node has a specific spec listed use this. */
	if(n->umbrella_spec){
		/* Use the basename if we can remote rename. */
		if (!batch_queue_supports_feature(queue, "remote_rename")) {
			local_spec = xxstrdup(n->umbrella_spec);
		} else {
			local_spec = xxstrdup(path_basename(n->umbrella_spec));
		}
	/* If no specific spec use generic. */
	} else {
		/* Use the basename if we can remote rename. */
		if (!batch_queue_supports_feature(queue, "remote_rename")) {
			local_spec = xxstrdup(w->spec);
		} else {
			local_spec = xxstrdup(path_basename(w->spec));
		}
	}

	char *umbrella_command = NULL;

	umbrella_command = string_format("%s --spec \"%s\" \
		--localdir ./umbrella_test \
		--inputs \"%s\" \
		--output \"%s\" \
		--sandbox_mode \"%s\" \
		--log \"%s\" \
		run \'{}\'", local_binary, local_spec, umbrella_input_opt, umbrella_output_opt, w->mode, log_file->filename);

	debug(D_MAKEFLOW_HOOK, "umbrella wrapper command: %s\n", umbrella_command);

	batch_task_wrap_command(task, umbrella_command);
	debug(D_MAKEFLOW_HOOK, "umbrella command: %s\n", task->command);

	free(local_binary);
	free(local_spec);
	free(umbrella_command);
	free(umbrella_input_opt);
	free(umbrella_output_opt);
}
