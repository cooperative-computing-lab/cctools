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

void makeflow_wrapper_umbrella_preparation(struct makeflow_wrapper_umbrella *w, struct batch_queue *queue, struct dag *d) {
	struct dag_node *cur;
	bool remote_rename_support = false;

	if (batch_queue_supports_feature(queue, "remote_rename")) {
		remote_rename_support = true;
	}

	if(!w->binary) {
		debug(D_MAKEFLOW_RUN, "the --umbrella-binary option is not set, therefore an umbrella binary should be available on an execution node if umbrella is used to deliver the execution environment.\n");
		fprintf(stdout, "the --umbrella-binary option is not set, therefore an umbrella binary should be available on an execution node if umbrella is used to deliver the execution environment.\n");
	}

	// add umbrella_spec (if specified) and umbrella_binary (if specified) into the input file list of w->wrapper
	if (!remote_rename_support) {
		if(w->spec) makeflow_wrapper_add_input_file(w->wrapper, w->spec);
		if(w->binary) makeflow_wrapper_add_input_file(w->wrapper, w->binary);
	} else {
		if(w->spec) {
			char *s = string_format("%s=%s", w->spec, path_basename(w->spec));
			if(!s) fatal("string_format for umbrella spec failed: %s.\n", strerror(errno));
			makeflow_wrapper_add_input_file(w->wrapper, s);
			free(s);
		}
		if(w->binary) {
			char *s = string_format("%s=%s", w->binary, path_basename(w->binary));
			if(!s) fatal("string_format for umbrella binary failed: %s.\n", strerror(errno));
			makeflow_wrapper_add_input_file(w->wrapper, s);
			free(s);
		}
	}

	// set wrapper_umbrella->log_prefix to the default value
	if(!w->log_prefix) {
		w->log_prefix = string_format("%s.umbrella.log", d->filename);
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->log_prefix to %s\n", w->log_prefix);
	}

	// check whether the umbrella log files already exist
	debug(D_MAKEFLOW_RUN, "checking whether the umbrella log files already exist...\n");
	cur = d->nodes;
	while(cur) {
		char *umbrella_logfile = NULL;

		if(!cur->umbrella_spec) {
			cur = cur->next;
			continue;
		}

		umbrella_logfile = string_format("%s.%d", w->log_prefix, cur->nodeid);

		if(!access(umbrella_logfile, F_OK)) {
			fprintf(stderr, "the umbrella log file for rule %d (`%s`) already exists!\n", cur->nodeid, umbrella_logfile);
			free(umbrella_logfile);
			exit(EXIT_FAILURE);
		}

		// add umbrella_logfile into the target files of a dag_node
		if(remote_rename_support) {
			dag_node_add_target_file(cur, umbrella_logfile, umbrella_logfile);
		} else {
			dag_node_add_target_file(cur, umbrella_logfile, NULL);
		}
		free(umbrella_logfile);
		cur = cur->next;
	}

	if(!w->mode) {
		w->mode = "local";
		debug(D_MAKEFLOW_RUN, "setting wrapper_umbrella->mode to %s\n", w->mode);
	}
}

// the caller should free the result.
char *create_umbrella_opt(bool remote_rename_support, char *files, bool is_output, const char *umbrella_logfile) {
	char *s = files;
	size_t size;
	char *result = NULL;

	// the result will be freed by the caller, therefore returning a copy of s is needed.
	// Returning the original copy and free the original copy may cuase memory corruption.
	if(!strcmp(s, "")) return xxstrdup(s);

	// construct the --output or --inputs option of umbrella based on files
	while((size = strcspn(s, ",\0")) > 0) {
		char *t;
		s[size] = '\0';

		if(!remote_rename_support) {
			t = s;
		} else {
			t = strchr(s, '=');
			t++;
		}

		// avoid adding umbrella_logfile into umbrella_output_opt
		if(strcmp(t, umbrella_logfile)) {
			result = string_combine(result, t);
			result = string_combine(result, "=");
			result = string_combine(result, t);

			if(is_output) result = string_combine(result, ":f,");
			else result = string_combine(result, ",");
		}

		s[size] = ',';
		s += size+1;
	}

	return result;
}

char *makeflow_wrap_umbrella(char *result, struct dag_node *n, struct makeflow_wrapper_umbrella *w, struct batch_queue *queue, char *input_files, char *output_files) {
	if(!n->umbrella_spec) return result;

	char *umbrella_command = NULL;
	char *umbrella_input_opt = NULL;
	char *umbrella_output_opt = NULL;
	char *umbrella_logfile = NULL;
	bool remote_rename_support = false;

	if (batch_queue_supports_feature(queue, "remote_rename")) {
		remote_rename_support = true;
	}

	umbrella_logfile = string_format("%s.%d", w->log_prefix, n->nodeid);

	debug(D_MAKEFLOW_RUN, "input_files: %s\n", input_files);
	umbrella_input_opt = create_umbrella_opt(remote_rename_support, input_files, false, umbrella_logfile);
	debug(D_MAKEFLOW_RUN, "umbrella input opt: %s\n", umbrella_input_opt);

	debug(D_MAKEFLOW_RUN, "output_files: %s\n", output_files);
	umbrella_output_opt = create_umbrella_opt(remote_rename_support, output_files, true, umbrella_logfile);
	debug(D_MAKEFLOW_RUN, "umbrella output opt: %s\n", umbrella_output_opt);

	// construct umbrella_command
	if(!remote_rename_support) {
		if(!w->binary) {
			umbrella_command = string_format("umbrella --spec \"%s\" \
				--localdir ./umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode \"%s\" \
				--log \"%s\" \
				run \'{}\'", n->umbrella_spec, umbrella_input_opt, umbrella_output_opt, w->mode, umbrella_logfile);
		} else {
			umbrella_command = string_format("%s --spec \"%s\" \
				--localdir ./umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode \"%s\" \
				--log \"%s\" \
				run \'{}\'", w->binary, n->umbrella_spec, umbrella_input_opt, umbrella_output_opt, w->mode, umbrella_logfile);
		}
	} else {
		if(!w->binary) {
			umbrella_command = string_format("umbrella --spec \"%s\" \
				--localdir ./umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode \"%s\" \
				--log \"%s\" \
				run \'{}\'", path_basename(n->umbrella_spec), umbrella_input_opt, umbrella_output_opt, w->mode, umbrella_logfile);
		} else {
			umbrella_command = string_format("./%s --spec \"%s\" \
				--localdir ./umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode \"%s\" \
				--log \"%s\" \
				run \'{}\'", path_basename(w->binary), path_basename(n->umbrella_spec), umbrella_input_opt, umbrella_output_opt, w->mode, umbrella_logfile);
		}
	}

	debug(D_MAKEFLOW_RUN, "umbrella wrapper command: %s\n", umbrella_command);

	result = string_wrap_command(result, umbrella_command);
	debug(D_MAKEFLOW_RUN, "umbrella command: %s\n", result);

	free(umbrella_command);
	free(umbrella_input_opt);
	free(umbrella_output_opt);
	free(umbrella_logfile);
	return result;
}
