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

void makeflow_wrapper_umbrella_preparation(struct makeflow_wrapper_umbrella *w, struct batch_queue *queue) {
	if(!w->binary) {
		debug(D_MAKEFLOW_RUN, "the --umbrella-binary option is not set, therefore an umbrella binary should be available on an execution node.\n");
		fprintf(stdout, "the --umbrella-binary option is not set, therefore an umbrella binary should be available on an execution node.\n");
	}

	// add umbrella_spec (if specified) and umbrella_binary (if specified) into the input file list of w->wrapper
	if (!batch_queue_supports_feature(queue, "remote_rename")) {
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
}

char *create_umbrella_opt(struct batch_queue *queue, char *files, bool is_output) {
	char *s = files;
	size_t size;
	char *result = NULL;
	// construct the --output or --inputs option of umbrella based on files
	while((size = strcspn(s, ",\0")) > 0) {
		char *t;
		s[size] = '\0';

		if(!batch_queue_supports_feature(queue, "remote_rename")) {
			t = s;
		} else {
			t = strchr(s, '=');
			t++;
		}

		result = string_combine(result, t);
		result = string_combine(result, "=");
		result = string_combine(result, t);

		if(is_output) result = string_combine(result, ":f,");
		else result = string_combine(result, ",");

		s[size] = ',';
		s += size+1;
	}

	return result;
}

char *makeflow_wrap_umbrella(char *result, struct makeflow_wrapper_umbrella *w, struct batch_queue *queue, char *input_files, char *output_files) {
	if(!w || !w->spec) return result;

	char *umbrella_command = NULL;
	char *umbrella_input_opt = NULL;
	char *umbrella_output_opt = NULL;

	debug(D_MAKEFLOW_RUN, "input_files: %s\n", input_files);
	umbrella_input_opt = create_umbrella_opt(queue, input_files, false);
	debug(D_MAKEFLOW_RUN, "umbrella input opt: %s\n", umbrella_input_opt);

	debug(D_MAKEFLOW_RUN, "output_files: %s\n", output_files);
	umbrella_output_opt = create_umbrella_opt(queue, output_files, true);
	debug(D_MAKEFLOW_RUN, "umbrella output opt: %s\n", umbrella_output_opt);

	// construct umbrella_command
	if(!batch_queue_supports_feature(queue, "remote_rename")) {
		if(!w->binary) {
			umbrella_command = string_format("umbrella --spec %s \
				--localdir /tmp/umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode parrot \
				run \'{}\'", w->spec, umbrella_input_opt, umbrella_output_opt);
		} else {
			umbrella_command = string_format("%s --spec %s \
				--localdir /tmp/umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode parrot \
				run \'{}\'", w->binary, w->spec, umbrella_input_opt, umbrella_output_opt);
		}
	} else {
		if(!w->binary) {
			umbrella_command = string_format("umbrella --spec %s \
				--localdir /tmp/umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode parrot \
				run \'{}\'", path_basename(w->spec), umbrella_input_opt, umbrella_output_opt);
		} else {
			umbrella_command = string_format("./%s --spec %s \
				--localdir /tmp/umbrella_test \
				--inputs \"%s\" \
				--output \"%s\" \
				--sandbox_mode parrot \
				run \'{}\'", path_basename(w->binary), path_basename(w->spec), umbrella_input_opt, umbrella_output_opt);
		}
	}

	debug(D_MAKEFLOW_RUN, "umbrella wrapper command: %s\n", umbrella_command);

	result = string_wrap_command(result, umbrella_command);
	debug(D_MAKEFLOW_RUN, "umbrella command: %s\n", result);

	free(umbrella_command);
	free(umbrella_input_opt);
	free(umbrella_output_opt);
	return result;
}
