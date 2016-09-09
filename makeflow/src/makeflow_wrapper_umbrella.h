/*
 * Copyright (C) 2016- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef MAKEFLOW_WRAPPER_UMBRELLA_H
#define MAKEFLOW_WRAPPER_UMBRELLA_H

struct makeflow_wrapper_umbrella {
	struct makeflow_wrapper *wrapper;
	const char *spec;
	const char *binary;
};

struct makeflow_wrapper_umbrella *makeflow_wrapper_umbrella_create();

void makeflow_wrapper_umbrella_set_spec(struct makeflow_wrapper_umbrella *w, const char *spec);

void makeflow_wrapper_umbrella_set_binary(struct makeflow_wrapper_umbrella *w, const char *binary);

void makeflow_wrapper_umbrella_preparation(struct makeflow_wrapper_umbrella *w, struct batch_queue *queue);

char *makeflow_wrap_umbrella(char *result, struct makeflow_wrapper_umbrella *w, struct batch_queue *queue, char *input_files, char *output_files);

#endif
