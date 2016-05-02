/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "rmonitor.h"
#include "stringtools.h"
#include "list.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_wrapper.h"

#include <string.h>
#include <stdlib.h>

struct makeflow_wrapper * makeflow_wrapper_create()
{
	struct makeflow_wrapper *w = malloc(sizeof(*w));
	w->command = NULL;

	w->input_files = list_create();
	w->output_files = list_create();

	w->remote_names = itable_create(0);
	w->remote_names_inv = hash_table_create(0, 0);

	w->uses_remote_rename = 0;

	return w;
}

void makeflow_wrapper_delete(struct makeflow_wrapper *w)
{
	if(w->command)
		free(w->command);	

	list_free(w->input_files);
	list_delete(w->input_files);

	list_free(w->output_files);
	list_delete(w->output_files);

	if(w->uses_remote_rename){
		uint64_t f;
		char *remote;
		itable_firstkey(w->remote_names);
		while(itable_nextkey(w->remote_names, &f, (void **) &remote)){
			free(remote);
		}
	}
	itable_delete(w->remote_names);

	hash_table_delete(w->remote_names_inv);

	free(w);
}

void makeflow_wrapper_add_command( struct makeflow_wrapper *w, const char *cmd )
{
	if(!w->command) {
		w->command = xxstrdup(cmd);
	} else {
		char *command = string_wrap_command(w->command,cmd);
		free(w->command);
		w->command = command;
	}
}

void makeflow_wrapper_add_input_file( struct makeflow_wrapper *w, const char *file )
{
	char *f = xxstrdup(file);
	char *p = strchr(f, '=');
	if(p) w->uses_remote_rename = 1;
	list_push_tail(w->input_files, f);
}

void makeflow_wrapper_add_output_file( struct makeflow_wrapper *w, const char *file )
{
	char *f = xxstrdup(file);
	char *p = strchr(f, '=');
	if(p) w->uses_remote_rename = 1;
	list_push_tail(w->output_files, f);
}

struct list *makeflow_wrapper_generate_files( struct list *result, struct list *input, struct dag_node *n, struct makeflow_wrapper *w)
{
	char *f;
	char *nodeid = string_format("%d",n->nodeid);

	struct list *files = list_create();

	list_first_item(input);
	while((f = list_next_item(input)))
	{
		char *filename = string_replace_percents(f, nodeid);
		char *f = xxstrdup(filename);
		free(filename);

		char *remote, *p;
		struct dag_file *file;
		p = strchr(f, '=');
		if(p) {
			*p = 0;
			file = dag_file_lookup_or_create(n->d, f);
			if(!n->local_job && !itable_lookup(w->remote_names, (uintptr_t) file)){
				remote = xxstrdup(p+1);
				itable_insert(w->remote_names, (uintptr_t) file, (void *)remote);
				hash_table_insert(w->remote_names_inv, remote, (void *)file);
			}
			*p = '=';
		} else {
			file = dag_file_lookup_or_create(n->d, f);
		}
		free(f);
		list_push_tail(files, file);
	}
	free(nodeid);

	result = list_splice(result, files);

	return result;
}

/* Returns the remotename used in wrapper for local name filename */
const char *makeflow_wrapper_get_remote_name(struct makeflow_wrapper *w, struct dag *d, const char *filename)
{
	struct dag_file *f;
	char *name;

	f = dag_file_from_name(d, filename);
	name = (char *) itable_lookup(w->remote_names, (uintptr_t) f);

	return name;
}

/* Takes node->command and wraps it in wrapper_command. Then, if in monitor
 *  * mode, wraps the wrapped command in the monitor command. */
char *makeflow_wrap_wrapper( char *command,  struct dag_node *n, struct makeflow_wrapper *w )
{
	if(!w) return xxstrdup(command);

	char *nodeid = string_format("%d",n->nodeid);
	char *wrap_tmp = string_replace_percents(w->command, nodeid);

	free(nodeid);

	char *result = string_wrap_command(command, wrap_tmp);
	free(wrap_tmp);

	return result;
}
