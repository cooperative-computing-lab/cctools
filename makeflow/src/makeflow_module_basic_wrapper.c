/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "rmonitor.h"
#include "stringtools.h"
#include "list.h"
#include "xxmalloc.h"

#include "batch_job.h"

#include "dag.h"
#include "makeflow_hook.h"

#include <string.h>
#include <stdlib.h>


struct wrapper_instance {
    char *command;
    struct list *input_files;
    struct list *output_files;

    struct itable *remote_names;

    int uses_remote_rename;
};

struct wrapper_instance *wrapper_instance_create()
{
	struct wrapper_instance *w = malloc(sizeof(*w));
	w->command = NULL;

	w->input_files = list_create();
	w->output_files = list_create();

	w->remote_names = itable_create(0);

	w->uses_remote_rename = 0;

	return w;
}


void wrapper_add_input_file( struct wrapper_instance *w, const char *file )
{
	char *f = xxstrdup(file);
	char *p = strchr(f, '=');
	if(p) w->uses_remote_rename = 1;
	list_push_tail(w->input_files, f);
}

void wrapper_add_output_file( struct wrapper_instance *w, const char *file )
{
	char *f = xxstrdup(file);
	char *p = strchr(f, '=');
	if(p) w->uses_remote_rename = 1;
	list_push_tail(w->output_files, f);
}


static int create( void ** instance_struct, struct jx *hook_args ){
    struct wrapper_instance *w = wrapper_instance_create();
    *instance_struct = w;

    struct jx *array = jx_lookup(hook_args, "wrapper_command");
    if (array && array->type == JX_ARRAY) {
        struct jx *item = NULL;
        while((item = jx_array_shift(array))) {
            if(item->type == JX_STRING){
               	if(!w->command) {
		            w->command = xxstrdup(item->u.string_value);
	            } else {
		            char *command = string_wrap_command(w->command,item->u.string_value);
		            free(w->command);
		            w->command = command;
	            }
                debug(D_MAKEFLOW_HOOK, "Wrapper input file added : %s", item->u.string_value);
            } else {
                debug(D_ERROR|D_MAKEFLOW_HOOK, "Non-string argument passed to Wrapper hook as command");
                return MAKEFLOW_HOOK_FAILURE;
            }
            jx_delete(item);
        }
    }


    array = jx_lookup(hook_args, "wrapper_input");
    if (array && array->type == JX_ARRAY) {
        struct jx *item = NULL;
        while((item = jx_array_shift(array))) {
            if(item->type == JX_STRING){
                wrapper_add_input_file(w, item->u.string_value);
                debug(D_MAKEFLOW_HOOK, "Wrapper input file added : %s", item->u.string_value);
            } else {
                debug(D_ERROR|D_MAKEFLOW_HOOK, "Non-string argument passed to Wrapper hook as input file");
                return MAKEFLOW_HOOK_FAILURE;
            }
            jx_delete(item);
        }
    }

    array = jx_lookup(hook_args, "wrapper_output");
    if (array && array->type == JX_ARRAY) {
        struct jx *item = NULL;
        while((item = jx_array_shift(array))) {
            if(item->type == JX_STRING){
                wrapper_add_output_file(w, item->u.string_value);
                debug(D_MAKEFLOW_HOOK, "Wrapper output file added : %s", item->u.string_value);
            } else {
                debug(D_ERROR|D_MAKEFLOW_HOOK, "Non-string argument passed to Wrapper hook as output file");
                return MAKEFLOW_HOOK_FAILURE;
            }
            jx_delete(item);
        }
    }

    return MAKEFLOW_HOOK_SUCCESS;
}

void wrapper_instance_delete(struct wrapper_instance *w)
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

	free(w);
}

static int destroy( void * instance_struct, struct dag *d ){
    struct wrapper_instance *w = (struct wrapper_instance*)instance_struct;
    if (w){
        wrapper_instance_delete(w);
    }
    return MAKEFLOW_HOOK_SUCCESS;
}

void wrapper_generate_files( struct batch_job *task, struct dag_node *n, struct wrapper_instance *w)
{
	char *f;
	char *nodeid = string_format("%d",n->nodeid);

	list_first_item(w->input_files);
	while((f = list_next_item(w->input_files)))
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
				makeflow_hook_add_input_file(n->d, task, f, remote, file->type);
			} else {
				makeflow_hook_add_output_file(n->d, task, f, NULL, file->type);
			}
			*p = '=';
		} else {
			file = dag_file_lookup_or_create(n->d, f);
			makeflow_hook_add_input_file(n->d, task, f, NULL, file->type);
		}
		free(f);
	}

	list_first_item(w->output_files);
	while((f = list_next_item(w->output_files)))
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
				makeflow_hook_add_output_file(n->d, task, f, remote, file->type);
			} else {
				makeflow_hook_add_output_file(n->d, task, f, NULL, file->type);
			}
			*p = '=';
		} else {
			file = dag_file_lookup_or_create(n->d, f);
			makeflow_hook_add_output_file(n->d, task, f, NULL, file->type);
		}
		free(f);
	}
	free(nodeid);

}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_job *t)
{
	struct wrapper_instance *w = (struct wrapper_instance*)instance_struct;

    wrapper_generate_files(t, n, w);

    char *nodeid = string_format("%d",n->nodeid);
    char *wrap_tmp = string_replace_percents(w->command, nodeid);

    batch_job_wrap_command(t, wrap_tmp); 

    free(nodeid);
    free(wrap_tmp);

	return MAKEFLOW_HOOK_SUCCESS;
}




struct makeflow_hook makeflow_hook_basic_wrapper = {
	.module_name = "Basic Wrapper",

	.create = create,
	.destroy = destroy,

    .node_submit = node_submit,

};



