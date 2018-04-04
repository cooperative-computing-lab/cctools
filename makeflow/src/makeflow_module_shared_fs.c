
#include <assert.h>

#include "debug.h"
#include "itable.h"
#include "makeflow_hook.h"
#include "xxmalloc.h"
#include "makeflow_log.h"
#include "makeflow_alloc.h"
#include "batch_task.h"
#include "dag.h"
#include "dag_node.h"
#include "dag_node_footprint.h"
#include "dag_file.h"
#include "jx.h"

struct shared_fs_instance {
	struct list *shared_fs_list;
	struct itable *shared_fs_saved_inputs;
	struct itable *shared_fs_saved_outputs;
};

struct shared_fs_instance *shared_fs_instance_create()
{
	struct shared_fs_instance *sf = malloc(sizeof(*sf));
	sf->shared_fs_list = list_create();
	sf->shared_fs_saved_inputs  = itable_create(0);
	sf->shared_fs_saved_outputs = itable_create(0);

	return sf;
}


/*
 * Match a filename (/home/fred) to a path stem (/home).
 * Returns 0 on match, non-zero otherwise.
 * */

static int prefix_match(void *stem, const void *filename) {
	assert(stem);
	assert(filename);
	return strncmp(stem, filename, strlen(stem));
}

/*
 * Returns true if the given filename is located in
 * a shared filesystem, as given by the shared_fs_list.
 * */

static int batch_file_on_sharedfs( struct list *shared_fs_list, const char *filename )
{
	return !list_iterate(shared_fs_list,prefix_match,filename);
}

static int create( void ** instance_struct, struct jx *hook_args )
{
	struct shared_fs_instance *sf = shared_fs_instance_create();
	*instance_struct = sf;

	//JX ARRAY ITERATE
	struct jx *array = jx_lookup(hook_args, "shared_fs_list");
	if (array && array->type == JX_ARRAY) {
		struct jx *item = NULL;
		while((item = jx_array_shift(array))) {
			if(item->type == JX_STRING){
				list_push_head(sf->shared_fs_list, xxstrdup(item->u.string_value));
			} else {
				debug(D_ERROR|D_MAKEFLOW_HOOK, "Non-string argument passed to Shared FS hook");
				return MAKEFLOW_HOOK_FAILURE;
			}
			jx_delete(item);
		}
	}
	
	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d ){
	struct shared_fs_instance *sf = (struct shared_fs_instance*)instance_struct;
	list_free(sf->shared_fs_list);
	list_delete(sf->shared_fs_list);
	itable_delete(sf->shared_fs_saved_inputs);
	itable_delete(sf->shared_fs_saved_outputs);
	free(sf);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_file_uses_unsupported_shared_fs( struct shared_fs_instance *sf, struct dag_node *n, struct dag_file *f)
{
	const char *remotename = dag_node_get_remote_name(n, f->filename);
	if (batch_file_on_sharedfs(sf->shared_fs_list, f->filename)) {
		if (remotename){
			debug(D_ERROR|D_MAKEFLOW_HOOK, "Remote renaming for %s is not supported on a shared filesystem",
					f->filename);
			return MAKEFLOW_HOOK_FAILURE;
		}
	} else if((remotename && *remotename == '/') || (*f->filename == '/' && !remotename)){
		debug(D_ERROR|D_MAKEFLOW_HOOK, "Absolute paths are not supported on %s: File %s Rule %d (line %d).\n",
					batch_queue_type_to_string(batch_queue_get_type(makeflow_get_queue(n))), 
					f->filename, n->nodeid, n->linenum);
		return MAKEFLOW_HOOK_FAILURE;
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_files_uses_unsupported_shared_fs( struct shared_fs_instance *sf, struct dag_node *n, struct list *files)
{
	struct dag_file *f;
	int failed = 0;
	list_first_item(files);
	while((f = list_next_item(files))) {
		int rc = node_file_uses_unsupported_shared_fs(sf, n, f);
		if(rc != MAKEFLOW_HOOK_SUCCESS)
			failed = 1;
	}
	if(failed)
		return MAKEFLOW_HOOK_FAILURE;
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check(void * instance_struct, struct dag *d){
	struct shared_fs_instance *sf = (struct shared_fs_instance*)instance_struct;
	struct dag_node *n;
	int failed = 0;
	for(n = d->nodes; n; n = n->next) {
		if(!batch_queue_supports_feature(makeflow_get_queue(n), "absolute_path")){
			int rc = node_files_uses_unsupported_shared_fs(sf, n, n->source_files);
			if(rc != MAKEFLOW_HOOK_SUCCESS)
				failed = 1;

			rc = node_files_uses_unsupported_shared_fs(sf, n, n->source_files);
			if(rc != MAKEFLOW_HOOK_SUCCESS)
				failed = 1;
		}
	}
	if(failed)
		return MAKEFLOW_HOOK_FAILURE;
	return MAKEFLOW_HOOK_SUCCESS;
}

static int batch_submit( void * instance_struct, struct batch_task *t){
	struct shared_fs_instance *sf = (struct shared_fs_instance*)instance_struct;
	struct batch_file *f = NULL;
	struct list *saved_inputs = list_create();
	struct list *saved_outputs = list_create();

	list_first_item(t->input_files);
	while((f = list_next_item(t->input_files))){
		if(batch_file_on_sharedfs(sf->shared_fs_list, f->outer_name)) {
			list_remove(t->input_files, f);
			list_push_tail(saved_inputs, f);
			debug(D_MAKEFLOW_HOOK, "skipping file %s on shared fs\n", f->outer_name);
		}
	}
	itable_insert(sf->shared_fs_saved_inputs, t->taskid, saved_inputs);


	list_first_item(t->output_files);
	while((f = list_next_item(t->output_files))){
		if(batch_file_on_sharedfs(sf->shared_fs_list, f->outer_name)) {
			list_remove(t->output_files, f);
			list_push_tail(saved_outputs, f);
			debug(D_MAKEFLOW_HOOK, "skipping file %s on shared fs\n", f->outer_name);
		}
	}
	itable_insert(sf->shared_fs_saved_outputs, t->taskid, saved_outputs);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int batch_retrieve( void * instance_struct, struct batch_task *t){
	struct shared_fs_instance *sf = (struct shared_fs_instance*)instance_struct;

	struct batch_file *f = NULL;
	struct list *saved_inputs = itable_remove(sf->shared_fs_saved_inputs, t->taskid);
	struct list *saved_outputs = itable_remove(sf->shared_fs_saved_outputs, t->taskid);

	if(saved_inputs){
	    list_first_item(saved_inputs);
	    while((f = list_next_item(saved_inputs))){
	        list_push_tail(t->input_files, f);
	        list_remove(saved_inputs, f);
			debug(D_MAKEFLOW_HOOK, "adding skipped file %s on shared fs\n", f->outer_name);
		}
		list_delete(saved_inputs);
	}

	if(saved_outputs){
		list_first_item(saved_outputs);
	    while((f = list_next_item(saved_outputs))){
	        list_push_tail(t->output_files, f);
	        list_remove(saved_outputs, f);
			debug(D_MAKEFLOW_HOOK, "adding skipped file %s on shared fs\n", f->outer_name);
		}
		list_delete(saved_outputs);
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_shared_fs = {
	.module_name = "Shared FS",
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,

	.batch_submit = batch_submit,

	.batch_retrieve = batch_retrieve,
};


