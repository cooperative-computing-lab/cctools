
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

static struct list *shared_fs_list = NULL;
static struct itable *shared_fs_saved_inputs  = NULL;
static struct itable *shared_fs_saved_outputs = NULL;

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

static int batch_file_on_sharedfs( const char *filename )
{
	return !list_iterate(shared_fs_list,prefix_match,filename);
}

static int create(struct jx *args){
	shared_fs_list = list_create();
	shared_fs_saved_inputs  = itable_create(0);
	shared_fs_saved_outputs = itable_create(0);

	//JX ARRAY ITERATE
	if(jx_lookup(args, "shared_fs_list")){
		struct jx *array = jx_lookup(args, "shared_fs_list");
		struct jx *item = NULL;
		while((item = jx_array_shift(array))) {
			list_push_head(shared_fs_list, xxstrdup(item->u.string_value));
		}
	}
	
	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy(){
	char * fs = NULL;

	list_first_item(shared_fs_list);
	while((fs = list_next_item(shared_fs_list))){
		free(fs);
	}

	itable_delete(shared_fs_saved_inputs);
	itable_delete(shared_fs_saved_outputs);
	return MAKEFLOW_HOOK_SUCCESS;
}

typedef enum {
	SHARED_FS_SUPPORTED,
	SHARED_FS_RENAME,
	SHARED_FS_UNSUPPORTED,
} shared_fs_support_t;

static int node_file_uses_unsupported_shared_fs( struct dag_node *n, struct dag_file *f)
{
	const char *remotename = dag_node_get_remote_name(n, f->filename);
	if (batch_file_on_sharedfs(f->filename)) {
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

static int node_files_uses_unsupported_shared_fs( struct dag_node *n, struct list *files)
{
	struct dag_file *f;
	int failed = 0;
	list_first_item(files);
	while((f = list_next_item(files))) {
		int rc = node_file_uses_unsupported_shared_fs(n, f);
		if(rc != MAKEFLOW_HOOK_SUCCESS)
			failed = 1;
	}
	if(failed)
		return MAKEFLOW_HOOK_FAILURE;
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check(struct dag *d){
	struct dag_node *n;
	int failed = 0;
	for(n = d->nodes; n; n = n->next) {
		if(!batch_queue_supports_feature(makeflow_get_queue(n), "absolute_path")){
			int rc = node_files_uses_unsupported_shared_fs(n, n->source_files);
			if(rc != MAKEFLOW_HOOK_SUCCESS)
				failed = 1;

			rc = node_files_uses_unsupported_shared_fs(n, n->source_files);
			if(rc != MAKEFLOW_HOOK_SUCCESS)
				failed = 1;
		}
	}
	if(failed)
		return MAKEFLOW_HOOK_FAILURE;
	return MAKEFLOW_HOOK_SUCCESS;
}

static int batch_submit(struct batch_task *t){
	struct batch_file *f = NULL;
	struct list *saved_inputs = list_create();
	struct list *saved_outputs = list_create();

	list_first_item(t->input_files);
	while((f = list_next_item(t->input_files))){
		if(batch_file_on_sharedfs(f->outer_name)) {
			list_remove(t->input_files, f);
			list_push_tail(saved_inputs, f);
			debug(D_MAKEFLOW_HOOK, "skipping file %s on shared fs\n", f->outer_name);
		}
	}
	itable_insert(shared_fs_saved_inputs, t->taskid, saved_inputs);


	list_first_item(t->output_files);
	while((f = list_next_item(t->output_files))){
		if(batch_file_on_sharedfs(f->outer_name)) {
			list_remove(t->output_files, f);
			list_push_tail(saved_outputs, f);
			debug(D_MAKEFLOW_HOOK, "skipping file %s on shared fs\n", f->outer_name);
		}
	}
	itable_insert(shared_fs_saved_outputs, t->taskid, saved_outputs);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int batch_retrieve(struct batch_task *t){
	struct batch_file *f = NULL;
	struct list *saved_inputs = itable_remove(shared_fs_saved_inputs, t->taskid);
	struct list *saved_outputs = itable_remove(shared_fs_saved_outputs, t->taskid);

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


