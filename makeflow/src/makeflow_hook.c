#include <assert.h>

#include "debug.h"
#include "list.h"
#include "stringtools.h"

#include "batch_queue.h"
#include "batch_job.h"

#include "makeflow_hook.h"
#include "xxmalloc.h"
#include "makeflow_log.h"
#include "dag.h"
#include "dag_node.h"
#include "dag_file.h"
#include "jx.h"

struct list * makeflow_hooks     = NULL;
struct list * makeflow_hook_args = NULL;
struct list * makeflow_hook_self = NULL;

#define MAKEFLOW_HOOK_CALL(hook_name, ...) do { \
	int rc = MAKEFLOW_HOOK_SUCCESS; \
	INT64_T debug_flags = D_MAKEFLOW_HOOK; \
	if (!makeflow_hooks) \
		return rc; \
	struct list_cursor *cur = list_cursor_create(makeflow_hooks); \
	struct list_cursor *scur = list_cursor_create(makeflow_hook_self); \
	struct makeflow_hook *h; \
	void *self; \
	for (list_seek(cur, 0), list_seek(scur, 0); \
		 list_get(cur, (void**)&h) && list_get(scur, &self); \
		 list_next(cur), list_next(scur)){ \
		if (h->hook_name) \
			rc = h->hook_name(self, __VA_ARGS__); \
		if (rc !=MAKEFLOW_HOOK_SUCCESS){ \
			if (rc == MAKEFLOW_HOOK_FAILURE){ \
				debug_flags = debug_flags|D_ERROR; \
			} \
			debug(debug_flags,"hook %s:" #hook_name " returned %d",h->module_name?h->module_name:"", rc); \
			break; \
		} \
	} \
	list_cursor_destroy(cur); \
	list_cursor_destroy(scur); \
	return rc; \
} while (0)



struct dag_file *makeflow_hook_add_input_file(struct dag *d, struct batch_job *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type)
{
	/* Output of dag_file is returned to use for final filename. */
	struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);
	/* Indicate this is an temporary file that should be cleaned up. */
	f->type = file_type;

	batch_job_add_input_file(task, name_on_submission, name_on_execution);

	return f;
}

struct dag_file * makeflow_hook_add_output_file(struct dag *d, struct batch_job *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type)
{
	/* Output of dag_file is returned to use for final filename. */
	struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);
	f->type = file_type;

	batch_job_add_output_file(task, name_on_submission, name_on_execution);

	return f;
}


int makeflow_hook_register(struct makeflow_hook *hook, struct jx **args) {
	assert(hook);
	if (!makeflow_hooks)     makeflow_hooks     = list_create();
	if (!makeflow_hook_args) makeflow_hook_args = list_create();
	if (!makeflow_hook_self) makeflow_hook_self = list_create();

	/* Add hook by default, if it doesn't exists in list of hooks. */
	int rc = MAKEFLOW_HOOK_SUCCESS;
	struct makeflow_hook *h = NULL;
	struct jx *h_args = NULL;
	debug(D_MAKEFLOW_HOOK, "Hook %s:trying to register",hook->module_name?hook->module_name:"");

	if(hook->register_hook){
		rc = hook->register_hook(hook, makeflow_hooks, args);
	}

	if(!(hook->register_hook) || rc == MAKEFLOW_HOOK_SKIP) {
		struct list_cursor *cur = list_cursor_create(makeflow_hooks);
		struct list_cursor *acur = list_cursor_create(makeflow_hook_args);
		for (list_seek(cur, -1), list_seek(acur, -1); 
			 list_get(cur, (void**)&h) && list_get(acur, (void**)&h_args); 
			 list_prev(cur), list_prev(acur)){
			if(h && !strcmp(h->module_name, hook->module_name)){
				*args = h_args;
				rc = MAKEFLOW_HOOK_SKIP;
				break;
			}
		}
		list_cursor_destroy(cur);
		list_cursor_destroy(acur);
	}

	if(rc == MAKEFLOW_HOOK_SUCCESS){
		h_args = jx_object(NULL);
		if(h_args){
			*args = h_args;
		}

		list_push_tail(makeflow_hooks, hook);
		list_push_tail(makeflow_hook_args, h_args);
		list_push_tail(makeflow_hook_self, NULL);

		debug(D_MAKEFLOW_HOOK, "Hook %s:registered",hook->module_name?hook->module_name:"");
	} else if(rc == MAKEFLOW_HOOK_FAILURE){
		/* Args are NULL to prevent other hooks from modifying an
		 * unintended arg list. */
		*args = NULL;
		debug(D_ERROR|D_MAKEFLOW_HOOK, "Hook %s:register failed",hook->module_name?hook->module_name:"");
	}

	return rc;
}

int makeflow_hook_create(){
	int rc = MAKEFLOW_HOOK_SUCCESS;
	if (!makeflow_hooks)
		return rc;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct list_cursor *acur = list_cursor_create(makeflow_hook_args);
	struct list_cursor *scur = list_cursor_create(makeflow_hook_self);
	struct makeflow_hook *h;
	struct jx *args;
	void *self = NULL;
	for (list_seek(cur, 0), list_seek(acur,  0), list_seek(scur, 0); 
		 list_get(cur, (void**)&h) && list_get(acur, (void**)&args) && list_get(scur, &self); 
		 list_next(cur), list_next(acur), list_next(scur)){
		debug(D_MAKEFLOW_HOOK, "hook %s:initializing",h->module_name);
		if (h->create){
			rc = h->create(&self, args);
			list_set(scur, self);
		}

		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_ERROR|D_MAKEFLOW_HOOK, "hook %s:create returned %d",h->module_name, rc);
			break;
		}
	}
	list_cursor_destroy(cur);
	list_cursor_destroy(acur);
	list_cursor_destroy(scur);

	return rc;
}

int makeflow_hook_destroy(struct dag *d){
	MAKEFLOW_HOOK_CALL(destroy, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_check(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_check, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_clean(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_clean, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_start(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_start, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_loop(struct dag *d){
	int rc = MAKEFLOW_HOOK_END;
	if(!makeflow_hooks)
		return rc;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct list_cursor *scur = list_cursor_create(makeflow_hook_self);
	struct makeflow_hook *h;
	void *self;
	for (list_seek(cur, 0), list_seek(scur, 0); 
		 list_get(cur, (void**)&h) && list_get(scur, &self); 
		 list_next(cur), list_next(scur)){
		if (h->dag_loop) {
			rc = h->dag_loop(self, d);
		} else {
			continue;
		}
 
		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_MAKEFLOW_HOOK, "Hook %s:dag_loop rejected DAG",h->module_name?h->module_name:"");
			break;
		}
	}
	list_cursor_destroy(cur);
	list_cursor_destroy(scur);

	return rc;
}


int makeflow_hook_dag_end(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_end, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_fail(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_fail, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_abort(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_abort, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_success(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_success, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_check(struct dag_node *node, struct batch_queue *queue){
	MAKEFLOW_HOOK_CALL(node_check, node, queue);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_submit(struct dag_node *node, struct batch_job *task){
	MAKEFLOW_HOOK_CALL(node_submit, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_end(struct dag_node *node, struct batch_job *task){
	MAKEFLOW_HOOK_CALL(node_end, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_success(struct dag_node *node, struct batch_job *task){
	MAKEFLOW_HOOK_CALL(node_success, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_fail(struct dag_node *node, struct batch_job *task){
	MAKEFLOW_HOOK_CALL(node_fail, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_abort(struct dag_node *node){
	MAKEFLOW_HOOK_CALL(node_abort, node);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_submit(struct batch_job *task){
	MAKEFLOW_HOOK_CALL(batch_submit, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_retrieve(struct batch_job *task){
	MAKEFLOW_HOOK_CALL(batch_retrieve, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_file_complete(struct dag_file *file){
	MAKEFLOW_HOOK_CALL(file_complete, file);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_file_clean(struct dag_file *file){
	MAKEFLOW_HOOK_CALL(file_clean, file);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_file_deleted(struct dag_file *file){
	MAKEFLOW_HOOK_CALL(file_deleted, file);
	return MAKEFLOW_HOOK_SUCCESS;
}
  
