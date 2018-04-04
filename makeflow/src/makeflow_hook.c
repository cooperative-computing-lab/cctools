#include <assert.h>

#include "debug.h"
#include "stringtools.h"

#include "batch_job.h"
#include "batch_task.h"

#include "makeflow_hook.h"
#include "xxmalloc.h"
#include "makeflow_log.h"
#include "dag.h"
#include "dag_node.h"
#include "dag_file.h"
#include "jx.h"

struct list * makeflow_hooks = NULL;

#define MAKEFLOW_HOOK_CALL(hook_name, ...) do { \
	if (!makeflow_hooks) \
		return MAKEFLOW_HOOK_SUCCESS; \
	struct list_cursor *cur = list_cursor_create(makeflow_hooks); \
	struct makeflow_hook *h; \
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){ \
		int rc = MAKEFLOW_HOOK_SUCCESS; \
		if (h->hook_name) \
			rc = h->hook_name(h->instance_struct, __VA_ARGS__); \
		if (rc !=MAKEFLOW_HOOK_SUCCESS){ \
			debug(D_ERROR|D_MAKEFLOW_HOOK,"hook %s:" #hook_name " returned %d",h->module_name?h->module_name:"", rc); \
			list_cursor_destroy(cur); \
			return rc; \
		} \
	} \
} while (0)



struct dag_file *makeflow_hook_add_input_file(struct dag *d, struct batch_task *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type)
{
	/* Output of dag_file is returned to use for final filename. */
	struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);
	/* Indicate this is an temporary file that should be cleaned up. */
	f->type = file_type;

	batch_task_add_input_file(task, name_on_submission, name_on_execution);

	return f;
}

struct dag_file * makeflow_hook_add_output_file(struct dag *d, struct batch_task *task, const char * name_on_submission, const char * name_on_execution, dag_file_type_t file_type)
{
	/* Output of dag_file is returned to use for final filename. */
	struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);
	f->type = file_type;

	batch_task_add_output_file(task, name_on_submission, name_on_execution);

	return f;
}


int makeflow_hook_register(struct makeflow_hook *hook, struct jx **args) {
	assert(hook);
	if (!makeflow_hooks) makeflow_hooks = list_create();

	/* Add hook by default, if it doesn't exists in list of hooks. */
	int rc = MAKEFLOW_HOOK_SUCCESS;
	struct jx *new_args = NULL;
	struct makeflow_hook *h = NULL;
	debug(D_MAKEFLOW_HOOK, "Hook %s:trying to registered",hook->module_name?hook->module_name:"");

	if(hook->register_hook){
		rc = hook->register_hook(hook, makeflow_hooks, args);
	} else {
		new_args = jx_object(NULL);
		struct list_cursor *cur = list_cursor_create(makeflow_hooks);
		for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
			if(!strcmp(h->module_name, hook->module_name)){
				jx_delete(new_args);
				new_args = NULL;
				*args = h->args;
				rc = MAKEFLOW_HOOK_SKIP;
				break;
			}
		}
		list_cursor_destroy(cur);
	}

	if(rc == MAKEFLOW_HOOK_SUCCESS){
		h = xxmalloc(sizeof(*h));
		memcpy(h, hook, sizeof(*h));
		if(new_args){
			*args = new_args;
		}
		h->args = *args;
		debug(D_MAKEFLOW_HOOK, "Hook %s:registered",h->module_name?h->module_name:"");

		list_push_tail(makeflow_hooks, h);
	} else if(rc == MAKEFLOW_HOOK_FAILURE){
		// Not safe, need to think about this
		args = NULL;
		debug(D_ERROR|D_MAKEFLOW_HOOK, "Hook %s:register failed",h->module_name?h->module_name:"");
	}

	return rc;
}

int makeflow_hook_create(){
	if (!makeflow_hooks)
		return MAKEFLOW_HOOK_SUCCESS;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		debug(D_MAKEFLOW_HOOK, "hook %s:initializing",h->module_name);
		int rc = MAKEFLOW_HOOK_SUCCESS;
		if (h->create){
			rc = h->create(&(h->instance_struct), h->args);
		}
		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_ERROR|D_MAKEFLOW_HOOK, "hook %s:create returned %d",h->module_name, rc);
			list_cursor_destroy(cur);
			return rc;
		}
	}
	list_cursor_destroy(cur);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_destroy(struct dag *d){
	MAKEFLOW_HOOK_CALL(destroy, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_check(struct dag *d){
	if (!makeflow_hooks)
		return MAKEFLOW_HOOK_SUCCESS;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		int rc = MAKEFLOW_HOOK_SUCCESS;
		if (h->dag_check)
			rc = h->dag_check(h->instance_struct, d);

		/* If the return is not success return this to Makeflow.
		 * If it was a failure report this in debugging, if it was something
		 * else than the system is chosing to exit. A case for this is the
		 * storage allocation printing function. If not returning FAILURE
		 * the module should provide a printout for why it is exiting. */
		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			if (rc ==MAKEFLOW_HOOK_FAILURE)
				debug(D_MAKEFLOW_HOOK, "Hook %s:dag_check rejected DAG",h->module_name?h->module_name:"");
			list_cursor_destroy(cur);
			return rc;
		}
	}
	list_cursor_destroy(cur);

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
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		if (h->dag_loop) {
			rc = h->dag_loop(h->instance_struct, d);
		} else {
			continue;
		}

		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_MAKEFLOW_HOOK, "Hook %s:dag_loop rejected DAG",h->module_name?h->module_name:"");
			list_cursor_destroy(cur);
			return rc;
		}
	}
	list_cursor_destroy(cur);

	return rc;
}


int makeflow_hook_dag_end(struct dag *d){
	if (!makeflow_hooks)
		return MAKEFLOW_HOOK_SUCCESS;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		int rc = MAKEFLOW_HOOK_SUCCESS;
		if (h->dag_end)
			rc = h->dag_end(h->instance_struct, d);

		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_MAKEFLOW_HOOK, "Hook %s:dag_end failed dag",h->module_name?h->module_name:"");
			list_cursor_destroy(cur);
			return rc;
		}
	}
	list_cursor_destroy(cur);

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
	if (!makeflow_hooks)
		return MAKEFLOW_HOOK_SUCCESS;

	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		int rc = MAKEFLOW_HOOK_SUCCESS;
		if (h->node_check)
			rc = h->node_check(h->instance_struct, node, queue);

		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_MAKEFLOW_HOOK, "Hook %s:node_check rejected Node %d",h->module_name?h->module_name:"", node->nodeid);
			list_cursor_destroy(cur);
			return rc;
		}
	}
	list_cursor_destroy(cur);

	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_submit(struct dag_node *node, struct batch_task *task){
	MAKEFLOW_HOOK_CALL(node_submit, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_end(struct dag_node *node, struct batch_task *task){
	MAKEFLOW_HOOK_CALL(node_end, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_success(struct dag_node *node, struct batch_task *task){
	MAKEFLOW_HOOK_CALL(node_success, node, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_fail(struct dag_node *node, struct batch_task *task){
	if (!makeflow_hooks)
		return MAKEFLOW_HOOK_SUCCESS;

	int hook_return = MAKEFLOW_HOOK_SUCCESS;
	struct list_cursor *cur = list_cursor_create(makeflow_hooks);
	struct makeflow_hook *h;
	for (list_seek(cur, 0); list_get(cur, (void**)&h); list_next(cur)){
		int rc = MAKEFLOW_HOOK_SUCCESS;
		if (h->node_fail){
			debug(D_MAKEFLOW_HOOK, "Checking %s\n", h->module_name);
			rc = h->node_fail(h->instance_struct, node, task);
		}

		if (rc !=MAKEFLOW_HOOK_SUCCESS){
			debug(D_MAKEFLOW_HOOK, "Hook %s:node_fail failed Node %d",h->module_name?h->module_name:"", node->nodeid);
			hook_return = rc;
		}
	}
	list_cursor_destroy(cur);

	return hook_return;
}

int makeflow_hook_node_abort(struct dag_node *node){
	MAKEFLOW_HOOK_CALL(node_abort, node);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_submit(struct batch_task *task){
	MAKEFLOW_HOOK_CALL(batch_submit, task);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_retrieve(struct batch_task *task){
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
  
