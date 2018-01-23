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
	list_first_item(makeflow_hooks); \
	for (struct makeflow_hook *h; (h = list_next_item(makeflow_hooks));) { \
		int rc = MAKEFLOW_HOOK_SUCCESS; \
		if (h->hook_name) \
			rc = h->hook_name(__VA_ARGS__); \
		if (rc !=MAKEFLOW_HOOK_SUCCESS) \
			fatal("hook %s:" #hook_name " returned %d",h->module_name?h->module_name:"", rc); \
	} \
} while (0)



struct dag_file *makeflow_hook_add_input_file(struct dag *d, struct batch_task *task, const char * name_on_submission_pattern, const char * name_on_execution_pattern)
{
    char *id = string_format("%d",task->taskid);
    char * name_on_submission = string_replace_percents(name_on_submission_pattern, id);
    char * name_on_execution = string_replace_percents(name_on_execution_pattern, id);

    /* Output of dag_file is returned to use for final filename. */
    struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);

    batch_task_add_input_file(task, name_on_submission, name_on_execution);

    free(id);
    free(name_on_submission);
    free(name_on_execution);

    return f;
}

struct dag_file * makeflow_hook_add_output_file(struct dag *d, struct batch_task *task, const char * name_on_submission_pattern, const char * name_on_execution_pattern)
{
    char *id = string_format("%d",task->taskid);
    char * name_on_submission = string_replace_percents(name_on_submission_pattern, id);
    char * name_on_execution = string_replace_percents(name_on_execution_pattern, id);

    /* Output of dag_file is returned to use for final filename. */
    struct dag_file *f = dag_file_lookup_or_create(d, name_on_submission);

    batch_task_add_output_file(task, name_on_submission, name_on_execution);

    free(id);
    free(name_on_submission);
    free(name_on_execution);

    return f;
}


void makeflow_hook_register(struct makeflow_hook *hook) {
	assert(hook);
	if (!makeflow_hooks) makeflow_hooks = list_create();
	struct makeflow_hook *h = xxmalloc(sizeof(*h));
	memcpy(h, hook, sizeof(*h));
	list_push_head(makeflow_hooks, h);
}

int makeflow_hook_create(struct jx *args){
	MAKEFLOW_HOOK_CALL(create, args);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_destroy(struct dag *d){
	MAKEFLOW_HOOK_CALL(destroy, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_init(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_init, d);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_dag_check(struct dag *d){
    if (!makeflow_hooks)
        return MAKEFLOW_HOOK_SUCCESS;

    list_first_item(makeflow_hooks);
    for (struct makeflow_hook *h; (h = list_next_item(makeflow_hooks));) {
        int rc = MAKEFLOW_HOOK_SUCCESS;
        if (h->dag_check)
            rc = h->dag_check(d);

        if (rc !=MAKEFLOW_HOOK_SUCCESS){
            debug(D_MAKEFLOW_HOOK, "Hook %s:dag_check rejected DAG",h->module_name?h->module_name:"");
			return rc;
		}
    }

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
    if (!makeflow_hooks)
        return MAKEFLOW_HOOK_END;

    list_first_item(makeflow_hooks);
    for (struct makeflow_hook *h; (h = list_next_item(makeflow_hooks));) {
        int rc = MAKEFLOW_HOOK_SUCCESS;
        if (h->dag_loop)
            rc = h->dag_loop(d);

        if (rc !=MAKEFLOW_HOOK_SUCCESS){
            debug(D_MAKEFLOW_HOOK, "Hook %s:dag_loop rejected DAG",h->module_name?h->module_name:"");
			return rc;
		}
    }

    return MAKEFLOW_HOOK_SUCCESS;
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

int makeflow_hook_node_create(struct dag_node *node, struct batch_queue *queue){
	MAKEFLOW_HOOK_CALL(node_create, node, queue);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_check(struct dag_node *node, struct batch_queue *queue){
    if (!makeflow_hooks)
        return MAKEFLOW_HOOK_SUCCESS;

    list_first_item(makeflow_hooks);
    for (struct makeflow_hook *h; (h = list_next_item(makeflow_hooks));) {
        int rc = MAKEFLOW_HOOK_SUCCESS;
        if (h->node_check)
            rc = h->node_check(node, queue);

        if (rc !=MAKEFLOW_HOOK_SUCCESS){
            debug(D_MAKEFLOW_HOOK, "Hook %s:node_check rejected Node %d",h->module_name?h->module_name:"", node->nodeid);
			return rc;
		}
    }

    return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_submit(struct dag_node *node, struct batch_queue *queue){
	MAKEFLOW_HOOK_CALL(node_submit, node, queue);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_end(struct dag_node *node, struct batch_job_info *info){
    MAKEFLOW_HOOK_CALL(node_end, node, info);
    return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_success(struct dag_node *node, struct batch_job_info *info){
	MAKEFLOW_HOOK_CALL(node_success, node, info);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_fail(struct dag_node *node, struct batch_job_info *info){
	MAKEFLOW_HOOK_CALL(node_fail, node, info);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_abort(struct dag_node *node){
	MAKEFLOW_HOOK_CALL(node_abort, node);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_submit(struct batch_queue *queue){
    MAKEFLOW_HOOK_CALL(batch_submit, queue);
    return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_batch_retrieve(struct batch_queue *queue){
    MAKEFLOW_HOOK_CALL(batch_retrieve, queue);
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
  
