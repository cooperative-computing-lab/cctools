
#include <assert.h>
#include "debug.h"

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

int makeflow_hook_dag_start(struct dag *d){
	MAKEFLOW_HOOK_CALL(dag_start, d);
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

int makeflow_hook_node_create(struct dag_node *n, struct hash_table *feat){
	MAKEFLOW_HOOK_CALL(node_create, n, feat);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_submit(struct dag_node *n){
	MAKEFLOW_HOOK_CALL(node_submit, n);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_success(struct dag_node *n, struct batch_job_info *bji){
	MAKEFLOW_HOOK_CALL(node_success, n, bji);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_fail(struct dag_node *n, struct batch_job_info *bji){
	MAKEFLOW_HOOK_CALL(node_fail, n, bji);
	return MAKEFLOW_HOOK_SUCCESS;
}

int makeflow_hook_node_abort(struct dag_node *n, struct batch_job_info *bji){
	MAKEFLOW_HOOK_CALL(node_abort, n, bji);
	return MAKEFLOW_HOOK_SUCCESS;
}
