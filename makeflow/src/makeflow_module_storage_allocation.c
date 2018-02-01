
#include "debug.h"
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


static struct makeflow_alloc *storage_allocation = NULL;

/* Variables used to hold the time used for storage alloc. */
uint64_t static_analysis = 0;
char * storage_print = NULL;

static int create(struct jx *args){
	if(jx_lookup_string(args, "allocation_storage_print"))
		storage_print = xxstrdup(jx_lookup_string(args, "allocation_storage_print"));

	uint64_t storage_limit = jx_lookup_integer(args, "allocation_storage_limit");
	int storage_type  = jx_lookup_integer(args, "allocation_storage_type");
    if(storage_limit || storage_type != MAKEFLOW_ALLOC_TYPE_NOT_ENABLED){
        storage_allocation = makeflow_alloc_create(-1, NULL, storage_limit, 1, storage_type);
    }
	
	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy(){
	free(storage_print);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_start(struct dag *d){
    uint64_t start = timestamp_get();
    struct dag_node *n = dag_node_create(d, -1);
    n->state = DAG_NODE_STATE_COMPLETE;
    struct dag_node *p;

    for(p = d->nodes; p; p = p->next) {
        if(set_size(p->ancestors) == 0) {
            set_push(n->descendants, p);
        }
    }

    dag_node_footprint_calculate(n);
    if(storage_print){
        dag_node_footprint_find_largest_residual(n, NULL);
        dag_node_footprint_print(d, n, storage_print);
        exit(0);
    }
    uint64_t end = timestamp_get();
    static_analysis += end - start;
    dag_node_delete(n);

	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_end(struct dag *d){

    if(storage_allocation){
        makeflow_log_alloc_event(d, storage_allocation);
        makeflow_log_event(d, "STATIC_ANALYSIS", static_analysis);
        makeflow_log_event(d, "DYNAMIC_ALLOC", makeflow_alloc_get_dynamic_alloc_time());
    }

	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_create(struct dag_node *n, struct batch_queue *q){

    if(storage_allocation && storage_allocation->locked){
        if(!( makeflow_alloc_check_space(storage_allocation, n))){
            return MAKEFLOW_HOOK_FAILURE;
        }

        if (!(dag_node_footprint_dependencies_active(n))){
            return MAKEFLOW_HOOK_FAILURE;
        }
    }

	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit(struct dag_node *n, struct batch_task *task){

    if(storage_allocation && makeflow_alloc_commit_space(storage_allocation, n)){
        makeflow_log_alloc_event(n->d, storage_allocation);
    } else if (storage_allocation && storage_allocation->locked)  {
        fatal("Unable to commit enough space for execution\n");
		return MAKEFLOW_HOOK_FAILURE;
    }
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_success(struct dag_node *n, struct batch_task *task){
	struct dag_file *f = NULL;
	
    if(storage_allocation && makeflow_alloc_use_space(storage_allocation, n)){
		makeflow_log_alloc_event(n->d, storage_allocation);
	}

    /* Mark source files that have been used by this node */
    list_first_item(n->source_files);
    while((f = list_next_item(n->source_files))) {
        f->reference_count+= -1;
        if(f->reference_count == 0 && f->state == DAG_FILE_STATE_EXISTS){
            if(storage_allocation && storage_allocation->locked && f->type != DAG_FILE_TYPE_OUTPUT)
				makeflow_clean_file(n->d, makeflow_get_remote_queue(), f, 0);
		}
	}

    /* Delete output files that have no use and are not actual outputs */
    if(storage_allocation && storage_allocation->locked){
        list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))){
			if(f->reference_count == 0 && f->type != DAG_FILE_TYPE_OUTPUT)
				makeflow_clean_file(n->d, makeflow_get_remote_queue(), f, 0);
		}
	}

    if(storage_allocation && makeflow_alloc_release_space(storage_allocation, n, 0, MAKEFLOW_ALLOC_RELEASE_COMMIT)) {
        makeflow_log_alloc_event(n->d, storage_allocation);
    } else if (storage_allocation && storage_allocation->locked) {
        printf("Unable to release space\n");
    }

	return MAKEFLOW_HOOK_SUCCESS;
}

struct dag *file_find_dag(struct dag_file *f){
	struct dag *d = NULL;
	if(f->created_by){
		d = f->created_by->d;
	} else {
		struct dag_node *n;
		list_first_item(f->needed_by);
		n = list_next_item(f->needed_by);
		d = n->d;
	}
	return d;
}

static int file_deleted(struct dag_file *f){
	struct dag *d = file_find_dag(f);
	if(storage_allocation && f->created_by)
		makeflow_alloc_release_space(storage_allocation, f->created_by, f->actual_size, MAKEFLOW_ALLOC_RELEASE_USED);
	if(storage_allocation)
		makeflow_log_alloc_event(d, storage_allocation);
	
	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_storage_allocation = {
	.module_name = "Storage Allocation",
	.create = create,
	.destroy = destroy,

	.dag_start = dag_start,
	.dag_end = dag_end,

	.node_create = node_create,
	.node_submit = node_submit,
	.node_success = node_success,

	.file_deleted = file_deleted,
};


