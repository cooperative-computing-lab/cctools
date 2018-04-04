
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

/* Flag to indicate that jobs were cleaned and to loop again. */
int cleaned_completed_node = 0;

/* Both of these flags are used to signal if there where tasks that
 * were skipped and never run as a result of storage allocation. */
/* Flag to indicate that the allocation failed to find available space. */
int failed_allocation_check = 0;

/* Flag to indicate a node was skipped to active node dependecies. */
int failed_dependencies_check = 0;

static int create( void ** instance_struct, struct jx *args){
	// There can only be one storage alloc
	if(storage_allocation) return MAKEFLOW_HOOK_FAILURE;

	if(jx_lookup_string(args, "storage_allocation_print")){
		storage_print = xxstrdup(jx_lookup_string(args, "storage_allocation_print"));
		printf("Storage Print = %s\n", storage_print);
	}

	uint64_t storage_limit = jx_lookup_integer(args, "storage_allocation_limit");
	printf("Storage Limit = %" PRIu64 "\n", storage_limit);
	int storage_type  = jx_lookup_integer(args, "storage_allocation_type");
	printf("Storage Type = %d\n", storage_type);
    if(storage_limit || storage_type != MAKEFLOW_ALLOC_TYPE_NOT_ENABLED){
        storage_allocation = makeflow_alloc_create(-1, NULL, storage_limit, 1, storage_type);
    }
	
	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( void * instance_struct, struct dag *d ){
	free(storage_print);
	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_check( void * instance_struct, struct dag *d){
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
        return MAKEFLOW_HOOK_END;
    }
    uint64_t end = timestamp_get();
    static_analysis += end - start;
    dag_node_delete(n);

	return MAKEFLOW_HOOK_SUCCESS;
}

static int dag_loop( void * instance_struct, struct dag *d){
	if(cleaned_completed_node == 1){
		cleaned_completed_node = 0;
		failed_allocation_check = 0;
		failed_dependencies_check = 0;
		return MAKEFLOW_HOOK_SUCCESS;
	}
	return MAKEFLOW_HOOK_END;
}

static int dag_end( void * instance_struct, struct dag *d){

    if(storage_allocation){
        makeflow_log_alloc_event(d, storage_allocation);
        makeflow_log_event(d, "STATIC_ANALYSIS", static_analysis);
        makeflow_log_event(d, "DYNAMIC_ALLOC", makeflow_alloc_get_dynamic_alloc_time());
    }

	if(failed_allocation_check || failed_dependencies_check){
		return MAKEFLOW_HOOK_FAILURE;
	}

	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_check( void * instance_struct, struct dag_node *n, struct batch_queue *q){

    if(storage_allocation->locked){
        if(!( makeflow_alloc_check_space(storage_allocation, n))){
			failed_allocation_check = 1;
            return MAKEFLOW_HOOK_SKIP;
        }

        if (!(dag_node_footprint_dependencies_active(n))){
			failed_dependencies_check = 1;
            return MAKEFLOW_HOOK_SKIP;
        }
    }

	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit( void * instance_struct, struct dag_node *n, struct batch_task *task){
    if(makeflow_alloc_commit_space(storage_allocation, n)){
        makeflow_log_alloc_event(n->d, storage_allocation);
    } else if (storage_allocation->locked)  {
        debug(D_MAKEFLOW_HOOK, "Unable to commit enough space for execution\n");
		return MAKEFLOW_HOOK_FAILURE;
    }
	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_success( void * instance_struct, struct dag_node *n, struct batch_task *task){
	struct dag_file *f = NULL;
	cleaned_completed_node = 1;
	
    if(makeflow_alloc_use_space(storage_allocation, n)){
		makeflow_log_alloc_event(n->d, storage_allocation);
	}

    /* Mark source files that have been used by this node */
    list_first_item(n->source_files);
    while((f = list_next_item(n->source_files))) {
        if(f->state == DAG_FILE_STATE_COMPLETE){
            if(storage_allocation->locked && f->type != DAG_FILE_TYPE_OUTPUT)
				makeflow_clean_file(n->d, makeflow_get_queue(n), f);
		}
	}

    /* Delete output files that have no use and are not actual outputs */
    if(storage_allocation->locked){
        list_first_item(n->target_files);
		while((f = list_next_item(n->target_files))){
			if(f->reference_count == 0 && f->type != DAG_FILE_TYPE_OUTPUT)
				makeflow_clean_file(n->d, makeflow_get_queue(n), f);
		}
	}

    if(makeflow_alloc_release_space(storage_allocation, n, 0, MAKEFLOW_ALLOC_RELEASE_COMMIT)) {
        makeflow_log_alloc_event(n->d, storage_allocation);
    } else if (storage_allocation->locked) {
        debug(D_MAKEFLOW_HOOK, "Unable to release space\n");
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

static int file_deleted( void * instance_struct, struct dag_file *f){
	struct dag *d = file_find_dag(f);
	if(f->created_by)
		makeflow_alloc_release_space(storage_allocation, f->created_by, f->actual_size, MAKEFLOW_ALLOC_RELEASE_USED);

	makeflow_log_alloc_event(d, storage_allocation);
	
	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_storage_allocation = {
	.module_name = "Storage Allocation",
	.create = create,
	.destroy = destroy,

	.dag_check = dag_check,
	.dag_loop = dag_loop,
	.dag_end = dag_end,

	.node_check = node_check,
	.node_submit = node_submit,
	.node_success = node_success,

	.file_deleted = file_deleted,
};


