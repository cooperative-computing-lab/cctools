#include "list.h"
#include "rmsummary.h"
#include <assert.h>
#include <stdbool.h>

/** @file qbucket.c Implements the QBucket algorithm to allocate resources to tasks.
 * Each task's category has its own QBucket object
 */

/* Listing of default values of hyperparameters of QBucket algorithm */
/* ----------------------------------------------- */

/* number of tasks needed to be run to warm up QBucket (fill it with some values)
 * -> cold start tasks */
static int64_t num_cold_start_tasks = 10;

/* rate to increase resources when tasks fail in cold start phase or when 
 * tasks exceed all buckets in QBucket */
static int increase_rate = 2;

/* maximum number of tasks to keep in qbucket */
static int max_num_tasks = 5000;

/* default resource values to request for new tasks in cold start phase */
static struct qbucket_resources
{
    int cores = 1;
    int memory = 1000;      //in MBs
    int disk = 1000;        //in MBs
    int gpus = 0;
} default_resource_values;

/* --------------------------------- */
/* End of listing of hyperparameters */

/* QBucket_task structure, only track 1 type of resource per structure instance */
static struct qbucket_task
{

    /* measured consumption of 1 resource */
    int measured_cons;

    /*significance value of current task */
    int sig;
}

/* QBucket structure */
struct qbucket
{

    /*qbucket id*/
    int qbucket_id;

    /* number of tasks needed to be run to warm up QBucket (fill it with some values)
    * -> cold start tasks */
    int64_t num_cold_start_tasks;
    
    /* rate to increase resources when tasks fail in cold start phase or when 
     * tasks exceed all buckets in QBucket */
    int increase_rate;
    
    /* default resource request, only use fields in {cores, memory, disk, gpus} */
    struct qbucket_resources *default_request;

    /* total number of tasks completed without errors from WQ or resource exhaustion*/
    int64_t total_tasks;

    /* whether qbucket is in cold start phase, 1 is yes, 0 is no */
    int cold_start_phase;

    /* maximum number of tasks to keep in qbucket */
    int max_num_tasks;

    /* The following arrays all have the same length */
    /* --------------------------------------------------- */
    
    /* a doubly linked list of qbucket_tasks of cores in sorted order */
    struct list *sorted_cores;

    /* a doubly linked list of qbucket_tasks of memory in sorted order */
    struct list *sorted_mem;
    
    /* a doubly linked list of qbucket_tasks of disk in sorted order */
    struct list *sorted_disk;
    
    /* a doubly linked list of qbucket_tasks of gpus in sorted order */
    struct list *sorted_gpus;
    
    /* a doubly linked list of indices to qbucket_tasks of cores
     * where each index is a bucket's delimiter */
    struct list *buckets_cores;

    /* a doubly linked list of indices to qbucket_tasks of memory
     * where each index is a bucket's delimiter */
    struct list *buckets_mem;

    /* a doubly linked list of indices to qbucket_tasks of disk
     * where each index is a bucket's delimiter */
    struct list *buckets_disk;

    /* a doubly linked list of indices to qbucket_tasks of gpus
     * where each index is a bucket's delimiter */
    struct list *buckets_gpus;
    
    /* ------------------------ */
    /* End of listing of arrays */
}

/* Initialize a qbucket
 * @param qbucket_id the id of the new qbucket
 * @return 1 if there's a problem
 * @return 0 if all is good
 */
int init_qbucket(int qbucket_id, struct qbucket *qb)
{
    if (!qb) 
    {
        fprintf(stderr, "Cannot initialize a qbucket with NULL pointer.\n");
        return 1;
    }

    qb->qbucket_id = qbucket_id;
    qb->num_cold_start_tasks = num_cold_start_tasks;
    qb->increase_rate = increase_rate;
    qb->max_num_tasks = max_num_tasks;
    qb->total_tasks = 0;
    qb->cold_start_phase = 1;
    qb->max_num_tasks = max_num_tasks;
    
    qb->sorted_cores = list_create();
    qb->sorted_mem = list_create();
    qb->sorted_disk = list_create();
    qb->sorted_gpus = list_create();
    qb->buckets_cores = list_create();
    qb->bucket_mem = list_create();
    qb->bucket_disk = list_create();
    qb->bucket_gpus = list_create();
    return 0;
}

static int __partitioning_policy(int p1, int p2, int delim_res, int max_res, int i, 
        int low_index, int high_index, int num_tasks_above_delim, 
        struct list *sorted_res, int *bot_sig)
{
    int exp_cons_lq_delim;
    struct list_cursor *sorted_res_cur = list_cursor_create(sorted_res);
    assert(list_seek(sorted_res_cur, low_index), true);
    struct qbucket_task **qbtask;
    int sig;
    int cons;
    int all_sig = bot_sig[high_index - low_index];
    int j;
    for (j = low_index; j <= i; ++j, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        sig = (*qbtask)->sig;
        cons = (*qbtask)->measured_cons;
        exp_cons_lq_delim = (sig/all_sig) * cons;
    }

    int cost_lower_hit = p1 * (p1 * (delim_res - exp_cons_lq_delim));
    int cost_lower_miss = p1 * (p2 * (max_res - exp_cons_lq_delim));

    int exp_cons_g_delim;
    assert(list_seek(sorted_res_cur, i+1), true);
    for (int j = i+1; j <= high_index; ++j, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        sig = (*qbtask)->sig;
        cons = (*qbtask)->measured_cons;
        exp_cons_g_delim = (sig/all_sig) * cons;       
    }

    int cost_upper_hit;
    int cost_upper_miss;
    if (num_tasks_above_delim == 0)
    {
        cost_upper_hit = 0;
        cost_upper_miss = 0;
    }
    else
    {
        cost_upper_hit = p2 * (p2 * (max_res - exp_cons_g_delim));
        cost_upper_miss = p2 * (p1 * (delim_res + max_res - exp_cons_g_delim));
    }
    
    int delim_cost = cost_lower_hit + cost_lower_miss + cost_upper_hit + cost_upper_miss;
    list_cursor_destroy(sorted_res_cur);
    return delim_cost;
}

static struct list *__create_one_num_list(int num)
{
    struct list *l = list_create();
    int *index = malloc(sizeof(*index));
    if (!index) fprintf(stderr, "Out of memory.\n");
    *index = num;
    list_push_head(l, index);
    return l;
}

static struct list *__bucket_partitioning(struct list *sorted_res, int low_index, int high_index)
{
    if (low_index == high_index)
    {
        return __create_one_num_list(high_index);    
    }

    int num_tasks = high_index - low_index + 1;
    int sum_sig = 0;
    int bot_sig[num_tasks];
    struct list_cursor *sorted_res_cur = list_cursor_create(sorted_res);
    assert(list_seek(sorted_res_cur, low_index), true);

    struct qbucket_task **qbtask;
    int i;
    int max_res;
    for (i = low_index; i <= high_index; ++i, list_next(sorted_res_cur))
    {   
        assert(list_get(sorted_res_cur, qbtask), true);
        if (i == high_index)
            max_res = (*qbtask)->measured_cons;
        
        if (i == low_index)
            bot_sig[i-low_index] = (*qbtask)->sig;
        else
            bot_sig[i-low_index] = bot_sig[i-low_index-1] + (*qbtask)->sig;
    }
    
    int cost = -1;
    int split_index = -1;
    i;
    assert(list_seek(sorted_res_cur, low_index), true);
    
    int delim_res;
    int num_tasks_above_delim;
    int p1;
    int p2;
    int delim_cost;
    for (i = low_index; i <= high_index; ++i, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        delim_res = (*qbtask)->measured_cons;
        num_tasks_above_delim = num_tasks - (i - low_index + 1);
        p1 = bot_sig[i-low_index]/bot_sig[high_index-low_index];
        p2 = 1 - p1;

        delim_cost = __partitioning_policy(p1, p2, delim_res, max_res, i, low_index, high_index, num_tasks_above_delim, sorted_res, bot_sig); 
        if (cost == -1 || cost > delim_cost)
        {
            cost = delim_cost;
            split_index = i;        
        }
    }

    if (split_index == high_index)
    {
        list_cursor_destroy(sorted_res_cur); 
        return __create_one_num_list(high_index); 
    }

    list_cursor_destroy(sorted_res_cur); 
    struct list *indices_low = __bucket_partitioning(sorted_res, low_index, split_index);
    struct list *indices_high = __bucket_partitioning(sorted_res, split_index+1, high_index);
    struct list *l = list_splice(indices_low, indices_high);
    return l;
}

static int __get_allocation_resource()
{

}

//need to maintain task_id -> last_attempt
struct rmsummary *get_allocation()
{
        
}

int add_task()
{

}
