#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include "twister.h"
#include "list.h"
#include "rmsummary.h"
#include "qbucket.h"

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
static double increase_rate = 2;

/* maximum number of tasks to keep in qbucket */
static int max_num_tasks = 5000;

/* default resource values to request for new tasks in cold start phase */
struct qbucket_resources qbucket_default_request = {1, 1000, 1000, 0};

/* --------------------------------- */
/* End of listing of hyperparameters */

/* random seed */
static uint64_t seed = 17052022;

void init_default_qbucket_resources(struct qbucket_resources *qbres, double cores, double mem, double disk, double gpus, double sig)
{
    qbres = malloc(sizeof(*qbres));
    qbres->cores = cores;
    qbres->memory = mem;
    qbres->disk = disk;
    qbres->gpus = gpus;
    qbres->sig = sig;
}

struct qbucket *qbucket_create()
{
    struct qbucket *qb = malloc(sizeof(*qb));
    if (qb == NULL)
    {
        fprintf(stderr, "Cannot initialize a qbucket struct.\n");
        return NULL;
    }
    return qb;
}

void qbucket_destroy(struct qbucket *qb)
{
    list_free_and_delete(qb->sorted_cores);
    list_free_and_delete(qb->sorted_mem);
    list_free_and_delete(qb->sorted_disk);
    list_free_and_delete(qb->sorted_gpus);
    list_free_and_delete(qb->buckets_cores);
    list_free_and_delete(qb->buckets_mem);
    list_free_and_delete(qb->buckets_disk);
    list_free_and_delete(qb->buckets_gpus);
    list_free_and_delete(qb->default_request);
    list_free_and_delete(qb->max_request);
    free(qb);
}

int init_qbucket(int qbucket_id, struct qbucket *qb)
{
    if (!qb) 
    {
        fprintf(stderr, "Null pointer to a qbucket struct.\n");
        return 1;
    }

    qb->qbucket_id = qbucket_id;
    qb->num_cold_start_tasks = num_cold_start_tasks;
    qb->increase_rate = increase_rate;
    qb->max_num_tasks = max_num_tasks;
    qb->total_tasks = 0;
    qb->cold_start_phase = 1;
    qb->max_num_tasks = max_num_tasks;
 
    init_default_qbucket_resources(qb->default_request, qbucket_default_request->cores,
            qbucket_default_request->mem, qbucket_default_request->disk,
            qbucket_default_request->gpus, -1);
    qb->max_request = NULL;

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

struct qbucket_task qbucket_task_create()
{
    struct qbucket_task *qbtask = malloc(sizeof(*qbtask));
    return qbtask;
}

void qbucket_task_destroy(struct qbucket_task *qbtask)
{
    free(qbtask);
}

static double __partitioning_policy(double p1, double p2, double delim_res, double max_res, 
        int i, int low_index, int high_index, int num_tasks_above_delim, 
        struct list *sorted_res, double *bot_sig)
{
    double exp_cons_lq_delim;
    struct list_cursor *sorted_res_cur;
    struct qbucket_task **qbtask;
    double sig;
    double cons;
    double all_sig;
    int j;
   
    double cost_lower_hit;
    double cost_lower_miss;
    double cost_upper_hit;
    double cost_upper_miss;
    double exp_cons_g_delim;
    double delim_cost;

    all_sig = bot_sig[high_index - low_index];

    sorted_res_cur = list_cursor_create_and_seek(sorted_res, low_index);
    
    for (j = low_index; j <= i; ++j, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        sig = (*qbtask)->sig;
        cons = (*qbtask)->measured_cons;
        exp_cons_lq_delim = (sig/all_sig) * cons;
    }

    cost_lower_hit = p1 * (p1 * (delim_res - exp_cons_lq_delim));
    cost_lower_miss = p1 * (p2 * (max_res - exp_cons_lq_delim));

    assert(list_seek(sorted_res_cur, i+1), true);
    for (j = i+1; j <= high_index; ++j, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        sig = (*qbtask)->sig;
        cons = (*qbtask)->measured_cons;
        exp_cons_g_delim = (sig/all_sig) * cons;       
    }

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
    
    delim_cost = cost_lower_hit + cost_lower_miss + cost_upper_hit + cost_upper_miss;
    list_cursor_destroy(sorted_res_cur);
    return delim_cost;
}

static struct list *__create_one_num_list(double num)
{
    struct list *l;
    double *index;
    l = list_create();
    index = malloc(sizeof(*index));
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

    int num_tasks;
    double bot_sig[num_tasks];
    double sum_sig;
    struct list_cursor *sorted_res_cur;
    struct qbucket_task **qbtask;
    int i;
    double max_res;

    double cost;
    int split_index;
    double delim_res;
    int num_tasks_above_delim;
    double p1;
    double p2;
    double delim_cost;

    struct list *indices_low;
    struct list *indices_high;
    struct list *l;
    
    num_tasks = high_index - low_index + 1;
    sum_sig = 0;
    sorted_res_cur = list_cursor_create_and_seek(sorted_res, low_index);
    
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
    
    cost = -1;
    split_index = -1;
    assert(list_seek(sorted_res_cur, low_index), true);
     
    for (i = low_index; i <= high_index; ++i, list_next(sorted_res_cur))
    {
        assert(list_get(sorted_res_cur, qbtask), true);
        delim_res = (*qbtask)->measured_cons;
        num_tasks_above_delim = num_tasks - (i - low_index + 1);
        p1 = bot_sig[i-low_index]/bot_sig[high_index-low_index];
        p2 = 1 - p1;

        delim_cost = __partitioning_policy(p1, p2, delim_res, max_res, i, low_index, 
                high_index, num_tasks_above_delim, sorted_res, bot_sig); 
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
    indices_low = __bucket_partitioning(sorted_res, low_index, split_index);
    indices_high = __bucket_partitioning(sorted_res, split_index+1, high_index);
    l = list_splice(indices_low, indices_high);
    return l;
}

static double __get_allocation_resource(struct qbucket *qb, const char *res_type,
        double last_res, int res_exceeded)
{
    struct list *buckets_res;
    struct list *sorted_res;
    double max_res;
    
    //track which base index to start considering next allocation for current task
    int base_index;
    struct list_cursor *sorted_res_cur;
    struct qbucket_task **qbtask;

    struct list_cursor *buckets_res_cur;
    int delim_res_ind;
    double delim_res;
    double **tmp_buckets_item;
    int i;
    int j;

    int num_buckets_resource;
    int num_completed_tasks;
    double weighted_bucket_resource[num_buckets_resource];
    int last_delim_res_ind;
    double bucket_weight;

    int64_t total_sample_space_resource;
    double random_num;
    double cumulative_density;

    if (qb->max_request == NULL)
    {
        fprintf(stderr, "Maximum request for any task is not specified.\n");
    }
    if (strncmp(res_type, "cores", 5) == 0)     /* 'cores' has 5 letters */
    {
        buckets_res = qb->buckets_cores;
        sorted_res = qb->sorted_cores;    
        max_res = qb->max_request->cores;
    }
    else if (strncmp(res_type, "memory", 6) == 0)     /* 'memory' has 6 letters */
    {
        buckets_res = qb->buckets_mem;
        sorted_res = qb->sorted_mem;
        max_res = qb->max_request->mem;
    }
    else if (strncmp(res_type, "disk", 4) == 0)     /* 'disk' has 4 letters */
    {
        buckets_res = qb->buckets_disk;
        sorted_res = qb->sorted_disk;
        max_res = qb->max_request->disk;
    }
    else if (strncmp(res_type, "gpus", 4) == 0)     /* 'gpus' has 4 letters */
    {
        buckets_res = qb->buckets_gpus;
        sorted_res = qb->sorted_gpus;
        max_res = qb->max_request->gpus;
    }
    else
    {
        fprintf(stderr, "Unknown resource type when allocating resources with qbucket.\n");
    }

    //get last value in sorted_res
    sorted_res_cur = list_cursor_create_and_seek(sorted_res, -1);
    assert(list_get(sorted_res_cur, qbtask), true);

    //if task has never been allocated before
    if (last_res == -1)
    {
        base_index = 0;     //use all buckets
    }

    else if (last_res > (*qbtask)->measured_cons)
    {
        if (qb->increase_rate * last_res > max_res)
        {
            list_cursor_destroy(sorted_res_cur);
            return max_res;
        }
        else
        {
            list_cursor_destroy(sorted_res_cur);
            return qb->increase_rate * last_res;
        }
    }
    else
    {
        buckets_res_cur = list_cursor_create_and_seek(buckets_res, 0);
        
        for (i = 0; i < list_length(buckets_res); ++i, list_next(buckets_res_cur))
        {
            assert(list_get(buckets_res_cur, tmp_buckets_item), true);
            delim_res_ind = **tmp_buckets_item;
            assert(list_seek_and_get(sorted_res_cur, delim_res_ind, qbtask), true);
            delim_res = (*qb_task)->measured_cons;
            if (res_exceeded)
            {
                if (last_res >= delim_res)
                    continue;
                else
                {
                    base_index = i;
                    break;
                }
            }
            else
            {
                if (last_res > delim_res)
                    continue;
                else
                {
                    base_index = i;
                    break;
                }
            }
        }
        assert(list_seek_and_get(sorted_res_cur, -1, qbtask), true);
        if (last_res == (*qbtask)->measured_cons)
        {
            base_index = list_length(buckets_res) - 1;
        }
    }

    num_buckets_resource = list_length(buckets_res);
    num_completed_tasks = list_length(sorted_res);
    last_delim_res_ind = -1;
    for (i = 0; i < num_buckets_resource; ++i)
    {
        bucket_weight = 0;
        assert(list_seek_and_get(buckets_res_cur, i, tmp_buckets_item), true);
        assert(list_seek_and_get(sorted_res_cur, **tmp_buckets_item, qbtask), true);
        delim_res_ind = (*qbtask)->measured_cons;
        if (last_delim_res_ind == -1)
        {
            for (j = 0; j <= delim_res_ind; ++j)
            {
                assert(list_seek_and_get(sorted_res_cur, j, qbtask), true);
                bucket_weight += (*qbtask)->sig;
            }
        }
        else
        {
            for (j = last_delim_res_ind+1; j <= delim_res_ind; ++j)
            {
                assert(list_seek-and_get(sorted_res_cur, j, qbtask), true);
                bucket_weight += (*qbtask)->sig;
            }
        }
        weighted_bucket_resource[i] = bucket_weight;
        last_delim_res_ind = delim_res_ind;
    }

    total_sample_space_resource = 0;
    for (i = base_index; i <= list_length(buckets_res); ++i)
    {
        total_sample_space_resource += weighted_bucket_resource[i];
    }

    twister_init_genrand64(seed);

    random_num = twister_genrand64_real1();
    cumulative density = 0;

    for (i = base_index; i < num_buckets_resource; ++i)
    {
        if (i == num_buckets-resource - 1)
        {
        assert(list_seek_and_get(buckets_res_cur, i, tmp_buckets_item), true);
        assert(list_seek(sorted_res_cur, **tmp_buckets_item, qbtask), true);
        list_cursor_destroy(sorted_res_cur);
        list_cursor_destroy(buckets_res_cur);
        return (*qbtask)->measured_cons;
        }
        else
        {
            cumulative_density += weighted_bucket_resource[i]/total_sample_space_resource;
            if (random_num <= cumulative_density)
            {
                assert(list_seek_and_get(buckets_res_cur, i, tmp_buckets_item), true);
                assert(list_seek_and_get(sorted_res_cur, **tmp_buckets_item, qbtask), true);
                list_cursor_destroy(sorted_res_cur);
                list_cursor_destroy(buckets_res_cur);
                return (*qbtask)->measured_cons;       
            }
        }
    }

    //control should never get here
    return -1;
}

struct rmsummary *qbucket_res_to_rmsummary(struct qbucket_resources *qbres)
{
    struct rmsummary *s = rmsummary_create(-1);
    s->cores = qb_res->cores;
    s->mem = qb_res->mem;
    s->disk = qb_res->disk;
    s->gpus = qb_res->gpus;
    return s;
}

static double min(double a, double b)
{
    return a ? a<=b : b;
}

struct qbucket_resources *qbucket_resources_create()
{
    struct qbucket_resources *qbres = malloc(sizeof(*qbres));
    return qbres;
}

//need to maintain task_id -> last_attempt
struct rmsummary *get_allocation(struct qbucket *qb, struct rmsummary *task_prev_res
        struct rmsummary *task_prev_alloc)
{
    int total_tasks;
    double tcore;
    double tmem;
    double tdisk;
    double tgpus;
    double acore;
    double amem;
    double adisk;
    double agpus;
    double max_core;
    double max_mem;
    double max_disk;
    double max_gpus;

    int max_out_cond;

    struct list_cursor *sorted_cores_cur;
    struct list_cursor *sorted_mem_cur;
    struct list_cursor *sorted_disk_cur;
    struct list_cursor *sorted_gpus_cur;

    struct qbucket_task **tmp_cores_item;
    struct qbucket_task **tmp_mem_item;
    struct qbucket_task **tmp_disk_item;
    struct qbucket_task **tmp_gpus_item;

    struct qbucket_resources *qbres;

    struct rmsummary *qbsummary;

    int res_exceeded[4]; #cores, mem, disk, gpus

    total_tasks = list_length(qb->sorted_cores);
    if (task_prev_res == NULL)
    {
        if (total_tasks < qb->num_cold_start)
            return qbucket_res_to_rmsummary(qb->default_request);
        else
        {
           tcore = -1;
           tmem = -1;
           tdisk = -1;
           tgpus = -1;
        }
    }
    else 
    {
        tcore = task_prev_res->cores;
        tmem = task_prev_res->mem;
        tdisk = task_prev_res->disk;
        tgpus = task_prev_res->gpus;
    }

    max_core = qb->max_res->cores;
    max_mem = qb->max_res->mem;
    max_disk = qb->max_res->disk;
    max_gpus = qb->max_res->gpus;

    sorted_cores_cur = list_cursor_create_and_seek(qb->sorted_cores, -1);
    assert(list_get(sorted_cores_cur, tmp_cores_item), true);
    sorted_mem_cur = list_cursor_create_and_seek(qb->sorted_mem, -1);
    assert(list_get(sorted_mem_cur, tmp_mem_item), true);
    sorted_disk_cur = list_cursor_create_and_seek(qb->sorted_disk, -1);
    assert(list_get(sorted_disk_cur, tmp_disk_item), true);
    sorted_gpus_cur = list_cursor_create_and_seek(qb->sorted_gpus, -1);
    assert(list_get(sorted_gpus_cur, tmp_gpus_item), true);
    list_cursor_destroy(sorted_cores_cur);
    list_cursor_destroy(sorted_mem_cur);
    list_cursor_destroy(sorted_disk_cur);
    list_cursor_destroy(sorted_gpus_cur);
    max_out_cond = ((tcore >= (*tmp_cores_item)->measured_cons) && (tmem >= (*tmp_mem_item)->measured_cons) && (tdisk >= (*tmp_disk_item)->measured_cons) && (tgpus >= (*tmp_gpus_item)->measured_cons));

    qbres = qbucket_resources_create();
    if ((total_tasks < qb->num_cold_start) || max_out_cond)
    {
        qbres->cores = min(tcore * qb->increase_rate, max_core);
        qbres->mem = min(tmem * qb->increase_rate, max_mem);
        qbres->disk = min(tdisk * qb->increase_rate, max_disk);
        qbres->gpus = min(tgpus * qb->increase_rate, max_gpus);
    }
    else
    {
        acore = task_prev_alloc->cores;
        amem = task_prev_alloc->mem;
        adisk = task_prev_alloc->disk;
        agpus = task_prev_alloc->gpus;
        qbres->cores = __get_allocation_resource(qb, "core", tcore, 1 ? tcore > acore : 0);
        qbres->mem = __get_allocation_resource(qb, "mem", tmem, 1 ? tmem > amem : 0);
        qbres->disk = __get_allocation_resource(qb, "disk", tdisk, 1 ? tdisk > adisk : 0);
        qbres->gpus = __get_allocation_resource(qb, "gpus", tgpus, 1 ? tgpus > agpus : 0);
    }
    qbsummary = qbucket_res_to_summary(qbres);
    return qbsummary;
}

double qbucket_task_priority(struct qbucket_task qbtask)
{
    return -qbtask->measured_cons;
}

int add_task(struct qbucket *qb, struct qbucket_resources qbtask)
{
    struct qbucket_task qbcore; 
    struct qbucket_task qbmem; 
    struct qbucket_task qbdisk; 
    struct qbucket_task qbgpus; 

    qbcore = qbucket_task_create();
    qbcore->measured_cons = qbtask->cores;
    qbcore->sig = qbtask->sig;
    qbcore = qbucket_task_create();
    qbcore->measured_cons = qbtask->cores;
    qbcore->sig = qbtask->sig;
    qbcore = qbucket_task_create();
    qbcore->measured_cons = qbtask->cores;
    qbcore->sig = qbtask->sig;
    qbcore = qbucket_task_create();
    qbcore->measured_cons = qbtask->cores;
    qbcore->sig = qbtask->sig;

    list_push_priority(qb->sorted_cores, (list_priority_t) qbucket_task_priority,
            qbcore);

    list_push_priority(qb->sorted_mem, (list_priority_t) qbucket_task_priority,
            qbmem);
    list_push_priority(qb->sorted_disk, (list_priority_t) qbucket_task_priority,
            qbdisk);
    list_push_priority(qb->sorted_gpus, (list_priority_t) qbucket_task_priority,
            qbgpus);

    qb->buckets_cores = __bucket_partitioning(qb->sorted_cores, 0, list_length(qb->sorted_cores));    
    qb->buckets_mem = __bucket_partitioning(qb->sorted_mem, 0, list_length(qb->sorted_mem));    
    qb->buckets_disk = __bucket_partitioning(qb->sorted_disk, 0, list_length(qb->sorted_disk));    
    qb->buckets_gpus = __bucket_partitioning(qb->sorted_gpus, 0, list_length(qb->sorted_gpus));    
}


