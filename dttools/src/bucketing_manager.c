#include <stdlib.h>
#include "bucketing_manager.h"
#include "xxmalloc.h"
#include "debug.h"

/* Begin: internals */

/* List of default parameters for a bucketing manager */
static const double default_cores = 1;
static const double default_mem = 1000;
static const double default_disk = 1000;
static const double default_gpus = 0;
static const int default_num_sampling_points = 10;
static const double default_increase_rate = 2;
static const double default_max_num_buckets = 10;
static const int default_update_epoch = 1;

/* Convert an int to its string representation
 * @param n the integer
 * @return a malloc'ed string in decimal of n */
static char* int_to_string(int n)
{
    char* s = xxmalloc(20);   //log10(2^64) = 19.2 -> 20 should be enough
    sprintf(s, "%d", n);
    return s;
}

/* Add default resource types with default values per resource type to manager
 * See this function's definition for more info
 * @param m the bucketing manager */
static void bucketing_manager_add_default_resource_types(bucketing_manager_t* m)
{
    bucketing_manager_add_resource_type(m, "cores", 0, 
            default_cores, default_num_sampling_points, default_increase_rate, 
            default_max_num_buckets, default_update_epoch);
    bucketing_manager_add_resource_type(m, "mem", 0, 
            default_mem, default_num_sampling_points, default_increase_rate, 
            default_max_num_buckets, default_update_epoch);
    bucketing_manager_add_resource_type(m, "disk", 0, 
            default_disk, default_num_sampling_points, default_increase_rate, 
            default_max_num_buckets, default_update_epoch);
    bucketing_manager_add_resource_type(m, "gpus", 0, 
            default_gpus, default_num_sampling_points, default_increase_rate, 
            default_max_num_buckets, default_update_epoch);
}
/* End: internals */

/* Begin: APIs */

bucketing_manager_t* bucketing_manager_create(bucketing_mode_t mode)
{    
    bucketing_manager_t* m = xxmalloc(sizeof(*m));
    
    /* only support two bucketing modes */
    if (mode != BUCKETING_MODE_GREEDY && mode != BUCKETING_MODE_EXHAUSTIVE)
    {
        fatal("Invalid bucketing mode\n");
        return 0;
    }

    m->mode = mode;
    m->res_type_to_bucketing_state = hash_table_create(0, 0);
    m->task_id_to_task_rmsummary = hash_table_create(0, 0);

    return m;
}

bucketing_manager_t* bucketing_manager_initialize(bucketing_mode_t mode)
{
    bucketing_manager_t* m = bucketing_manager_create(mode);
    bucketing_manager_add_default_resource_types(m);
    return m;
}

void bucketing_manager_delete(bucketing_manager_t* m)
{
    if (!m)
        return;

    /* delete all hash tables' contents and themselves */
    hash_table_clear(m->res_type_to_bucketing_state, (void (*) (void*)) bucketing_state_delete);
    hash_table_delete(m->res_type_to_bucketing_state);

    hash_table_clear(m->task_id_to_task_rmsummary, (void (*) (void*)) rmsummary_delete);
    hash_table_delete(m->task_id_to_task_rmsummary);

    free(m);
}

void bucketing_manager_add_resource_type(bucketing_manager_t* m, const char* r, 
        int set_default, double default_value, int num_sampling_points, 
        double increase_rate, int max_num_buckets, int update_epoch)
{
    if (!m)
    {
        fatal("No bucketing manager to add resource\n");
        return;
    }

    bucketing_state_t* b; 
    /* only add resource type if it doesn't exist, warn otherwise */
    if (!hash_table_lookup(m->res_type_to_bucketing_state, r))
    {
        if (set_default)
        {
            if (!strcmp(r, "cores"))
            {
                b = bucketing_state_create(default_cores, default_num_sampling_points, 
                        default_increase_rate, default_max_num_buckets, m->mode, default_update_epoch);
            }
            else if (!strcmp(r, "memory"))
            {
                b = bucketing_state_create(default_mem, default_num_sampling_points, 
                        default_increase_rate, default_max_num_buckets, m->mode, default_update_epoch);
            }
            else if (!strcmp(r, "disk"))
            {
                b = bucketing_state_create(default_disk, default_num_sampling_points, 
                        default_increase_rate, default_max_num_buckets, m->mode, default_update_epoch);
            }
            else if (!strcmp(r, "gpus"))
            {
                b = bucketing_state_create(default_gpus, default_num_sampling_points, 
                        default_increase_rate, default_max_num_buckets, m->mode, default_update_epoch);
            }
            else
            {
                warn(D_BUCKETING, "resource type %s is not supported to set default\n", r);
                return;
            }
        }
        else
        {
            b = bucketing_state_create(default_value, num_sampling_points, increase_rate, max_num_buckets, m->mode, update_epoch);
        }
        
        if (!hash_table_insert(m->res_type_to_bucketing_state, r, b))
            fatal("Cannot insert bucketing state into bucket manager\n");
    }
    else
    {
        warn(D_BUCKETING, "Ignoring request to add %s as a resource type as it already exists in the given bucketing manager\n", r);
    }
}

void bucketing_manager_remove_resource_type(bucketing_manager_t* m, const char* r)
{  
    if (!m)
    {
        fatal("No bucketing_manager to remove resource\n");
        return;
    }

    bucketing_state_t* b;

    /* only remove resource if it exists, do nothing otherwise */
    if ((b = hash_table_remove(m->res_type_to_bucketing_state, r)))
    {
        bucketing_state_delete(b);
    }
}

void bucketing_manager_set_mode(bucketing_manager_t* m, bucketing_mode_t mode)
{
    if (!m)
    {
        fatal("No bucketing manager to set algorithm mode\n");
        return;
    }

    /* can only set two modes of bucketing */
    if (mode != BUCKETING_MODE_GREEDY && mode != BUCKETING_MODE_EXHAUSTIVE)
    {
        fatal("Invalid bucketing mode\n");
        return;
    }
   
    m->mode = mode;
}

void bucketing_manager_tune_by_resource(bucketing_manager_t* m, const char* res_name, 
        const char* field, void* val)
{
    if (!m)
    {
        fatal("No manager to tune\n");
        return;
    }

    if (!res_name)
    {
        fatal("No resource to tune\n");
        return;
    }

    if (!field)
    {
        fatal("No field to tune bucketing state of resource %s", res_name);
        return;
    }

    if (!val)
    {
        fatal("No value to tune field %s of bucketing state of resource %s to", field, res_name);
        return;
    }

    bucketing_state_t* tmp_s = hash_table_lookup(m->res_type_to_bucketing_state, res_name);
    if (!tmp_s)
    {
        warn(D_BUCKETING, "Bucketing state is not keeping track of resource %s\n. Ignoring..", res_name);
        return;
    }
    
    bucketing_state_tune(tmp_s, field, val);
}

struct rmsummary* bucketing_manager_predict(bucketing_manager_t* m, int task_id)
{
    if (!m)
    {
        fatal("No bucketing manager to predict resources\n");
        return 0;
    }
    
    char* task_id_str = int_to_string(task_id);
    char* res_name;
    bucketing_state_t* state;

    /* get old resource report */
    struct rmsummary* old_res = hash_table_lookup(m->task_id_to_task_rmsummary, task_id_str);
    double old_val;

    /* prepare predicted resource report/consumption */
    struct rmsummary* pred_res = rmsummary_create(-1);
    double pred_val;
    
    struct hash_table* ht = m->res_type_to_bucketing_state;
    hash_table_firstkey(ht);
        
    /* loop through all types of resources and get their respective bucketing states */
    while (hash_table_nextkey(ht, &res_name, (void**) &state))
    {
        /* if previous resource report doesn't exist, then it's a new task */
        if (!old_res)
        {
            pred_val = bucketing_predict(state, -1);    //-1 means no prev value
        }

        /* otherwise already have a value */
        else
        {
            /*get old value */
            old_val = rmsummary_get(old_res, res_name);

            /* if task doesn't exceed limits or it does but not this resource */
            if (!old_res->limits_exceeded || (old_res->limits_exceeded && rmsummary_get(old_res->limits_exceeded, res_name) == -1))
            {
                /* if this resource is a newly added resource, predict a new value */
                if (old_val == -1)
                    pred_val = bucketing_predict(state, old_val);
                
                /* otherwise this resource doesn't exceed limit so return the same value */
                else
                    pred_val = old_val;
            }

            /* if it does exceed then predict */
            else
            {
                pred_val = bucketing_predict(state, old_val);
            }
        }
       
        if (pred_val == -1)
        {
            fatal("Problem predicting value in bucketing\n");
            return 0;
        }

        rmsummary_set(pred_res, res_name, pred_val);
    }

    /* replace the old resource report with the predicted one */
    if (old_res)
    {
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
        rmsummary_delete(old_res);
    }

    /* bucketing manager keeps its own copy of datum */
    struct rmsummary* pred_res_copy = rmsummary_copy(pred_res, 1);
    hash_table_insert(m->task_id_to_task_rmsummary, task_id_str, pred_res_copy);
    
    free(task_id_str);
    
    return pred_res;
}

void bucketing_manager_add_resource_report(bucketing_manager_t* m, int task_id, struct rmsummary* r, int success)
{
    if (!m)
    {
        fatal("No bucketing manager to add task's resources\n");
        return;
    }

    struct rmsummary* old_r;
    char* task_id_str = int_to_string(task_id);
    struct rmsummary* new_r = rmsummary_copy(r, 1);

    /* replace the old report with new one if possible */
    if ((old_r = hash_table_lookup(m->task_id_to_task_rmsummary, task_id_str)))
    {
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
        rmsummary_delete(old_r);
    }

    hash_table_insert(m->task_id_to_task_rmsummary, task_id_str, new_r);
    
    /* if task successfully finishes then clear out its index in internal table */
    if (success == 1)
    {
        struct hash_table* ht = m->res_type_to_bucketing_state;
        char* res_name;
        bucketing_state_t* state;
        double val;

        hash_table_firstkey(ht);

        while(hash_table_nextkey(ht, &res_name, (void**) &state))
        {
            val = rmsummary_get(new_r, res_name);
            bucketing_add(state, val);
        }

        rmsummary_delete(new_r);
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
    }

    free(task_id_str);
}

/* End: APIs */
