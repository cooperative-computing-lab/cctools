#include <stdlib.h>
#include "bucketing_manager.h"
#include "xxmalloc.h"
#include "debug.h"

/* Begin: internals */

static char* int_to_string(int n)
{
    char* s = malloc(20);   //log10(2^64) = 19.2 -> 20 should be enough
    sprintf(s, "%d", n);
    return s;
}

/* End: internals */

/* Begin: APIs */

bucketing_manager_t* bucketing_manager_create(category_mode_t mode)
{    
    bucketing_manager_t* m = xxmalloc(sizeof(*m));
    
    if (mode != CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING && mode != CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING)
    {
        fatal("Invalid bucketing mode\n");
        return 0;
    }

    m->mode = mode;
    m->res_type_to_bucketing_state = hash_table_create(0, 0);
    m->task_id_to_task_rmsummary = hash_table_create(0, 0);

    return m;
}

void bucketing_manager_delete(bucketing_manager_t* m)
{
    if (!m)
        return;

    hash_table_clear(m->res_type_to_bucketing_state, (void (*) (void*)) bucketing_state_delete);
    hash_table_delete(m->res_type_to_bucketing_state);

    hash_table_clear(m->task_id_to_task_rmsummary, (void (*) (void*)) rmsummary_delete);
    hash_table_delete(m->task_id_to_task_rmsummary);

    free(m);
}

void bucketing_manager_add_resource_type(bucketing_manager_t* m, const char* r, double default_value, int num_sampling_points, double increase_rate, int max_num_buckets)
{
    if (!m)
    {
        fatal("No bucketing manager to add resource\n");
        return;
    }

    if (!hash_table_lookup(m->res_type_to_bucketing_state, r))
    {
        bucketing_state_t* b = bucketing_state_create(default_value, num_sampling_points, increase_rate, max_num_buckets, m->mode);
        
        if (!hash_table_insert(m->res_type_to_bucketing_state, r, b))
        {
            fatal("Cannot insert bucketing state into bucket manager\n");
        }
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

    if ((b = hash_table_remove(m->res_type_to_bucketing_state, r)))
    {
        bucketing_state_delete(b);
    }
}

void bucketing_manager_set_mode(bucketing_manager_t* m, category_mode_t mode)
{
    if (!m)
    {
        fatal("No bucketing manager to set algorithm mode\n");
        return;
    }

    if (mode != CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING && mode != CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING)
    {
        fatal("Invalid bucketing mode\n");
        return;
    }
   
    m->mode = mode;
}

//TODO: add set functions to set bucketing state external fields

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

    struct rmsummary* old_res = hash_table_lookup(m->task_id_to_task_rmsummary, task_id_str);
    double old_val;
    struct rmsummary* pred_res = rmsummary_create(0);
    double pred_val;
    
    struct hash_table* ht = m->res_type_to_bucketing_state;
    hash_table_firstkey(ht);
        
    while (hash_table_nextkey(ht, &res_name, (void**) &state))
    {
        if (!old_res)
        {
            old_val = -1;   //-1 means no prev value
        }
        else
        {
            old_val = rmsummary_get(old_res, res_name);
            
            if (old_val <= 0)   //uninitialized value means task needs new prediction
                old_val = -1;

        }
        
        if (old_val < 3)
        {
            old_val = 2;
        }
        pred_val = bucketing_predict(state, old_val);

        if (pred_val == -1)
        {
            fatal("Problem predicting value in bucketing\n");
            return 0;
        }

        rmsummary_set(pred_res, res_name, pred_val);
    }

    if (old_res)
    {
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
        rmsummary_delete(old_res);
    }

    hash_table_insert(m->task_id_to_task_rmsummary, task_id_str, pred_res);
    
    free(task_id_str);
    
    return pred_res;
}

void bucketing_manager_add_task(bucketing_manager_t* m, int task_id, struct rmsummary* r, int success)
{
    if (!m)
    {
        fatal("No bucketing manager to add task's resources\n");
        return;
    }

    struct rmsummary* old_r;
    char* task_id_str = int_to_string(task_id);
    
    if ((old_r = hash_table_lookup(m->task_id_to_task_rmsummary, task_id_str)))
    {
        rmsummary_delete(old_r);
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
    }

    hash_table_insert(m->task_id_to_task_rmsummary, task_id_str, r);
    
    if (success)
    {
        struct hash_table* ht = m->res_type_to_bucketing_state;
        char* res_name;
        bucketing_state_t* state;
        double val;

        hash_table_firstkey(ht);

        while(hash_table_nextkey(ht, &res_name, (void**) &state))
        {
            val = rmsummary_get(r, res_name);
            bucketing_add(state, val);
        }

        rmsummary_delete(hash_table_lookup(m->task_id_to_task_rmsummary, task_id_str));
        hash_table_remove(m->task_id_to_task_rmsummary, task_id_str);
    }

    free(task_id_str);
}

/* End: APIs */
