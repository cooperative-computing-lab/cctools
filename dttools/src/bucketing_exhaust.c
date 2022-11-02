#include <stdlib.h>
#include <stdio.h>
#include "bucketing.h"
#include "bucketing_exhaust.h"
#include "list.h"

int bucketing_exhaust_update_buckets(bucketing_state *s)
{
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    s->sorted_buckets = bucketing_exhaust_get_min_cost_bucket_list(s);
    
    return 0;
}

struct list* bucketing_exhaust_get_min_cost_bucket_list(bucketing_state* s)
{
    double cost;
    double min_cost = -1;
    struct list* best_bucket_list = 0;
    struct list* bucket_list = 0;

    for (int i = 0; i < s->max_num_buckets; ++i)
    {
        bucket_list = bucketing_exhaust_get_buckets(s, i + 1);
        cost = bucketing_exhaust_compute_cost(s, bucket_list);
        
        if (min_cost == -1 || min_cost > cost)
        {
            min_cost = cost;
            if (best_bucket_list)
            {
                list_clear(best_bucket_list, (void (*) (void*)) bucketing_bucket_delete);
                list_delete(best_bucket_list);
            }
            best_bucket_list = bucket_list;
        }
        else
        {
            list_clear(bucket_list, (void (*) (void*)) bucketing_bucket_delete);
            list_delete(bucket_list);           
        }
    }

    return best_bucket_list;
}

struct list* bucketing_exhaust_get_buckets(bucketing_state* s, int n)
{
    double max_val = ((bucketing_point*) list_peek_tail(s->sorted_points))->val;
    double candidate_vals[n];
    
    for (int i = 0; i < n; ++i) 
        candidate_vals[i] = max_val * (i + 1) / (n * 1.0);
    
    double buck_sig = 0;
    double total_sig = 0;
    int i = 0;
    double prev_val = 0;
    double candidate_probs[n];
    list_first_item(s->sorted_points);
    bucketing_point* tmp = list_next_item(s->sorted_points);
    
    while ((tmp) && (i < n))
    {
        if (candidate_vals[i] < tmp->val)
        {
            total_sig += buck_sig;
            candidate_probs[i] = buck_sig;
            candidate_vals[i] = prev_val;
            ++i;
            buck_sig = 0;
        }
        else if (candidate_vals[i] >= tmp->val)
        {
            prev_val = tmp->val;
            buck_sig += tmp->sig;
            
            tmp = list_next_item(s->sorted_points);
        }
    }
    candidate_probs[i] = buck_sig;
    total_sig += buck_sig;

    struct list* ret = list_create();
    
    for (i = 0; i < n; ++i)
    {
        if (candidate_probs[i] != 0)
        {
            list_push_tail(ret, bucketing_bucket_create(candidate_vals[i], candidate_probs[i] / total_sig));
        }
    }

    return ret;
}

double bucketing_exhaust_compute_cost(bucketing_state* s, struct list* bucket_list)
{
    int N = list_size(bucket_list);
    double cost_table[N][N];

    double* task_exps = bucketing_exhaust_compute_task_exps(s, bucket_list);
    bucketing_bucket** bucket_array = bucketing_bucket_list_to_array(bucket_list);

    //i is task, j is chosen bucket
    for (int j = 0; j < N; ++j)
    {
        for (int i = 0; i <= j; ++i)
        {
            cost_table[i][j] = bucket_array[j]->val - task_exps[i];
        }
    }

    double* upper_bucket_probs;
    for (int i = N - 1; i > -1; --i) 
    {
        for (int j = i - 1; j > -1; --j)
        {
            cost_table[i][j] = bucket_array[j]->val;
            upper_bucket_probs = bucketing_reweight_bucket_probs(bucket_array, j + 1, N - 1);
            
            for (int k = j + 1; k < N; ++k)
            {
                cost_table[i][j] += upper_bucket_probs[k - (j + 1)] * cost_table[i][k];
            }
            free(upper_bucket_probs);
        }
    }

    double expected_cost = 0;
    for (int i = 0; i < N; ++i)
    {
        for (int j = 0; j < N; ++j) 
        {
            expected_cost += bucket_array[i]->prob * bucket_array[j]->prob * cost_table[i][j];
        }
    }
    
    free(bucket_array);
    free(task_exps);
    
    return expected_cost;
}

double* bucketing_exhaust_compute_task_exps(bucketing_state* s, struct list* bucket_list)
{
    double* task_exps = calloc(list_size(bucket_list), sizeof(*task_exps));
    bucketing_point* tmp_pnt;
    bucketing_bucket* tmp_buck;
    int i = 0;
    int total_sig_buck = 0;
    
    list_first_item(s->sorted_points);
    list_first_item(bucket_list);
    tmp_pnt = list_next_item(s->sorted_points);
    tmp_buck = list_next_item(bucket_list);

    while ((tmp_pnt))
    {
        if (tmp_pnt->val <= tmp_buck->val)
        {
            total_sig_buck += tmp_pnt->sig;
            task_exps[i] += tmp_pnt->val * tmp_pnt->sig;
            tmp_pnt = list_next_item(s->sorted_points);
        }
        else
        {
            task_exps[i] /= total_sig_buck;
            ++i;
            total_sig_buck = 0;
            tmp_buck = list_next_item(bucket_list);
        }
    }
    task_exps[i] /= total_sig_buck;

    return task_exps;
}
