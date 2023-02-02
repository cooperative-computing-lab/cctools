#include <stdlib.h>
#include <math.h>
#include "bucketing_exhaust.h"
#include "list.h"
#include "xxmalloc.h"
#include "debug.h"

/** Begin: internals **/

/* Convert a list of bucketing_bucket_t to an array of those
 * @param bucket_list list of bucketing_bucket_t
 * @return pointer to array of bucketing_bucket_t
 * @return 0 if failure */
static bucketing_bucket_t** bucketing_bucket_list_to_array(struct list* bucket_list)
{
    if (!bucket_list)
    {
        fatal("No bucket list\n");
        return 0;
    }

    list_first_item(bucket_list);
    bucketing_bucket_t* tmp_buck;
    bucketing_bucket_t** bucket_array = xxmalloc(list_size(bucket_list) * sizeof(*bucket_array));
    
    int i = 0;
    while ((tmp_buck = list_next_item(bucket_list)))
    {
        bucket_array[i] = tmp_buck;
        ++i;
    }

    return bucket_array;
}

/* Reweight the probabilities of a range of buckets to 1
 * @param bucket_array the array of bucketing_bucket_t*
 * @param lo index of low bucket
 * @param hi index of high bucket
 * @return array of reweighted probabilities
 * @return 0 if failure */
static double* bucketing_reweight_bucket_probs(bucketing_bucket_t** bucket_array, int lo, int hi)
{
    if (!bucket_array)
    {
        fatal("No array of buckets\n");
        return 0;
    }

    double* bucket_probs = xxmalloc((hi - lo + 1) * sizeof(*bucket_probs));

    /* get all probabilities of buckets in range */
    double total_prob = 0;
    for (int i = lo; i <= hi; ++i)
    {
        total_prob += bucket_array[i]->prob;
    }

    /* reweight to [0, 1] */
    for (int i = lo; i <= hi; ++i)
    {
        bucket_probs[i - lo] = bucket_array[i]->prob / total_prob;
    }

    return bucket_probs;
}

/* Compute the expectations of tasks' values in all buckets
 * @param s the relevant bucketing state
 * @param bucket_list the list of buckets
 * @return pointer to a malloc'ed array of values
 * @return 0 if failure */
static double* bucketing_exhaust_compute_task_exps(bucketing_state_t* s, struct list* bucket_list)
{
    if (!s || !bucket_list)
    {
        fatal("At least one parameter is empty\n");
        return 0;
    }

    double* task_exps = xxcalloc(list_size(bucket_list), sizeof(*task_exps));

    bucketing_point_t* tmp_pnt;
    bucketing_bucket_t* tmp_buck;
    int i = 0;
    double total_sig_buck = 0;
    
    list_first_item(s->sorted_points);
    list_first_item(bucket_list);
    tmp_pnt = list_next_item(s->sorted_points);
    tmp_buck = list_next_item(bucket_list);

    /* Loop though all points to compute expected value of task if it is in a given bucket */
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

    /* Update last task expectation of last bucket */
    task_exps[i] /= total_sig_buck;

    return task_exps;
}

/* Compute cost of a list of buckets using the relevant bucketing state
 * @param s the relevant bucketing state
 * @param bucket_list the list of buckets to be computed
 * @return expected cost of the list of buckets 
 * @return -1 if failure */
static double bucketing_exhaust_compute_cost(bucketing_state_t* s, struct list* bucket_list)
{
    if (!s || !bucket_list)
    {
        fatal("At least one parameter is empty\n");
        return -1;
    }

    int N = list_size(bucket_list);
    double cost_table[N][N];

    /* Compute task expectation in each bucket */
    double* task_exps = bucketing_exhaust_compute_task_exps(s, bucket_list);
    if (!task_exps)
    {
        fatal("Cannot compute task expectations\n");
        return -1;
    }

    bucketing_bucket_t** bucket_array = bucketing_bucket_list_to_array(bucket_list);
    if (!bucket_array)
    {
        fatal("Cannot convert list of buckets to array of buckets\n");
        return -1;
    }

    /* i is task in which bucket, j is which bucket is chosen */
    /* fill easy entries */
    for (int j = 0; j < N; ++j)
    {
        for (int i = 0; i <= j; ++i)
        {
            cost_table[i][j] = bucket_array[j]->val - task_exps[i];
        }
    }

    double* upper_bucket_probs;
    /* fill entries that depend on other entries */
    for (int i = N - 1; i > -1; --i) 
    {
        for (int j = i - 1; j > -1; --j)
        {
            cost_table[i][j] = bucket_array[j]->val;
            upper_bucket_probs = bucketing_reweight_bucket_probs(bucket_array, j + 1, N - 1);
            if (!upper_bucket_probs)
            {
                fatal("Cannot reweight buckets\n");
                return -1;
            }
            
            for (int k = j + 1; k < N; ++k)
            {
                cost_table[i][j] += upper_bucket_probs[k - (j + 1)] * cost_table[i][k];
            }
            free(upper_bucket_probs);
        }
    }

    /* Compute final cost */
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

/* Get the list of buckets from a list of points and the number of buckets
 * @param s the relevant bucketing state
 * @param n the number of buckets to get
 * @return a list of bucketing_bucket_t
 * @return 0 if failure */
static struct list* bucketing_exhaust_get_buckets(bucketing_state_t* s, int n)
{
    if (!s)
    {
        fatal("No state of compute buckets\n");
        return 0;
    }

    if (!s->sorted_points)
    {
        fatal("sorted list of points is empty\n");
        return 0;
    }

    bucketing_point_t* tmp_point = list_peek_tail(s->sorted_points);
    if (!tmp_point)
    {
        fatal("list of points is empty so can't get a list of buckets\n");
        return 0;
    }

    double max_val = tmp_point->val;    //max value in all points
   
    int steps;
    if (max_val == 0)   //corner case where max value is 0, so no possible steps are available
        steps = 0;
    else
        steps = floor(log(max_val / n) / log(2));   //logarithmic steps to take below max_val/n

    /* No steps if steps is negative */
    if (steps < 0)
        steps = 0;

    double candidate_vals[steps + n];

    /* fill candidate values with logarithmic increase */
    for (int i = 0; i < steps; ++i) {
        candidate_vals[i] = pow(2, i);
    }
    /* fill candidate values with linear increase */
    for (int i = 0; i < n; ++i) 
    {
        if (i == (n-1))
        {
            candidate_vals[i + steps] = max_val;
        }
        else
        {
            candidate_vals[i + steps] = max_val * (i + 1) / (n * 1.0);
        }
    }
        
    double buck_sig = 0;    //track signficance of a bucket
    double total_sig = 0;   //track total significance
    int i = 0;              //track index to candidate buckets
    double prev_val = 0;    //previous seen value of point
    double candidate_probs[steps + n];  //probabilities of candidate buckets
    for (i = 0; i < steps + n; ++i)
        candidate_probs[i] = 0;

    i = 0;
    list_first_item(s->sorted_points);
    bucketing_point_t* tmp = list_next_item(s->sorted_points);

    /* loop through points to fill values for buckets */
    while ((tmp) && (i < steps + n))
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

    /* update last bucket */
    candidate_probs[i] = buck_sig;
    total_sig += buck_sig;

    struct list* ret = list_create();
   
    bucketing_bucket_t* tmp_bucket;

    /* push a bucket in sorted order if bucket is not empty */
    for (i = 0; i < steps + n; ++i)
    {
        if (candidate_probs[i] != 0)
        {
            tmp_bucket = bucketing_bucket_create(candidate_vals[i], candidate_probs[i] / total_sig);
            if (!tmp_bucket)
            {
                fatal("Cannot create bucket\n");
                return 0;
            }

            list_push_tail(ret, tmp_bucket);
        }
    }

    return ret;
}

/* Return list of buckets that have the lowest expected cost
 * @param s the relevant bucketing state
 * @return a list of bucketing_bucket_t
 * @return 0 if failure */
static struct list* bucketing_exhaust_get_min_cost_bucket_list(bucketing_state_t* s)
{
    if (!s)
    {
        fatal("No bucket state to get min cost bucket list\n");
        return 0;
    }

    double cost = 0;
    double min_cost = -1;
    struct list* best_bucket_list = 0;
    struct list* bucket_list = 0;

    /* try to see which number of buckets yields the lowest cost */
    for (int i = 0; i < s->max_num_buckets; ++i)
    {
        /* get list of buckets */
        bucket_list = bucketing_exhaust_get_buckets(s, i + 1);
        if (!bucket_list)
        {
            fatal("Cannot compute buckets\n");
            return 0;
        }

        /* compute cost associated with bucket_list */
        cost = bucketing_exhaust_compute_cost(s, bucket_list);
        if (cost == -1)
        {
            fatal("Cannot compute cost of bucket list\n");
            return 0;
        }
        
        /* get list with lowest cost */
        if (min_cost == -1 || min_cost > cost)
        {
            min_cost = cost;

            /* delete old best list if there's better list */
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

/** End: internals **/

/** Begin: APIs **/

void bucketing_exhaust_update_buckets(bucketing_state_t *s)
{
    if (!s)
    {
        fatal("No bucket state to update buckets\n");
        return;
    }

    /* Destroy old list */
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);

    /* Update with new list */
    s->sorted_buckets = bucketing_exhaust_get_min_cost_bucket_list(s);
    if (!(s->sorted_buckets))
    {
        fatal("Problem updating new sorted list of buckets\n");
        return;
    }
}

/** End: APIs **/
