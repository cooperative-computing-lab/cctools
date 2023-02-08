#include <stdlib.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include "bucketing.h"
#include "random.h"
#include "xxmalloc.h"
#include "debug.h"
#include "bucketing_greedy.h"
#include "bucketing_exhaust.h"

/** Begin: internals **/

/* Create a bucketing point
 * @param val value of point
 * @param sig significance of point
 * @return pointer to created point
 * @return NULL if failure */
static bucketing_point_t* bucketing_point_create(double val, double sig)
{
    bucketing_point_t* p = xxmalloc(sizeof(*p));
    
    p->val = val;
    p->sig = sig;

    return p;
}

/* Delete a bucketing point
 * @param p the bucketing point to be deleted */
static void bucketing_point_delete(bucketing_point_t *p)
{
    free(p);
}

/* Insert a bucketing point into a sorted list of points in O(log(n))
 * @param l pointer to sorted list of points
 * @param p pointer to point */
static void bucketing_insert_point_to_sorted_list(struct list* l, bucketing_point_t *p)
{
    struct list_cursor* lc = list_cursor_create(l);
    if (!lc)
    {
        fatal("Cannot create list cursor\n");
        return;
    }
    
    /* If list is empty, append new point to list */
    if (list_length(l) == 0)
    {
        list_insert(lc, p);
        list_cursor_destroy(lc);
        return;
    }

    /* Linear insert a data point */
    if (!list_seek(lc, 0))
    {
        fatal("Cannot seek list to index 0\n");
        return;
    }

    bucketing_point_t* bpp = 0;
    int inserted = 0;
    do
    {
        if (!list_get(lc, (void**) &bpp))
        {
            fatal("Cannot get element from list.\n");
            return;
        }

        if (bpp->val >= p->val)
        {
            list_insert(lc, p);
            inserted = 1;
            break;
        }
    }
    while (list_next(lc));

    /* Append point if it isn't inserted */
    if (inserted == 0)
    {
        list_insert(lc, p);
    }

    list_cursor_destroy(lc);
}

static void generate_next_task_sig(bucketing_state_t* s)
{
    ++s->next_task_sig;
}

static int bucketing_ready_to_update_buckets(bucketing_state_t* s)
{
    int num_points_since_predict_phase = s->num_points - s->num_sampling_points;
    
    /* Update when in predict phase and bucketing state is at the update epoch */
    if (!s->in_sampling_phase && 
        num_points_since_predict_phase % s->update_epoch == 0)
    {
        return 1;
    }
    return 0;
}

static void bucketing_update_buckets(bucketing_state_t* s)
{
    switch (s->mode)
    {
        case BUCKETING_MODE_GREEDY:
            bucketing_greedy_update_buckets(s);
            break;
        case BUCKETING_MODE_EXHAUSTIVE:
            bucketing_exhaust_update_buckets(s);
            break;
        default:
            fatal("Invalid mode to update buckets\n");
    }
}

/** End: internals **/

/** Begin: APIs **/

bucketing_bucket_t* bucketing_bucket_create(double val, double prob)
{
    if (val < 0)
        warn(D_BUCKETING, "bucket value cannot be less than 0\n");

    bucketing_bucket_t* b = xxmalloc(sizeof(*b));

    b->val = val;
    b->prob = prob;

    return b;
}

void bucketing_bucket_delete(bucketing_bucket_t* b)
{
    free(b);
}

bucketing_state_t* bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, int max_num_buckets, bucketing_mode_t mode, int update_epoch)
{
    if (default_value < 0)
    {
        warn(D_BUCKETING, "default value cannot be less than 0\n");
        default_value = 1;
    }

    if (num_sampling_points < 1)
    {
        warn(D_BUCKETING, "number of sampling points cannot be less than 1\n");
        num_sampling_points = 1;
    }

    if (increase_rate <= 1)
    {
        warn(D_BUCKETING, "increase rate must be greater than 1 to be meaningful\n");
        increase_rate = 2;
    }

    if (max_num_buckets < 1 && mode == BUCKETING_MODE_EXHAUSTIVE)
    {
        warn(D_BUCKETING, "The maximum number of buckets for exhaustive bucketing must be at least 1\n");
        max_num_buckets = 1;
    }
    
    if (mode != BUCKETING_MODE_GREEDY && mode != BUCKETING_MODE_EXHAUSTIVE)
    {
        warn(D_BUCKETING, "Invalid bucketing mode\n");
        mode = BUCKETING_MODE_GREEDY;
    }

    if (update_epoch < 1)
    {
        warn(D_BUCKETING, "Update epoch for bucketing cannot be less than 1\n");
        update_epoch = 1;
    }

    bucketing_state_t* s = xxmalloc(sizeof(*s));

    s->sorted_points = list_create();
    s->sequence_points = list_create();
    s->sorted_buckets = list_create();
    
    s->num_points = 0;
    s->in_sampling_phase = 1;
    s->prev_op = BUCKETING_OP_NULL;
    s->next_task_sig = 1;
    
    s->default_value = default_value;
    s->num_sampling_points = num_sampling_points;
    s->increase_rate = increase_rate;
    s->max_num_buckets = max_num_buckets;
    s->mode = mode;
    s->update_epoch = update_epoch;

    return s;
}

void bucketing_state_delete(bucketing_state_t* s)
{
    if (s)
    {
        list_clear(s->sorted_points, (void*) bucketing_point_delete);
        list_delete(s->sorted_points);
        
        /* pointers already free'd as sorted_points and sequence_points
         * share the same set of pointers */
        list_delete(s->sequence_points);
        list_clear(s->sorted_buckets, (void*) bucketing_bucket_delete);
        list_delete(s->sorted_buckets);
        free(s);
    }
}

void bucketing_state_tune(bucketing_state_t* s, const char* field, void* val)
{
    if (!s) {
        fatal("No bucketing state to tune\n");
        return;
    }

    if (!field) {
        fatal("No field in bucketing state to tune\n");
        return;
    }

    if (!val) {
        fatal("No value to tune field %s in bucketing state to\n", field);
        return;
    }

    if (!strncmp(field, "default_value", strlen("default_value"))) {
        s->default_value = *((double*) val);
    }
    else if (!strncmp(field, "num_sampling_points", strlen("num_sampling_points"))) {
        s->num_sampling_points = *((int*) val);
    }
    else if (!strncmp(field, "increase_rate", strlen("increase_rate"))) {
        s->increase_rate = *((double*) val);
    }
    else if (!strncmp(field, "max_num_buckets", strlen("max_num_buckets"))) {
        s->num_sampling_points = *((int*) val);
    }
    else if (!strncmp(field, "mode", strlen("mode"))) {
        s->num_sampling_points = *((bucketing_mode_t*) val);
    }
    else if (!strncmp(field, "update_epoch", strlen("update_epoch"))) {
        s->update_epoch = *((int*) val);
    }
    else {
        warn(D_BUCKETING, "Cannot tune field %s as it doesn't exist\n", field);
    }
}

void bucketing_add(bucketing_state_t* s, double val)
{
    /* insert to sorted list and append to sequence list */
    bucketing_point_t *p = bucketing_point_create(val, s->next_task_sig);
    if (!p)
    {
        fatal("Cannot create point\n");
        return;
    }

    bucketing_insert_point_to_sorted_list(s->sorted_points, p);

    if (!list_push_tail(s->sequence_points, p))
    {
        fatal("Cannot push point to list tail\n");
        return;
    }
    
    /* Change to predicting phase if appropriate */
    ++s->num_points;
    if (s->num_points >= s->num_sampling_points)
    {
        s->in_sampling_phase = 0;
    }

    /* set previous operation */
    s->prev_op = BUCKETING_OP_ADD;
    
    /* increment to next task significance value */
    generate_next_task_sig(s);

    /* check if it is condition to bucket or not */
    if (bucketing_ready_to_update_buckets(s))
        bucketing_update_buckets(s);
}

double bucketing_predict(bucketing_state_t* s, double prev_val)
{
    /* set previous operation */
    s->prev_op = BUCKETING_OP_PREDICT;
    
    /* in sampling phase */
    if (s->in_sampling_phase)
    {
        /* if new or empty resource, return default value */
        if (prev_val == -1 || prev_val == 0)
        {
            return s->default_value;
        }

        /* prevous value must be -1 or greater than 0 */
        else if (prev_val != -1 && prev_val < 0)
        {
            fatal("invalid previous value to predict\n");
            return -1;
        }
        
        /* otherwise increase to exponent level */
        else
        {
            int exp = floor(log(prev_val/s->default_value)/log(s->increase_rate)) + 1;
            return s->default_value * pow(s->increase_rate, exp);
        } 
    }

    struct list_cursor* lc = list_cursor_create(s->sorted_buckets); //cursor to iterate
    if (!lc)
    {
        fatal("Cannot create list cursor\n");
        return -1;
    }
    
    /* reset to 0 */
    if (!list_seek(lc, 0))
    {
        fatal("Cannot seek list\n");
        return -1;
    }

    bucketing_bucket_t* bb_ptr = 0;   //pointer to hold item from list
    double sum = 0;                 //sum of probability
    double ret_val;                 //predicted value to be returned
    int exp;                        //exponent to raise if prev_val > max_val
    double rand = random_double();  //random double to choose a bucket
    double total_net_prob = 1;      //total considered probability

    /* Loop through list of buckets to choose 1 */
    for (unsigned int i = 0; i < list_length(s->sorted_buckets); ++i, list_next(lc))
    {
        if (!list_get(lc, (void**) &bb_ptr))
        {
            fatal("Cannot get item from list\n");
            return -1;
        }

        /* return if at last bucket */
        if (i == list_length(s->sorted_buckets) - 1)
        {
            ret_val = bb_ptr->val;
            
            if (ret_val <= prev_val)
            {
                exp = floor(log(prev_val/s->default_value)/log(s->increase_rate)) + 1;
                list_cursor_destroy(lc);
                return s->default_value * pow(s->increase_rate, exp);   
            }
            
            list_cursor_destroy(lc);
            return ret_val;
        }
       
        /* skip the small buckets */
        if (bb_ptr->prob <= prev_val)
        {
            total_net_prob -= bb_ptr->prob;
            continue;
        }

        sum += bb_ptr->prob;

        if (sum / total_net_prob > rand) //rescale sum to [0, 1] as we skip small buckets
        {
            ret_val = bb_ptr->val;

            list_cursor_destroy(lc);
            return ret_val;
        }
    }
    
    fatal("Control should never reach here\n");
    return -1; 
}

/** End: APIs **/

/** Begin: debug functions **/

void bucketing_sorted_buckets_print(struct list* l)
{
    if (!l)
        return;
    bucketing_bucket_t *tmp;
    list_first_item(l);
    printf("Printing sorted buckets\n");
    int i = 0;
    while((tmp = list_next_item(l)))
    {
        printf("bucket pos: %d, value: %lf, prob: %lf\n", i, tmp->val, tmp->prob);
        ++i;
    }
}

void bucketing_sorted_points_print(struct list* l)
{
    if (!l)
        return;
    bucketing_point_t* tmp;
    list_first_item(l);
    printf("Printing sorted points\n");
    int i = 0;
    while((tmp = list_next_item(l)))
    {
        printf("pos: %d, value: %lf, sig: %lf\n", i, tmp->val, tmp->sig);
        ++i;
    }
}

/** End: debug functions **/
