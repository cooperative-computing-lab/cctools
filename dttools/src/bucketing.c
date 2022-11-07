#include <stdlib.h>
#include <math.h>
#include <stdio.h>
#include "bucketing.h"
#include "random.h"
#include "xxmalloc.h"
#include "debug.h"

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
    if (p)
        free(p);
}

/* Insert a bucketing point into a sorted list of points in O(log(n))
 * @param l pointer to sorted list of points
 * @param p pointer to point */
static void bucketing_insert_point_to_sorted_list(struct list* l, bucketing_point_t *p)
{
    struct list_cursor* lc = list_cursor_create(l);
    
    /* If list is empty, append new point to list */
    if (list_length(l) == 0)
    {
        list_insert(lc, p);
        list_cursor_destroy(lc);
        return;
    }

    /* Linear insert a data point */
    list_seek(lc, 0);
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

/** End: internals **/

/** Begin: APIs **/

bucketing_state_t* bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, int max_num_buckets)
{
    bucketing_state_t* s = xxmalloc(sizeof(*s));

    s->sorted_points = list_create();
    s->sequence_points = list_create();
    s->sorted_buckets = list_create();
    s->num_points = 0;
    s->in_sampling_phase = 1;
    s->prev_op = BUCKETING_OP_NULL;
    s->default_value = default_value;
    s->num_sampling_points = num_sampling_points;
    s->increase_rate = increase_rate;
    s->max_num_buckets = max_num_buckets;

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

void bucketing_add(double val, double sig, bucketing_state_t* s)
{
    /* insert to sorted list and append to sequence list */
    bucketing_point_t *p = bucketing_point_create(val, sig);
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
    s->num_points++;
    if (s->num_points >= s->num_sampling_points)
    {
        s->in_sampling_phase = 0;
    }

    /* set previous operation */
    s->prev_op = BUCKETING_OP_ADD;
}

double bucketing_predict(double prev_val, bucketing_state_t* s)
{
    /* set previous operation */
    s->prev_op = BUCKETING_OP_PREDICT;
    
    /* in sampling phase */
    if (s->in_sampling_phase)
    {
        /* if new, return default value */
        if (prev_val == -1)
            return s->default_value;
        
        /* otherwise increase to exponent level */
        else
        {
            int exp = floor(log(prev_val/s->default_value)/log(s->increase_rate)) + 1;
            return s->default_value * pow(s->increase_rate, exp);
        } 
    }

    struct list_cursor* lc = list_cursor_create(s->sorted_buckets); //cursor to iterate
    
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

        sum += bb_ptr->prob;

        if (sum > rand)
        {
            ret_val = bb_ptr->val;
            list_cursor_destroy(lc);
            return ret_val;
        }
    }
    
    fatal("Control should never reach here\n");
    return -1; 
}

bucketing_bucket_t* bucketing_bucket_create(double val, double prob)
{
    bucketing_bucket_t* b = xxmalloc(sizeof(*b));

    b->val = val;
    b->prob = prob;

    return b;
}

/* Delete a bucketing bucket
 * @param b the bucket to be deleted */
void bucketing_bucket_delete(bucketing_bucket_t* b)
{
    if (b)
        free(b);
}

/** End: APIs **/

/** Begin: debug functions **/

void bucketing_sorted_buckets_print(struct list* l)
{
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
