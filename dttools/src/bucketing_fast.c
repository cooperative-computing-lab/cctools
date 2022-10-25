#include <stdbool.h>
#include <stdio.h>
#include "bucketing_fast.h"
#include "bucketing.h"
#include "random.h"
#include "list.h"

double bucketing_fast_predict(double prev_val, bucketing_state* s)
{
    /* if in sampling phase, just return default value */
    /* TODO: fix action when in sampling phase */
    if (s->in_sampling_phase)
    {
        return s->default_value;
    }

    struct list_cursor* lc = list_cursor_create(s->sorted_buckets); //cursor to iterate
    list_seek(lc, 0);   //reset to 0
    bucketing_bucket* bb_ptr = 0;   //pointer to hold item from list
    double sum = 0; //sum of probability
    double ret_val; //predicted value to be returned
    double rand = random_double();  //random double to choose a bucket

    /* Loop through list of buckets to choose 1 */
    for (unsigned int i = 0; i < list_length(s->sorted_buckets); ++i, list_next(lc))
    {    
        list_get(lc, (void**) &bb_ptr);
       
        /* return if at last bucket */
        if (i == list_length(s->sorted_buckets) - 1)
        {
            ret_val = bb_ptr->val;
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

    return -1;  //control should never reach here
}

int bucketing_fast_update_buckets(bucketing_state* s)
{
    /* Delete old list of buckets */
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    
    /* Create new list of buckets */
    s->sorted_buckets = list_create();

    /* Find all break points */
    struct list* break_point_list = bucketing_find_break_points(s);
    
    bucketing_cursor_w_pos* tmp_break_point;    //pointer pointing to item in break point list
    bucketing_bucket* tmp_bucket;   //pointer to a created bucket
    bucketing_point* tmp_point_ptr = 0; //pointer to what tmp_break_point->lc points
    int prev_pos = 0;   //previous position to find probability of buckets
    list_first_item(break_point_list);  //reset to beginning of break point list

    /* Loop through list of break points */
    while ((tmp_break_point = list_next_item(break_point_list)) != NULL)
    {
        list_get(tmp_break_point->lc, (void**) &tmp_point_ptr);
        tmp_bucket = bucketing_bucket_create(tmp_point_ptr->val, (tmp_break_point->pos - prev_pos + 1)/list_length(s->sorted_points));
        prev_pos = tmp_break_point->pos;
        list_push_tail(s->sorted_buckets, tmp_bucket);
    }
   
    /* Delete break point list */
    list_clear(break_point_list, (void*) bucketing_cursor_w_pos_delete);
    list_delete(break_point_list);
    return 0;
}

struct list* bucketing_find_break_points(bucketing_state* s)
{
    int min = 0; //min index of first bucket
    int max = list_length(s->sorted_points) - 1; //max index of first bucket

    /* Create list of break points to be returned */
    struct list* break_point_list = list_create();

    /* create list and push (0, n-1) of sorted points to list of buckets */
    struct list* bucket_range_list = list_create();
    bucketing_bucket_range* init_range = bucketing_bucket_range_create(min, max, s->sorted_points);
    list_push_tail(bucket_range_list, init_range);

    bucketing_bucket_range* lo_bucket_range;    //create low bucket, if possible
    bucketing_bucket_range* hi_bucket_range;    //create high bucket, if possible
    bucketing_cursor_w_pos* break_point = 0;    //store break point of high and low buckets
    bucketing_bucket_range* bbr_ptr = 0;        //pointer to a bucket in bucket_range_list
    
    struct list_cursor* lc = list_cursor_create(bucket_range_list);
    list_seek(lc, 0);
    
    /* Loop through all buckets and break them if broken buckets have more than 1 point */
    do
    {
        list_get(lc, (void**) &bbr_ptr);
        printf("Range (%d, %d)\n", bbr_ptr->lo->pos, bbr_ptr->hi->pos);
        if (bucketing_fast_break_bucket(bbr_ptr, &break_point) == 0)
        {
            printf("Break point chosen: %d\n", break_point->pos);
            list_push_tail(break_point_list, break_point);
            if (bbr_ptr->lo->pos != break_point->pos)
            {
                lo_bucket_range = bucketing_bucket_range_create(bbr_ptr->lo->pos, break_point->pos, s->sorted_points);
                list_push_tail(bucket_range_list, lo_bucket_range);
            }
            if (break_point->pos + 1 != bbr_ptr->hi->pos)
            {
                hi_bucket_range = bucketing_bucket_range_create(break_point->pos + 1, bbr_ptr->hi->pos, s->sorted_points);
                list_push_tail(bucket_range_list, hi_bucket_range);
            }
        }
        printf("------------------\n");
    } while (list_next(lc));

    /* Push the highest point into the break point list */
    bucketing_cursor_w_pos* last_break_point = bucketing_cursor_w_pos_create(list_cursor_clone(init_range->hi->lc), init_range->hi->pos);
    list_push_tail(break_point_list, last_break_point);

    /* Sort in increasing order */
    list_sort(break_point_list, (void*) compare_break_points);

    /* Destroy bucket range list */
    list_clear(bucket_range_list, (void*) bucketing_bucket_range_delete); 
    list_cursor_destroy(lc);
    list_free(bucket_range_list);
    
    return break_point_list;
}

int compare_break_points(bucketing_cursor_w_pos* p1, bucketing_cursor_w_pos* p2)
{
    return p1->pos - p2->pos;
}

int bucketing_fast_break_bucket(bucketing_bucket_range* range, bucketing_cursor_w_pos** break_point)
{
    double min_cost = -1;   //track min cost of a candidate break point
    double cost;    //track cost of current point
    bucketing_cursor_w_pos* tmp_break_point = 0;   //get current point 

    /* Loop through all points in range and choose 1 with the lowest cost */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i)
    {
        cost = bucketing_fast_policy(range, i, &tmp_break_point);
        if (min_cost == -1)
        {
            min_cost = cost;
            *break_point = tmp_break_point;
        }
        else if (cost < min_cost)
        {
            min_cost = cost;
            bucketing_cursor_w_pos_delete(*break_point);
            *break_point = tmp_break_point;
        }
        else 
        {
            bucketing_cursor_w_pos_delete(tmp_break_point);
        }
    }
    
    /* If chosen break point is the highest point, don't do anything as it is included already */
    if ((*break_point)->pos == range->hi->pos)
        return 1;
    return 0;
}

double bucketing_fast_policy(bucketing_bucket_range* range, int break_index, bucketing_cursor_w_pos** break_point)
{
    int total_sig = 0;  //track total significance of points in range
    int total_lo_sig = 0;   //track total significance in low range
    int total_hi_sig = 0;   //track total significance in high range
    double p1 = 0;  //probability of candidate lower bucket
    double p2 = 0;  //probability of candidate higher bucket
    bucketing_point* tmp_point_ptr = 0; //pointer to get item from sorted points
    double exp_cons_lq_break = 0;   //expected value if next point is lower than or equal to break point
    double exp_cons_g_break = 0;    //expected value if next point is higher than break point
    int break_val, max_val; //values at break point and max point
    struct list_cursor* iter = list_cursor_clone(range->lo->lc);    //cursor to iterate through list

    /* Loop through the range to collect statistics */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i, list_next(iter))
    {
        list_get(iter, (void**) &tmp_point_ptr);
        total_sig += tmp_point_ptr->sig;
        
        if (i == break_index)
        {
            break_val = tmp_point_ptr->val;
            *break_point = bucketing_cursor_w_pos_create(list_cursor_clone(iter), break_index);
        }
        
        if (i == range->hi->pos)
            max_val = tmp_point_ptr->val;
        
        if (i <= break_index)
        {
            p1 += tmp_point_ptr->sig;
            exp_cons_lq_break += tmp_point_ptr->val * tmp_point_ptr->sig;
            total_lo_sig += tmp_point_ptr->sig;
        }
        else
        {
            p2 += tmp_point_ptr->sig;
            exp_cons_g_break += tmp_point_ptr->val * tmp_point_ptr->sig;
            total_hi_sig += tmp_point_ptr->sig;
        }
    }

    /* Update general statistics */
    p1 /= total_sig;
    p2 /= total_sig; 
    exp_cons_lq_break /= total_lo_sig;
    if (total_hi_sig == 0)
        exp_cons_g_break = 0;
    else
        exp_cons_g_break /= total_hi_sig;

    /* Compute individual costs */
    double cost_lower_hit = p1*(p1*(break_val - exp_cons_lq_break));
    double cost_lower_miss = p1*(p2*(max_val - exp_cons_lq_break));
    double cost_upper_miss = p2*(p1*(break_val + max_val - exp_cons_g_break));
    double cost_upper_hit = p2*(p2*(max_val - exp_cons_g_break));

    /* Compute final cost */
    double cost = cost_lower_hit + cost_lower_miss + cost_upper_miss + cost_upper_hit;
    printf("break point %d with cost %lf\n", break_index, cost);    
    list_cursor_destroy(iter);

    return cost;
}
