#include <stdlib.h>
#include "bucketing_fast.h"
#include "bucketing.h"
#include "list.h"

int bucketing_fast_update_buckets(bucketing_state* s)
{
    if (!s)
        return 1;
    
    /* Delete old list of buckets */
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    
    /* Create new list of buckets */
    s->sorted_buckets = list_create();

    /* Find all break points */
    struct list* break_point_list = bucketing_find_break_points(s);
    if (!break_point_list)
        return 1;
    
    /* Find probabilities of buckets */
    double bucket_probs[list_size(break_point_list)];   //store probabilities of buckets
    list_first_item(s->sorted_points);
    list_first_item(break_point_list);
    bucketing_point* tmp_point;                 //pointer to item in s->sorted_points
    bucketing_cursor_w_pos* tmp_break_point;    //pointer pointing to item in break point list
    int i = 0;
    bucket_probs[0] = 0;
    double total_sig = 0;                       //track total significance

    tmp_point = list_next_item(s->sorted_points);
    tmp_break_point = list_next_item(break_point_list);
    bucketing_point* tmp_point_of_break_point = 0;
    if (!list_get(tmp_break_point->lc, (void**) &tmp_point_of_break_point))
        return 1;
    
    /* loop to compute buckets' probabilities */
    while(tmp_point)
    {

        if (tmp_point->val <= tmp_point_of_break_point->val)
        {
            bucket_probs[i] += tmp_point->sig;
            total_sig += tmp_point->sig;
            tmp_point = list_next_item(s->sorted_points);
        }
        else
        {

            ++i;
            bucket_probs[i] = 0;
            tmp_break_point = list_next_item(break_point_list);
            if (!list_get(tmp_break_point->lc, (void**) &tmp_point_of_break_point))
                return 1;
        }
    }

    /* must divide by total significance to normalize to [0, 1] */
    for (int i = 0; i < list_size(break_point_list); ++i)
    {
        bucket_probs[i] /= total_sig;    
    }

    bucketing_bucket* tmp_bucket;               //pointer to a created bucket
    bucketing_point* tmp_point_ptr = 0;         //pointer to what tmp_break_point->lc points
    list_first_item(break_point_list);          //reset to beginning of break point list
    i = 0;

    /* Loop through list of break points */
    while ((tmp_break_point = list_next_item(break_point_list)) != NULL)
    {
        if (!list_get(tmp_break_point->lc, (void**) &tmp_point_ptr))
            return 1;

        tmp_bucket = bucketing_bucket_create(tmp_point_ptr->val, bucket_probs[i]);
        if (!tmp_bucket)
            return 1;

        list_push_tail(s->sorted_buckets, tmp_bucket);
        ++i;
    }
    
    /* Delete break point list */
    if (bucketing_cursor_pos_list_clear(break_point_list, bucketing_cursor_w_pos_delete))
        return 1;

    list_delete(break_point_list);
    
    return 0;
}

struct list* bucketing_find_break_points(bucketing_state* s)
{
    if (!s)
        return 0;

    int min = 0; //min index of first bucket

    if (!s->sorted_points)
        return 0;

    int max = list_length(s->sorted_points) - 1; //max index of first bucket

    /* Create list of break points to be returned */
    struct list* break_point_list = list_create();

    /* create list and push (0, n-1) of sorted points to list of buckets */
    struct list* bucket_range_list = list_create();
    bucketing_bucket_range* init_range = bucketing_bucket_range_create(min, max, s->sorted_points);
    if (!init_range)
        return 0;

    list_push_tail(bucket_range_list, init_range);

    bucketing_bucket_range* lo_bucket_range;    //create low bucket, if possible
    bucketing_bucket_range* hi_bucket_range;    //create high bucket, if possible
    bucketing_cursor_w_pos* break_point = 0;    //store break point betweem high and low buckets
    bucketing_bucket_range* bbr_ptr = 0;        //pointer to a bucket in bucket_range_list
    
    struct list_cursor* lc = list_cursor_create(bucket_range_list);
    if (!list_seek(lc, 0))
        return 0;
    
    int breakable;

    /* Loop through all buckets and break them if broken buckets have more than 1 point */
    do
    {
        if (!list_get(lc, (void**) &bbr_ptr))
            return 0;
        
        breakable = bucketing_fast_break_bucket(bbr_ptr, &break_point);
        
        /* If bucket is breakable, break it. Else do nothing */
        if (breakable == 0)
        {
            list_push_tail(break_point_list, break_point);

            /* cannot spawn low bucket */
            if (break_point->pos == bbr_ptr->lo->pos)
            {
                /* cannot spawn high bucket */ 
                if (break_point->pos + 1 == bbr_ptr->hi->pos)
                    continue;
                else
                {
                    hi_bucket_range = bucketing_bucket_range_create(break_point->pos + 1, bbr_ptr->hi->pos, s->sorted_points);
                    if (!hi_bucket_range)
                        return 0;

                    list_push_tail(bucket_range_list, hi_bucket_range);   
                }
            }

            /* can spawn low bucket */
            else
            {
                /* can spawn high bucket */
                if (break_point->pos + 1 != bbr_ptr->hi->pos)
                {
                    hi_bucket_range = bucketing_bucket_range_create(break_point->pos + 1, bbr_ptr->hi->pos, s->sorted_points);
                    if (!hi_bucket_range)
                        return 0;

                    list_push_tail(bucket_range_list, hi_bucket_range);
                }
                lo_bucket_range = bucketing_bucket_range_create(bbr_ptr->lo->pos, break_point->pos, s->sorted_points);
                if (!lo_bucket_range)
                    return 0;

                list_push_tail(bucket_range_list, lo_bucket_range);
            } 
        }
        else if (breakable == -1)
            return 0;

    } while (list_next(lc));

    /* Push the highest point into the break point list */
    bucketing_cursor_w_pos* last_break_point = bucketing_cursor_w_pos_create(list_cursor_clone(init_range->hi->lc), init_range->hi->pos);
    if (!last_break_point)
        return 0;

    list_push_tail(break_point_list, last_break_point);

    /* Sort in increasing order */
    break_point_list = bucketing_cursor_pos_list_sort(break_point_list, compare_break_points);
    if (!break_point_list)
        return 0;
    
    /* Destroy bucket range list */
    list_cursor_destroy(lc);
    if (bucketing_bucket_range_list_clear(bucket_range_list, bucketing_bucket_range_delete))
        return 0;

    list_delete(bucket_range_list);
    
    return break_point_list;
}

int bucketing_fast_break_bucket(bucketing_bucket_range* range, bucketing_cursor_w_pos** break_point)
{
    if (!range)
        return -1;

    double min_cost = -1;   //track min cost of a candidate break point
    double cost;    //track cost of current point
    bucketing_cursor_w_pos* tmp_break_point = 0;   //get current point 

    /* Loop through all points in range and choose 1 with the lowest cost */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i)
    {
        cost = bucketing_fast_policy(range, i, &tmp_break_point);
        if (cost == -1)
            return -1;

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
    
    /* If chosen break point is the highest point, delete the break point as it is included already */
    if ((*break_point)->pos == range->hi->pos)
    {
        bucketing_cursor_w_pos_delete(*break_point); 
        return 1;
    }
    return 0;
}

double bucketing_fast_policy(bucketing_bucket_range* range, int break_index, bucketing_cursor_w_pos** break_point)
{
    if (!range)
        return -1;

    int total_sig = 0;                  //track total significance of points in range
    int total_lo_sig = 0;               //track total significance in low range
    int total_hi_sig = 0;               //track total significance in high range
    double p1 = 0;                      //probability of candidate lower bucket
    double p2 = 0;                      //probability of candidate higher bucket
    bucketing_point* tmp_point_ptr = 0; //pointer to get item from sorted points
    double exp_cons_lq_break = 0;       //expected value if next point is lower than or equal to break point
    double exp_cons_g_break = 0;        //expected value if next point is higher than break point
    int break_val, max_val; //values at break point and max point
    struct list_cursor* iter = list_cursor_clone(range->lo->lc);    //cursor to iterate through list

    /* Loop through the range to collect statistics */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i, list_next(iter))
    {
        if (!list_get(iter, (void**) &tmp_point_ptr))
            return -1;

        total_sig += tmp_point_ptr->sig;
        
        if (i == break_index)
        {
            break_val = tmp_point_ptr->val;
            *break_point = bucketing_cursor_w_pos_create(list_cursor_clone(iter), break_index);
            if (!(*break_point))
                return -1;
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
    
    list_cursor_destroy(iter);

    return cost;
}
