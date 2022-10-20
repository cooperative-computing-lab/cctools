#include <stdbool.h>
#include "bucketing_fast.h"
#include "bucketing.h"
#include "random.h"
#include "list.h"

double bucketing_fast_predict(double prev_val, bucketing_state* s)
{
    if (s->in_sampling_phase)
    {
        return s->default_value;
    }
    double rand = random_double();
    struct list_cursor* lc = list_cursor_create(s->sorted_buckets);
    list_seek(lc, 0);
    double sum = 0;
    bucketing_bucket** bb_ptr;
    for (int i = 0; i < list_length(s->sorted_buckets); ++i, list_next(lc))
    {
        
        list_get(lc, (void**) bb_ptr);
        if (i == list_length(s->sorted_buckets) - 1)
        {
            list_cursor_destroy(lc);
            return (*bb_ptr)->val;
        }
        sum += (*bb_ptr)->prob;
        if (sum > rand)
        {
            list_cursor_destroy(lc);
            return (*bb_ptr)->val;
        }
    }
    return -1;  //control should never reach here
}

int bucketing_fast_update_buckets(bucketing_state* s)
{
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    
    struct list* break_point_list;
    bucketing_find_break_points(s, break_point_list);
    
    s->sorted_buckets = list_create();
    list_first_item(break_point_list);
    bucketing_cursor_w_pos* tmp_break_point;
    bucketing_bucket* tmp_bucket;
    bucketing_point** tmp_point_ptr;
    int total = 0;
    int prev_pos = 0;
    while (tmp_break_point = list_next_item(break_point_list))
    {
        list_get(tmp_break_point->lc, (void**) tmp_point_ptr);
        bucketing_bucket_create((*tmp_point_ptr)->val, (tmp_break_point->pos - prev_pos + 1)/list_length(s->sorted_points), tmp_bucket);
        prev_pos = tmp_break_point->pos;
        list_push_tail(s->sorted_buckets, tmp_bucket);
    }
    list_clear(break_point_list, bucketing_cursor_w_pos_delete);
    list_free(break_point_list);
    return 0;
}

int bucketing_find_break_points(bucketing_state* s, struct list* break_point_list)
{
    int min = 0;
    int max = list_length(s->sorted_points) - 1;
    break_point_list = list_create();

    /* create list and push (0, n-1) to list */
    struct list* bucket_range_list = list_create();
    bucketing_bucket_range* init_range;
    bucketing_bucket_range_create(min, max, s->sorted_points, init_range);
    list_push_tail(bucket_range_list, init_range);

    bucketing_cursor_w_pos* break_point;
    bucketing_bucket_range** bbr_ptr;
    bucketing_bucket_range* lo_bucket_range;
    bucketing_bucket_range* hi_bucket_range;
    struct list_cursor* lc = list_cursor_create(bucket_range_list);
    list_seek(lc, 0);
    
    do
    {
        list_get(lc, (void**) bbr_ptr);
        if (bucketing_fast_break_bucket(*(bbr_ptr), break_point) == 0)
        {
            list_push_tail(break_point_list, break_point);
            if ((*bbr_ptr)->lo->pos != break_point->pos)
            {
                bucketing_bucket_range_create((*bbr_ptr)->lo->pos, break_point->pos, 
                    s->sorted_points, lo_bucket_range);
                list_push_tail(bucket_range_list, lo_bucket_range);
            }
            if (break_point->pos + 1 != (*bbr_ptr)->hi->pos)
            {
                bucketing_bucket_range_create(break_point->pos + 1, (*bbr_ptr)->hi->pos, 
                    s->sorted_points, hi_bucket_range);
                list_push_tail(bucket_range_list, hi_bucket_range);
            }
        }
    } while (list_next(lc));
    bucketing_cursor_w_pos* last_break_point;
    bucketing_cursor_w_pos_create(init_range->hi->lc, init_range->hi->pos, last_break_point);
    list_push_tail(break_point_list, last_break_point);

    list_sort(break_point_list, compare_break_points);

    list_clear(bucket_range_list, bucketing_bucket_range_delete); 
    list_free(bucket_range_list);
    list_cursor_destroy(lc);
    return 0;
}

int compare_break_points(bucketing_cursor_w_pos* p1, bucketing_cursor_w_pos* p2)
{
    return p1->pos - p2->pos;
}

int bucketing_fast_break_bucket(bucketing_bucket_range* range, bucketing_cursor_w_pos* break_point)
{
    double min_cost = -1;
    double cost;
    bucketing_cursor_w_pos* tmp_break_point;
    for (int i = range->lo->pos; i <= range->hi->pos; ++i)
    {
        cost = bucketing_fast_policy(range, i, tmp_break_point);
        if (min_cost == -1)
        {
            min_cost = cost;
            break_point = tmp_break_point;
        }
        else if (cost < min_cost)
        {
            min_cost = cost;
            bucketing_cursor_w_pos_delete(break_point);
            break_point = tmp_break_point;
        }
        else 
        {
            bucketing_cursor_w_pos_delete(tmp_break_point);
        }
    }
    return 0;
}

double bucketing_fast_policy(bucketing_bucket_range* range, int break_index, bucketing_cursor_w_pos* break_point)
{
    bucketing_point** tmp_point_ptr;
    int total_sig = 0;
    int break_val, max_val;
    double p1 = 0;
    double p2 = 0;
    struct list_cursor* iter = list_cursor_clone(range->lo->lc);
   
    double exp_cons_lq_break, exp_cons_g_break;

    for (int i = range->lo->pos; i <= range->hi->pos; ++i, list_next(iter))
    {
        list_get(iter, (void**) tmp_point_ptr);
        total_sig += (*tmp_point_ptr)->sig;
        if (i == break_index)
        {
            break_val = (*tmp_point_ptr)->val;
            bucketing_cursor_w_pos_create(list_cursor_clone(iter), break_index, break_point);
        }
        if (i == range->hi->pos)
            max_val = (*tmp_point_ptr)->val;
        if (i <= break_index)
        {
            p1 += (*tmp_point_ptr)->sig;
            exp_cons_lq_break += (*tmp_point_ptr)->val * (*tmp_point_ptr)->sig;
        }
        else
        {
            p2 += (*tmp_point_ptr)->sig;
            exp_cons_g_break += (*tmp_point_ptr)->val * (*tmp_point_ptr)->sig;
        }
    }

    p1 /= total_sig;
    p2 /= total_sig;
    
    exp_cons_lq_break /= total_sig;
    exp_cons_g_break /= total_sig;

    double cost_lower_hit = p1*(p1*(break_val - exp_cons_lq_break));
    double cost_lower_miss = p1*(p2*(max_val - exp_cons_lq_break));
    double cost_upper_miss = p2*(p1*(break_val + max_val - exp_cons_g_break));
    double cost_upper_hit = p2*(p2*(max_val - exp_cons_g_break));

    double cost = cost_lower_hit + cost_lower_miss + cost_upper_miss + cost_upper_hit;

    list_cursor_destroy(iter);
    iter = list_cursor_clone(range->lo->lc);
    return cost;
}

int main()
{
    bucketing_state* s;
    double default_value = 1000;
    int num_sampling_points = 10;
    double increase_rate = 2;
    bucketing_state_create(default_value, num_sampling_points, increase_rate, s);
    return 0;
}
