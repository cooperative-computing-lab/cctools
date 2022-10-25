#include <stdlib.h>
#include "bucketing.h"
#include "list.h"

/* Insert a bucketing point into a sorted list of points in O(log(n))
 * @param l pointer to sorted list of points
 * @param p pointer to point
 * @return 0 if success
 * @return 1 if failure */
static int bucketing_insert_point_to_sorted_list(struct list* li, bucketing_point *p)
{
    struct list_cursor* lc = list_cursor_create(li);
    
    /* If list is empty, append new point to list */
    if (list_length(li) == 0)
    {
        list_insert(lc, p);
        list_cursor_destroy(lc);
        return 0;
    }

    /* Linear insert a data point */
    list_seek(lc, 0);
    bucketing_point** bpp = malloc(sizeof(*bpp));
    int inserted = 0;
    do
    {
        list_get(lc, (void**) bpp);
        if ((*bpp)->val >= p->val)
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
    free(bpp);
    return 0;
}

bucketing_point* bucketing_point_create(double val, double sig)
{
    bucketing_point* p = malloc(sizeof(*p));
    
    if (p == NULL)
        return p;

    p->val = val;
    p->sig = sig;
    return p;
}

int bucketing_point_delete(bucketing_point *p)
{
    free(p);
    return 0;
}

bucketing_bucket* bucketing_bucket_create(double val, double prob)
{
    bucketing_bucket* b = malloc(sizeof(*b));

    if (b == NULL)
        return b;

    b->val = val;
    b->prob = prob;
    return b;
}

int bucketing_bucket_delete(bucketing_bucket* b)
{
    free(b);
    return 0;
}

bucketing_state* bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate)
{
    bucketing_state* s = malloc(sizeof(*s));

    s->sorted_points = list_create();
    s->sequence_points = list_create();
    s->sorted_buckets = list_create();
    s->num_points = 0;
    s->in_sampling_phase = 1;
    s->prev_op = null;
    s->default_value = default_value;
    s->num_sampling_points = num_sampling_points;
    s->increase_rate = increase_rate;

    return s;
}

int bucketing_state_delete(bucketing_state* s)
{
    list_free(s->sorted_points);
    list_delete(s->sorted_points);
    list_free(s->sequence_points);
    list_delete(s->sequence_points);
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    free(s);

    return 0;
}

bucketing_cursor_w_pos* bucketing_cursor_w_pos_create(struct list_cursor* lc, int pos)
{
    bucketing_cursor_w_pos* cursor_pos = malloc(sizeof(*cursor_pos));
    
    if (!cursor_pos)
        return cursor_pos;
    
    cursor_pos->lc = lc;
    cursor_pos->pos = pos;

    return cursor_pos;
}

int bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos* cursor_pos)
{
    list_cursor_destroy(cursor_pos->lc);
    free(cursor_pos);
    return 0;
}

bucketing_bucket_range* bucketing_bucket_range_create(int lo, int hi, struct list* l)
{
    bucketing_bucket_range* range = malloc(sizeof(*range));
    
    if (!range)
        return range;
    
    struct list_cursor* cursor_lo = list_cursor_create(l);
    list_seek(cursor_lo, lo);
    bucketing_cursor_w_pos* cursor_pos_lo = bucketing_cursor_w_pos_create(cursor_lo, lo);
    range->lo = cursor_pos_lo;
    
    struct list_cursor* cursor_hi = list_cursor_create(l);
    list_seek(cursor_hi, hi);
    bucketing_cursor_w_pos* cursor_pos_hi = bucketing_cursor_w_pos_create(cursor_hi, hi);
    range->hi = cursor_pos_hi;
    return range;
}

int bucketing_bucket_range_delete(bucketing_bucket_range* range)
{
    bucketing_cursor_w_pos_delete(range->lo);
    bucketing_cursor_w_pos_delete(range->hi);
    free(range);
    return 0;
}

int bucketing_add(double val, double sig, bucketing_state* s)
{
    /* insert to sorted list and append to sequence list */
    bucketing_point *p = bucketing_point_create(val, sig);
    bucketing_insert_point_to_sorted_list(s->sorted_points, p);
    list_push_tail(s->sequence_points, p);
    
    /* Change to predicting phase if appropriate */
    s->num_points++;
    if (s->num_points >= s->num_sampling_points)
    {
        s->in_sampling_phase = 0;
    }

    /* set previous operation */
    s->prev_op = add;
    
    return 0;
}
