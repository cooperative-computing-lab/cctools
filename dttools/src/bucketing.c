#include <stdlib.h>
#include "bucketing.h"
#include "list.h"

int bucketing_point_create(double val, double sig, bucketing_point* p)
{
    p = malloc(sizeof(bucketing_point));
    if (p == NULL)
    {
        return 1;
    }
    p->val = val;
    p->sig = sig;
    return 0;
}

int bucketing_point_delete(bucketing_point *p)
{
    free(p);
    return 0;
}

int bucketing_bucket_create(double val, double prob, bucketing_bucket* b)
{
    b = malloc(sizeof(bucketing_bucket));
    if (b == NULL)
    {
        return 1;
    }
    b->val = val;
    b->prob = prob;
    return 0;
}

int bucketing_bucket_delete(bucketing_bucket* b)
{
    free(b);
    return 0;
}

int bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, bucketing_state* s)
{
    s = malloc(sizeof(bucketing_state));
    s->sorted_points = list_create();
    s->sequence_points = list_create();
    s->sorted_buckets = list_create();
    s->num_points = 0;
    s->in_sampling_phase = 1;
    s->prev_op = -1;
    s->default_value = default_value;
    s->num_sampling_points = num_sampling_points;
    s->increase_rate = increase_rate;
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
}

int bucketing_cursor_w_pos_create(struct list_cursor* lc, int pos, bucketing_cursor_w_pos* cursor_pos)
{
    cursor_pos = malloc(sizeof(*cursor_pos));
    cursor_pos->lc = lc;
    cursor_pos->pos = pos;
    return 0;
}

int bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos* cursor_pos)
{
    list_cursor_destroy(cursor_pos->lc);
    free(cursor_pos);
}

int bucketing_bucket_range_create(int lo, int hi, struct list* l, bucketing_bucket_range* range)
{
    range = malloc(sizeof(*range));
    struct list_cursor* lc_lo = list_cursor_create(l);
    list_seek(lc_lo, lo);
    struct list_cursor* lc_hi = list_cursor_create(l);
    list_seek(lc_hi, hi);
    bucketing_cursor_w_pos* cursor_pos_lo;
    bucketing_cursor_w_pos_create(lc_lo, lo, cursor_pos_lo);
    bucketing_cursor_w_pos* cursor_pos_hi; 
    bucketing_cursor_w_pos_create(lc_hi, hi, cursor_pos_hi);
    range->lo = cursor_pos_lo;
    range->hi = cursor_pos_hi;
    return range;
}

int bucketing_bucket_range_delete(bucketing_bucket_range* range)
{
    bucketing_cursor_w_pos_delete(range->lo);
    bucketing_cursor_w_pos_delete(range->hi);
    free(range);
}

int bucketing_add(double val, double sig, bucketing_state* s)
{
    bucketing_point *p = bucketing_point_create(val, sig);
    bucketing_insert_point_to_sorted_list(s->sorted_points, p);
    list_push_tail(s->sequence_points, p);
    s->num_points++;
    if (s->num_points >= s->num_sampling_points)
    {
        s->in_sampling_phase = 0;
    }
    s->prev_op = add;
    return 0;
}

static int bucketing_insert_point_to_sorted_list(struct list* li, bucketing_point *p)
{
    list_cursor* lc = list_cursor_create(li);
    if (list_length(li) == 0)
    {
        list_insert(lc, p);
        list_cursor_destroy(lc);
        return 0;
    }
    list_seek(lc, 0);
    bucketing_point_ptr** bpp;
    inserted = 0;
    do
    {
        list_get(lc, bpp);
        if ((*bpp)->val >= p->val)
        {
            list_insert(lc, p);
            inserted = 1;
            break;
        }
    }
    while (list_next(lc));
    if (inserted == 0)
    {
        list_insert(lc, p);
    }
    list_cursor_destroy(lc);
    return 0;
}
