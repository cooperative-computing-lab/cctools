#include <stdlib.h>
#include "bucketing_greedy.h"
#include "xxmalloc.h"
#include "debug.h"

/** Begin: internals **/

/* List cursor with its position in a list */
typedef struct
{
    struct list_cursor* lc;
    int pos;
} bucketing_cursor_w_pos_t;

/* Range defined by a low cursor and a high cursor pointing to a list */
typedef struct
{
    bucketing_cursor_w_pos_t* lo;
    bucketing_cursor_w_pos_t* hi;
} bucketing_bucket_range_t;

/* Cursor but with position in list
 * @param lc pointer to list cursor
 * @param pos position of list cursor in a list
 * @return pointer to bucketing_cursor_w_pos_t structure if success
 * @return 0 if failure */
static bucketing_cursor_w_pos_t* bucketing_cursor_w_pos_create(struct list_cursor* lc, int pos)
{
    bucketing_cursor_w_pos_t* cursor_pos = xxmalloc(sizeof(*cursor_pos));

    cursor_pos->lc = lc;
    cursor_pos->pos = pos;

    return cursor_pos;
}

/* Delete a bucketing_cursor_w_pos_t structure
 * @param cursor_pos the structure to be deleted */
static void bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos_t* cursor_pos)
{
    if(cursor_pos)
    {
        list_cursor_destroy(cursor_pos->lc);
        free(cursor_pos);
    }
}

/* Create a bucketing_bucket_range_t structure
 * @param lo low index
 * @param hi high index
 * @param l list that indices point to
 * @return pointer to a bucketing range if success
 * @return 0 if failure */
static bucketing_bucket_range_t* bucketing_bucket_range_create(int lo, int hi, struct list* l)
{
    bucketing_bucket_range_t* range = xxmalloc(sizeof(*range));
       
    struct list_cursor* cursor_lo = list_cursor_create(l);
    if (!list_seek(cursor_lo, lo))
        return 0;
 
    bucketing_cursor_w_pos_t* cursor_pos_lo = bucketing_cursor_w_pos_create(cursor_lo, lo);
    if (!cursor_pos_lo)
        return 0;
    
    range->lo = cursor_pos_lo;
    
    struct list_cursor* cursor_hi = list_cursor_create(l);
    if (!list_seek(cursor_hi, hi))
        return 0;
    
    bucketing_cursor_w_pos_t* cursor_pos_hi = bucketing_cursor_w_pos_create(cursor_hi, hi);
    if (!cursor_pos_hi)
        return 0;

    range->hi = cursor_pos_hi;

    return range;
}

/* Delete a bucketing_bucket_range_t
 * @param range the structure to be deleted */
static void bucketing_bucket_range_delete(bucketing_bucket_range_t* range)
{
    if (range)
    {
        bucketing_cursor_w_pos_delete(range->lo);
        bucketing_cursor_w_pos_delete(range->hi);
        free(range);
    }
    else {
        warn(D_BUCKETING, "ignoring command to delete a null pointer to bucket range\n");
    }
}

/* Free the list with the function used to free a bucketing_cursor_pos
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_cursor_pos */
static void bucketing_cursor_pos_list_clear(struct list* l, void (*f) (bucketing_cursor_w_pos_t*))
{
    if (!l)
        return;

    bucketing_cursor_w_pos_t* tmp;

    while ((tmp = list_pop_head(l)))
        f(tmp);
}

/* Free the list with the function used to free a bucketing_bucket_range_t
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_bucket_range_t */
static void bucketing_bucket_range_list_clear(struct list* l, void (*f) (bucketing_bucket_range_t*))
{
    if (!l)
        return;

    bucketing_bucket_range_t* tmp;

    while ((tmp = list_pop_head(l)))
        f(tmp);
}

/* Sort a list of bucketing_cursor_pos
 * @param l the list to be sorted
 * @param f the compare function
 * @return pointer to a sorted list of bucketing_cursor_pos
 * @return 0 if failure */
static struct list* bucketing_cursor_pos_list_sort(struct list* l, int (*f) (const void*, const void*))
{
    if (!l)
        return 0;
    
    unsigned int size = list_length(l);
    unsigned int i = 0;
    bucketing_cursor_w_pos_t** arr = xxmalloc(size * sizeof(*arr));
    if (!arr)
    {
        fatal("Cannot create temp array\n");
        return 0;
    }

    struct list_cursor* lc = list_cursor_create(l);
  
    if (!list_seek(lc, 0))
    {
        fatal("Cannot seek list\n");
        return 0;
    }

    /* Save all elements to array */
    while (list_get(lc, (void**) &arr[i]))
    {
        ++i;
        list_next(lc);
    }

    /* Destroy the list but not its elements */
    list_cursor_destroy(lc); 
    list_delete(l);

    /* Qsort the array */
    qsort(arr, size, sizeof(*arr), f);

    struct list* ret = list_create();
    lc = list_cursor_create(ret);

    /* Put back elements to a new list */
    for (i = 0; i < size; ++i)
        list_insert(lc, arr[i]);

    list_cursor_destroy(lc);
    free(arr);

    return ret;
}

/* Compare position of two break points
 * @param p1 first break point
 * @param p2 second break point
 * @return negative if p1 < p2, 0 if p1 == p2, positive if p1 > p2 */
static int bucketing_compare_break_points(const void* p1, const void* p2)
{
    if (!p1 || !p2)
    {
        fatal("Cannot compare empty break points\n");
        return 0;
    }
    return (*((bucketing_cursor_w_pos_t**) p1))->pos - (*((bucketing_cursor_w_pos_t**) p2))->pos;
}

/* Apply policy to see if calculate cost of using this break point at break index
 * @param range range of two break points denoting current bucket
 * @param break_index the index of break point
 * @param break_point empty pointer to be filled
 * @return cost of current break point
 * @return -1 if failure */
static double bucketing_greedy_policy(bucketing_bucket_range_t* range, int break_index, bucketing_cursor_w_pos_t** break_point)
{
    if (!range)
    {
        fatal("No range to apply policy\n");
        return -1;
    }

    int total_sig = 0;                  //track total significance of points in range
    int total_lo_sig = 0;               //track total significance in low range
    int total_hi_sig = 0;               //track total significance in high range
    double p1 = 0;                      //probability of candidate lower bucket
    double p2 = 0;                      //probability of candidate higher bucket
    bucketing_point_t* tmp_point_ptr = 0; //pointer to get item from sorted points
    double exp_cons_lq_break = 0;       //expected value if next point is lower than or equal to break point
    double exp_cons_g_break = 0;        //expected value if next point is higher than break point
    int break_val = -1; //value at break point
    int max_val = -1;   //value at max point
    struct list_cursor* iter = list_cursor_clone(range->lo->lc);    //cursor to iterate through list

    /* Loop through the range to collect statistics */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i, list_next(iter))
    {
        if (!list_get(iter, (void**) &tmp_point_ptr))
        {
            fatal("Cannot get item from list\n");
            return -1;
        }

        total_sig += tmp_point_ptr->sig;
        
        if (i == break_index)
        {
            break_val = tmp_point_ptr->val;
            *break_point = bucketing_cursor_w_pos_create(list_cursor_clone(iter), break_index);
            if (!(*break_point))
            {
                fatal("Cannot create break point\n");
                return -1;
            }
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

/* Break a bucket into 2 buckets if possible
 * @param range range of to-be-broken bucket
 * @param break_point empty pointer
 * @return 0 if can break bucket
 * @return 1 if cannot break bucket
 * @return -1 if failure */
static int bucketing_greedy_break_bucket(bucketing_bucket_range_t* range, bucketing_cursor_w_pos_t** break_point)
{
    if (!range)
    {
        fatal("No range to break\n");
        return -1;
    }

    double min_cost = -1;   //track min cost of a candidate break point
    double cost;    //track cost of current point
    bucketing_cursor_w_pos_t* tmp_break_point = 0;   //get current point 

    /* Loop through all points in range and choose 1 with the lowest cost */
    for (int i = range->lo->pos; i <= range->hi->pos; ++i)
    {
        cost = bucketing_greedy_policy(range, i, &tmp_break_point);
        if (cost == -1)
        {
            fatal("Problem computing cost\n");
            return -1;
        }

        if (min_cost == -1)
        {
            min_cost = cost;
            *break_point = tmp_break_point;
        }
        else if (cost <= min_cost)
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

/* Find all break points from a bucketing state
 * @param s bucketing state
 * @return pointer to a break point list if success
 * @return null if failure */
static struct list* bucketing_greedy_find_break_points(bucketing_state_t* s)
{
    if (!s)
    {
        fatal("Empty bucketing state\n");
        return 0;
    }

    int min = 0; //min index of first bucket

    if (!s->sorted_points)
    {
        fatal("Empty sorted list of points\n");
        return 0;
    }

    int max = list_length(s->sorted_points) - 1; //max index of first bucket

    /* Create list of break points to be returned */
    struct list* break_point_list = list_create();

    /* create list and push (0, n-1) of sorted points to list of buckets */
    struct list* bucket_range_list = list_create();
    bucketing_bucket_range_t* init_range = bucketing_bucket_range_create(min, max, s->sorted_points);
    if (!init_range)
    {
        fatal("Cannot create first range\n");
        return 0;
    }

    if (!list_push_tail(bucket_range_list, init_range))
    {
        fatal("Cannot push init_range bucket to end of list\n");
        return 0;
    }

    bucketing_bucket_range_t* lo_bucket_range;    //create low bucket, if possible
    bucketing_bucket_range_t* hi_bucket_range;    //create high bucket, if possible
    bucketing_cursor_w_pos_t* break_point = 0;    //store break point betweem high and low buckets
    bucketing_bucket_range_t* bbr_ptr = 0;        //pointer to a bucket in bucket_range_list
    
    struct list_cursor* lc = list_cursor_create(bucket_range_list);
    if (!list_seek(lc, 0))
    {
        fatal("Cannot seek list\n");
        return 0;
    }
    
    int breakable;

    /* Loop through all buckets and break them if broken buckets have more than 1 point */
    do
    {
        if (!list_get(lc, (void**) &bbr_ptr))
        {
            fatal("Cannot get item from list\n");
            return 0;
        }
        
        breakable = bucketing_greedy_break_bucket(bbr_ptr, &break_point);
        
        /* If bucket is breakable, break it. Else do nothing */
        if (breakable == 0)
        {
            if (!list_push_tail(break_point_list, break_point))
            {
                fatal("Cannot push break point to end of break point list\n");
                return 0;
            }

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
                    {
                        fatal("Cannot create high bucket range\n");
                        return 0;
                    }

                    if (!list_push_tail(bucket_range_list, hi_bucket_range))
                    {
                        fatal("Cannot push high bucket to bucket range list\n");
                        return 0;
                    }
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
                    {
                        fatal("Cannot create high bucket range\n");
                        return 0;
                    }

                    if (!list_push_tail(bucket_range_list, hi_bucket_range))
                    {
                        fatal("Cannot push high bucket to bucket range list\n");
                        return 0;
                    }
                }
                lo_bucket_range = bucketing_bucket_range_create(bbr_ptr->lo->pos, break_point->pos, s->sorted_points);
                if (!lo_bucket_range)
                {
                    fatal("Cannot create low bucket range\n");
                    return 0;
                }

                if (!list_push_tail(bucket_range_list, lo_bucket_range))
                {
                    fatal("Cannot push low bucket to bucket range list\n");
                    return 0;
                }
            } 
        }
        else if (breakable == -1)
        {
            fatal("Problem breaking bucket\n");
            return 0;
        }

    } while (list_next(lc));

    /* Push the highest point into the break point list */
    bucketing_cursor_w_pos_t* last_break_point = bucketing_cursor_w_pos_create(list_cursor_clone(init_range->hi->lc), init_range->hi->pos);
    if (!last_break_point)
    {
        fatal("Cannot create last break point\n");
        return 0;
    }

    if (!list_push_tail(break_point_list, last_break_point))
    {
        fatal("Cannot push last break point to break point list\n");
        return 0;
    }

    /* Sort in increasing order */
    break_point_list = bucketing_cursor_pos_list_sort(break_point_list, bucketing_compare_break_points);
    if (!break_point_list)
    {
        fatal("Cannot sort list of break points\n");
        return 0;
    }
    
    /* Destroy bucket range list */
    list_cursor_destroy(lc);
    bucketing_bucket_range_list_clear(bucket_range_list, bucketing_bucket_range_delete);

    list_delete(bucket_range_list);
    
    return break_point_list;
}

/** End: internals **/

void bucketing_greedy_update_buckets(bucketing_state_t* s)
{
    if (!s)
    {
        fatal("No state to update buckets\n");
        return;
    }
    
    /* Delete old list of buckets */
    list_free(s->sorted_buckets);
    list_delete(s->sorted_buckets);
    
    /* Create new list of buckets */
    s->sorted_buckets = list_create();

    /* Find all break points */
    struct list* break_point_list = bucketing_greedy_find_break_points(s);
    if (!break_point_list)
    {
        fatal("Cannot find break points\n");
        return;
    }

    /* Find probabilities of buckets */
    double bucket_probs[list_size(break_point_list)];   //store probabilities of buckets
    list_first_item(s->sorted_points);
    list_first_item(break_point_list);
    bucketing_point_t* tmp_point;                 //pointer to item in s->sorted_points
    bucketing_cursor_w_pos_t* tmp_break_point;    //pointer pointing to item in break point list
    int i = 0;
    bucket_probs[0] = 0;
    double total_sig = 0;                       //track total significance

    if (!(tmp_point = list_next_item(s->sorted_points)))
    {
        fatal("bucketing: cannot get tmp point\n");
        return;
    }
    
    if (!(tmp_break_point = list_next_item(break_point_list)))
    {
        fatal("bucketing: cannot get tmp break point\n");
        return;
    }
    bucketing_point_t* tmp_point_of_break_point = 0;
    if (!list_get(tmp_break_point->lc, (void**) &tmp_point_of_break_point))
    {
        fatal("Cannot get item from list\n");
        return;
    }
    
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
            {
                fatal("Cannot get item from list\n");
                return;
            }
        }
    }

    /* must divide by total significance to normalize to [0, 1] */
    for (int i = 0; i < list_size(break_point_list); ++i)
    {
        bucket_probs[i] /= total_sig;    
    }

    bucketing_bucket_t* tmp_bucket;               //pointer to a created bucket
    bucketing_point_t* tmp_point_ptr = 0;         //pointer to what tmp_break_point->lc points
    list_first_item(break_point_list);          //reset to beginning of break point list
    i = 0;

    /* Loop through list of break points */
    while ((tmp_break_point = list_next_item(break_point_list)))
    {
        if (!list_get(tmp_break_point->lc, (void**) &tmp_point_ptr))
        {
            fatal("Cannot get item from list\n");
            return;
        }

        tmp_bucket = bucketing_bucket_create(tmp_point_ptr->val, bucket_probs[i]);
        if (!tmp_bucket)
        {
            fatal("Cannot create bucket\n");
            return;
        }

        if (!list_push_tail(s->sorted_buckets, tmp_bucket))
        {
            fatal("Cannot push tmp bucket to sorted buckets\n");
            return;
        }
        ++i;
    }
    
    /* Delete break point list */
    bucketing_cursor_pos_list_clear(break_point_list, bucketing_cursor_w_pos_delete);

    list_delete(break_point_list);
    
    return;
}
