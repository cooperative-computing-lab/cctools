#ifndef BUCKETING_H
#define BUCKETING_H

#include "list.h"

/* Bucketing has two operations, add and predict */ 
typedef enum
{
    add = 0,
    predict,
    null    //only used when initializing
} bucketing_operation;

/* Each point (e.g., a task) in a bucket is a pair of value 
 * (e.g., memory consumption) and significance 
 * (encoding relative time position of task) */
typedef struct
{
    /* value */
    double val;

    /* significance */
    double sig;
} bucketing_point;

/* Each bucket is a pair of value (top delimiter) and probability 
 * that the next task falls into its range (lo, hi) where lo is
 * the top delimiter of the right below bucket (or zero if bucket 
 * is the lowest) and hi is value */
typedef struct
{
    /* value */
    double val;

    /* probability */
    double prob;
} bucketing_bucket;

/* State of the bucket */
typedef struct
{
    /** Begin: internally maintained fields **/
    /* a doubly linked list of pointers to points of type 'bucketing_point'
     * sorted by 'point->val' in increasing order
     * sorted_points and sequence_points share the same set of pointers */
    struct list *sorted_points;

    /* a doubly linked list of pointers to points of type 'bucketing_point'
     * sorted by 'point->sig' in increasing order
     * sequence_points and sorted_points share the same set of pointers */
    struct list *sequence_points;
    
    /* a doubly linked list of pointers to buckets of type 'bucketing_bucket'
     * sorted by 'bucket->val' in increasing order */
    struct list *sorted_buckets;
    
    /* total number of points */
    int num_points;
    
    /* whether bucketing is in sampling phase, 1 is yes, 0 is no */
    int in_sampling_phase; 
    
    /* track previous operation, this helps with the decision to find
     * buckets or not. This is -1 in the beginning as there's no previous
     * operation. */
    bucketing_operation prev_op;

    /** End: internally maintained fields **/

    /** Begin: externally provided fields **/
    /* default value to be used in sampling phase */
    double default_value;

    /* number of points needed to transition from sampling to predicting phase */
    int num_sampling_points;
    
    /* rate to increase a value when in sampling phase or exceeded max value in 
     * predicting phase */
    double increase_rate;

    /* the maximum number of buckets to break (only exhaustive bucketing */
    int max_num_buckets;

    /** End: externally provided fields **/ 
} bucketing_state;

/* List cursor with its position in a list */
typedef struct
{
    struct list_cursor* lc;
    int pos;
} bucketing_cursor_w_pos;

/* Range defined by a low cursor and a high cursor pointing to a list */
typedef struct
{
    bucketing_cursor_w_pos* lo;
    bucketing_cursor_w_pos* hi;
} bucketing_bucket_range;

/** Begin: APIs **/

/* Create a bucketing point
 * @param val value of point
 * @param sig significance of point
 * @return pointer to created point
 * @return NULL if failure */
bucketing_point* bucketing_point_create(double val, double sig);

/* Delete a bucketing point
 * @param p the bucketing point to be deleted */
void bucketing_point_delete(bucketing_point* p);

/* Create a bucketing bucket
 * @param val value of bucket
 * @param prob probability of bucket
 * @return pointer to created bucket
 * @return NULL if failure */
bucketing_bucket* bucketing_bucket_create(double val, double prob);

/* Delete a bucketing bucket
 * @param b the bucket to be deleted */
void bucketing_bucket_delete(bucketing_bucket* b);

/* Create a bucketing state
 * @param default_value default value in sampling state
 * @param num_sampling_points number of needed sampling points
 * @param increase_rate rate to increase values
 * @param max_num_buckets the maximum number of buckets to find (only for exhaustive bucketing)
 * @return pointer to created bucketing state
 * @return NULL if failure */
bucketing_state* bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, int max_num_buckets);

/* Delete a bucketing state
 * @param s pointer to bucketing state to be deleted */
void bucketing_state_delete(bucketing_state* s);

/* Cursor but with position in list
 * @param lc pointer to list cursor
 * @param pos position of list cursor in a list
 * @return pointer to bucketing_cursor_w_pos structure if success
 * @return NULL if failure */
bucketing_cursor_w_pos* bucketing_cursor_w_pos_create(struct list_cursor* lc, int pos);

/* Delete a bucketing_cursor_w_pos structure
 * @param cursor_pos the structure to be deleted */
void bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos* cursor_pos);

/* Create a bucketing_bucket_range structure
 * @param lo low index
 * @param hi high index
 * @param l list that indices point to
 * @return pointer to a bucketing range if success
 * @return NULL if failure */
bucketing_bucket_range* bucketing_bucket_range_create(int lo, int hi, struct list* l);

/* Delete a bucketing_bucket_range
 * @param range the structure to be deleted */
void bucketing_bucket_range_delete(bucketing_bucket_range* range);

/* Add a point
 * @param val value of point to be added
 * @param sig significance of point to be added
 * @param s the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_add(double val, double sig, bucketing_state* s);

/* Predict a value
 * @param prev_val previous value to consider, -1 if no previous value, 
 * > 0 means a larger value is expected from prediction
 * @param s the relevant bucketing_state
 * @return the predicted value, -1 if failure */
double bucketing_predict(double prev_val, bucketing_state* s);

/** End: APIs **/

/** Begin: internals **/

/* Insert a bucketing point into a sorted list of points in O(log(n))
 * @param l pointer to sorted list of points
 * @param p pointer to point
 * @return 0 if success
 * @return 1 if failure */
int bucketing_insert_point_to_sorted_list(struct list* li, bucketing_point *p);

/* Free the list with the function used to free a bucketing_cursor_pos
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_cursor_pos
 * @return 0 if success
 * @return 1 if failure */
int bucketing_cursor_pos_list_clear(struct list* l, void (*f) (bucketing_cursor_w_pos*));

/* Free the list with the function used to free a bucketing_bucket_range
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_bucket_range
 * @return 0 if success
 * @return 1 if failure */
int bucketing_bucket_range_list_clear(struct list* l, void (*f) (bucketing_bucket_range*));

/* Sort a list of bucketing_cursor_pos
 * @param l the list to be sorted
 * @param f the compare function
 * @return pointer to a sorted list of bucketing_cursor_pos */
struct list* bucketing_cursor_pos_list_sort(struct list* l, int (*f) (const void*, const void*));

/* Compare position of two break points
 * @param p1 first break point
 * @param p2 second break point
 * @return negative if p1 < p2, 0 if p1 == p2, positive if p1 > p2 */
int compare_break_points(const void* p1, const void* p2);

/** End: internals **/

#endif
