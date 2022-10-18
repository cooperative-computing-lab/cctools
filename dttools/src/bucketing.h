#ifndef BUCKETING_H
#define BUCKETING_H

#include "list.h"

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

/* Bucketing has two operations, add and predict */ 
typedef enum
{
    add = 0,
    predict
} bucketing_operation;

/* State of the bucket */
typedef struct
{
    /** Begin: internally maintained fields **/
    /* a doubly linked list of points of type 'bucketing_point'
     * sorted by 'point->val' in increasing order */
    struct list *sorted_points;

    /* a doubly linked list of points of type 'bucketing_point'
     * sorted by 'point->sig' in increasing order */
    struct list *sequence_points;
    
    /* a doubly linked list of buckets of type 'bucketing_bucket'
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
    /** End: externally provided fields **/ 
} bucketing_state;

/** Begin: bucketing's API **/
/* Create a bucketing point
 * @param val value of point
 * @param sig significance of point
 * @param p pointer to a bucketing point
 * @return 0 if success
 * @return 1 if failure */
int bucketing_point_create(double val, double sig, bucketing_point* p);

/* Delete a bucketing point
 * @param p the bucketing point to be deleted
 * @return 0 if success
 * @return 1 if failure */
int bucketing_point_delete(bucketing_point* p);

/* Create a bucketing bucket
 * @param val value of bucket
 * @param prob probability of bucket
 * @param b pointer to a bucketing bucket
 * @return 0 if success
 * @return 1 if failure */
int bucketing_bucket_create(double val, double prob, bucketing_bucket* b);

/* Delete a bucketing bucket
 * @param b the bucket to be deleted
 * @return 0 if success
 * @return 1 if failure */
int bucketing_bucket_delete(bucketing_bucket* b);

/* Create a bucketing state
 * @param default_value default value in sampling state
 * @param num_sampling_points number of needed sampling points
 * @param increase_rate rate to increase values
 * @param s pointer to bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, bucketing_state* s);

/* Delete a bucketing state
 * @param s pointer to bucketing state to be deleted
 * @return 0 if success
 * @return 1 if failure */
int bucketing_state_delete(bucketing_state* s);

/* Add a point
 * @param val value of point to be added
 * @param sig significance of point to be added
 * @param s the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_add(double val, double sig, bucketing_state* s);

/** End: bucketing's API **/

/** Begin: bucketing's internals **/

/* Insert a bucketing point into a sorted list of points in O(log(n))
 * @param l pointer to sorted list of points
 * @param p pointer to point
 * @return 0 if success
 * @return 1 if failure */
static int bucketing_insert_point_to_sorted_list(struct list* l, bucketing_point *p);

/** End: bucketing's internals **/

#endif
