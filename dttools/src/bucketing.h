#ifndef BUCKETING_H
#define BUCKETING_H

#include "list.h"

/* all modes of bucketing */
typedef enum {
    BUCKETING_MODE_GREEDY,
    BUCKETING_MODE_EXHAUSTIVE
} bucketing_mode_t;

/* Bucketing has two operations, add and predict */
typedef enum {
    BUCKETING_OP_ADD = 0,
    BUCKETING_OP_PREDICT,
    BUCKETING_OP_NULL    //only used when initializing
} bucketing_operation_t;

/* Each point (e.g., a task) in a bucket is a pair of value 
 * (e.g., memory consumption) and significance 
 * (encoding relative time position of task) */
typedef struct
{
    /* value */
    double val;

    /* significance */
    double sig;
} bucketing_point_t;

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
} bucketing_bucket_t;

/* State of the bucket */
typedef struct
{
    /** Begin: internally maintained fields **/
    /* a doubly linked list of pointers to points of type 'bucketing_point_t'
     * sorted by 'point->val' in increasing order
     * sorted_points and sequence_points share the same set of pointers */
    struct list *sorted_points;

    /* a doubly linked list of pointers to points of type 'bucketing_point_t'
     * sorted by 'point->sig' in increasing order
     * sequence_points and sorted_points share the same set of pointers */
    struct list *sequence_points;
    
    /* a doubly linked list of pointers to buckets of type 'bucketing_bucket_t'
     * sorted by 'bucket->val' in increasing order */
    struct list *sorted_buckets;
    
    /* total number of points */
    int num_points;
    
    /* whether bucketing is in sampling phase, 1 is yes, 0 is no */
    int in_sampling_phase; 
    
    /* track previous operation, this helps with the decision to find
     * buckets or not. This is -1 in the beginning as there's no previous
     * operation. */
    bucketing_operation_t prev_op;

    /* the significance value of the next task to be added */
    int next_task_sig;

    /** End: internally maintained fields **/

    /** Begin: externally provided fields **/
    /* default value to be used in sampling phase */
    double default_value;

    /* number of points needed to transition from sampling to predicting phase */
    int num_sampling_points;
    
    /* rate to increase a value when in sampling phase or exceeded max value in 
     * predicting phase */
    double increase_rate;

    /* the maximum number of buckets to break (only exhaustive bucketing) */
    int max_num_buckets;

    /* the update mode to use */
    bucketing_mode_t mode;

    /* The number of iterations before another bucketing happens */
    int update_epoch;

    /** End: externally provided fields **/ 
} bucketing_state_t;

/** Begin: APIs **/

/* Create a bucketing bucket
 * @param val value of bucket
 * @param prob probability of bucket
 * @return pointer to created bucket
 * @return 0 if failure */
bucketing_bucket_t* bucketing_bucket_create(double val, double prob);

/* Delete a bucketing bucket
 * @param b the bucket to be deleted */
void bucketing_bucket_delete(bucketing_bucket_t* b);

/* Create a bucketing state
 * @param default_value default value in sampling state
 * @param num_sampling_points number of needed sampling points
 * @param increase_rate rate to increase values
 * @param max_num_buckets the maximum number of buckets to find (only for exhaustive bucketing)
 * @param mode specify which update mode of bucketing state
 * @param update_epoch number of iterations to wait before updating the bucketing state
 * @return pointer to created bucketing state
 * @return 0 if failure */
bucketing_state_t* bucketing_state_create(double default_value, int num_sampling_points,
    double increase_rate, int max_num_buckets, bucketing_mode_t mode, int update_epoch);

/* Delete a bucketing state
 * @param s pointer to bucketing state to be deleted */
void bucketing_state_delete(bucketing_state_t* s);

/* Tune externally provided fields
 * @param s the bucketing state
 * @param field string describing the field, must be the same as external fields
 * defined in bucketing state
 * @param val value to be casted inside this function, -1 otherwise */
void bucketing_state_tune(bucketing_state_t* s, const char* field, void* val);

/* Add a point
 * @param s the relevant bucketing state 
 * @param val value of point to be added */
void bucketing_add(bucketing_state_t* s, double val);

/* Predict a value, only predict when we need a new higher value, don't predict when prev value
 * (if available) is usable
 * @param s the relevant bucketing_state_t
 * @param prev_val previous value to consider, -1 if no previous value, 
 * > 0 means a larger value is expected from prediction
 * @return the predicted value
 * @return -1 if failure */
double bucketing_predict(bucketing_state_t* s, double prev_val);

/** End: APIs **/

/** Begin: debug functions **/

/* Print a sorted list of bucketing_bucket_t
 * @param l the list of buckets */
void bucketing_sorted_buckets_print(struct list* l);

/* Print a sorted list of bucketing_point_t
 * @param l the list of points */
void bucketing_sorted_points_print(struct list* l);

/** End: debug functions **/

#endif
