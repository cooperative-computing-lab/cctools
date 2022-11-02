#ifndef BUCKETING_EXHAUST_H
#define BUCKETING_EXHAUST_H

#include "bucketing.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param s the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_exhaust_update_buckets(bucketing_state *s);

/** End: APIs **/

/** Begin: Internals **/

/* Return list of buckets that have the lowest expected cost
 * @param s the relevant bucketing state
 * @return a list of bucketing_bucket */
struct list* bucketing_exhaust_get_min_cost_bucket_list(bucketing_state* s);

/* Get the list of buckets from a list of points and the number of buckets
 * @param s the relevant bucketing state
 * @param n the number of buckets to get
 * @return a list of bucketing_bucket */
struct list* bucketing_exhaust_get_buckets(bucketing_state* s, int n);

/* Compute cost of a list of buckets using the relevant bucketing state
 * @param s the relevant bucketing state
 * @param bucket_list the list of buckets to be computed
 * @return expected cost of the list of buckets */
double bucketing_exhaust_compute_cost(bucketing_state* s, struct list* bucket_list);

/* Compute the expectations of tasks' values in all buckets
 * @param s the relevant bucketing state
 * @param bucket_list the list of buckets
 * @return pointer to a malloc'ed array of values */
double* bucketing_exhaust_compute_task_exps(bucketing_state* s, struct list* bucket_list);

/** End: Internals **/
#endif
