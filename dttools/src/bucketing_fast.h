#ifndef BUCKETING_FAST_H
#define BUCKETING_FAST_H

#include "bucketing.h"
#include "list.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_fast_update_buckets(bucketing_state* s);

/** End: APIs **/

/** Begin: Internals **/

/* Find all break points from a bucketing state
 * @param s bucketing state
 * @return pointer to a break point list if success
 * @return null if failure */
struct list* bucketing_find_break_points(bucketing_state* s);

/* Break a bucket into 2 buckets if possible
 * @param range range of to-be-broken bucket
 * @param break_point empty pointer
 * @return 0 if success
 * @return 1 if failure */
int bucketing_fast_break_bucket(bucketing_bucket_range* range, bucketing_cursor_w_pos** break_point);

/* Apply policy to see if calculate cost of using this break point at break index
 * @param range range of two break points denoting current bucket
 * @param break_index the index of break point
 * @param break_point empty pointer to be filled
 * @return cost of current break point */
double bucketing_fast_policy(bucketing_bucket_range* range, int break_index, bucketing_cursor_w_pos** break_point);

/** End: Internals **/

#endif
