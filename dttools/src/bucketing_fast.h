#ifndef BUCKETING_FAST_H
#define BUCKETING_FAST_H

#include "bucketing.h"
#include "list.h"

/** Begin: APIs **/
/* Predict a value
 * @param prev_val previous value to consider, -1 if no previous value
 * > 0 means a larger value is expected from prediction
 * @param s the relevant bucketing_state
 * @return the predicted value, -1 if failure */
double bucketing_fast_predict(double prev_val, bucketing_state* s);

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_fast_update_buckets(bucketing_state* s);

/** End: APIs **/

/** Begin: Internals **/
/* Find all break points from a bucketing state
 * @param s bucketing state
 * @param break_point_list empty pointer to be filled w list
 * @return 0 if success
 * @return 1 if failure */
int bucketing_find_break_points(bucketing_state* s, struct list* break_point_list);

/* Compare position of two break points
 * @param p1 first break point
 * @param p2 second break point
 * @return negative if p1 < p2, 0 if p1 == p2, positive if p1 > p2 */
int compare_break_points(bucketing_cursor_w_pos* p1, bucketing_cursor_w_pos* p2);

/* Break a bucket into 2 buckets if possible
 * @param range range of to-be-broken bucket
 * @param break_point empty pointer
 * @return 0 if success
 * @return 1 if failure */
int bucketing_fast_break_bucket(bucketing_bucket_range* range, bucketing_cursor_w_pos* break_point);

/* Apply policy to see if calculate cost of using this break point at break index
 * @param range range of two break points denoting current bucket
 * @param break_index the index of break point
 * @param break_point empty pointer to be filled
 * @return cost of current break point */
double bucketing_fast_policy(bucketing_bucket_range* range, int break_index, bucketing_cursor_w_pos* break_point);

#endif
