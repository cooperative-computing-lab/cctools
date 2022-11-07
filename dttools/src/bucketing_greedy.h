#ifndef BUCKETING_GREEDY_H
#define BUCKETING_GREEDY_H

#include "bucketing.h"
#include "list.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_greedy_update_buckets(bucketing_state* s);

/** End: APIs **/

#endif
