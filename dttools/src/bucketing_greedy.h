#ifndef BUCKETING_GREEDY_H
#define BUCKETING_GREEDY_H

#include "bucketing.h"
#include "list.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state */
void bucketing_greedy_update_buckets(bucketing_state_t* s);

/** End: APIs **/

#endif
