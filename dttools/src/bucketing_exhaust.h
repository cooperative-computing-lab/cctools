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

#endif
