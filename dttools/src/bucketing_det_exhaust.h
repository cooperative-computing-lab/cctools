#ifndef BUCKETING_DET_EXHAUST_H
#define BUCKETING_DET_EXHAUST_H

#include "bucketing.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param s the relevant bucketing state */
void bucketing_det_exhaust_update_buckets(bucketing_state_t *s);

/** End: APIs **/

#endif
