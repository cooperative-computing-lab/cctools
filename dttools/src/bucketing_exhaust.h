#ifndef BUCKETING_EXHAUSTIVE_H
#define BUCKETING_EXHAUSTIVE_H

#include "bucketing.h"

/** Begin: APIs **/

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_exhaustive_update_buckets(bucketing_state *s);

/** End: APIs **/

/** Begin: Internals **/



/** End: Internals **/
#endif
