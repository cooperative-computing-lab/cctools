#ifndef BUCKETING_EXHAUST_COMMON_H
#define BUCKETING_EXHAUST_COMMON_H

#include "bucketing.h"
#include "list.h"

/* Convert a list of bucketing_bucket_t to an array of those
 * @param bucket_list list of bucketing_bucket_t
 * @return pointer to array of bucketing_bucket_t
 * @return 0 if failure */
bucketing_bucket_t **bucketing_bucket_list_to_array(struct list *bucket_list);

#endif
