#include "bucketing.h"
#include "bucketing_exhaust_common.h"
#include "debug.h"
#include "list.h"
#include "xxmalloc.h"

bucketing_bucket_t **bucketing_bucket_list_to_array(struct list *bucket_list)
{
    if (!bucket_list) {
        fatal("No bucket list\n");
        return 0;
    }

    list_first_item(bucket_list);
    bucketing_bucket_t *tmp_buck;
    bucketing_bucket_t **bucket_array = xxmalloc(list_size(bucket_list) * sizeof(*bucket_array));

    int i = 0;
    while ((tmp_buck = list_next_item(bucket_list))) {
        bucket_array[i] = tmp_buck;
        ++i;
    }

    return bucket_array;
}
