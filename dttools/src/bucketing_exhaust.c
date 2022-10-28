#include "bucketing.h"
#include "bucketing_exhaust.h"
#include "list.h"

int bucketing_exhaust_update_buckets(bucketing_state *s)
{
    s->sorted_buckets = bucketing_exhaust_get_min_cost_bucket_list(s);
    
    return 0;
}

struct list* bucketing_exhaust_get_min_cost_bucket_list(s)
{
    for (int i = 0; i < s->max_num_buckets; ++i)
        {
            struct list* bucket_list = bucketing_exhaust_get_buckets(s, i);
        }
}

struct list* bucketing_exhaust_get_buckets(s, n)
{
    
}
