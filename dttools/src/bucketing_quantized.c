#include <stdlib.h>
#include <math.h>
#include "bucketing_quantized.h"
#include "list.h"
#include "xxmalloc.h"
#include "debug.h"

/** Begin: internals **/

/** End: internals **/

/** Begin: APIs **/

void bucketing_quantized_update_buckets(bucketing_state_t *s)
{
	if (!s) {
		fatal("No bucket state to update buckets\n");
		return;
	}

	/* Destroy old list */
	list_free(s->sorted_buckets);
	list_delete(s->sorted_buckets);

	/* Update with new list */
	int l = list_size(s->sorted_points);	     // size of list of sorted points
	int i = 0;				     // track number of points seen so far
	int j = 0;				     // track number of added buckets
	int q = floor(1.0 * l / s->max_num_buckets); // track index of next quantize
	double prob = 0;			     // track probability of current bucket
	s->sorted_buckets = list_create();

	bucketing_point_t *p;
	bucketing_bucket_t *b;
	list_first_item(s->sorted_points);
	while ((p = list_next_item(s->sorted_points))) {
		++i;
		++prob;
		if (i >= q) {
			prob /= l;
			b = bucketing_bucket_create(p->val, prob);
			if (!list_push_tail(s->sorted_buckets, b)) {
				fatal("Cannot push a bucket to tail");
			}
			++j;
			prob = 0;
			q = floor((1.0 + j) / s->max_num_buckets * l);
		}
	}
}

/** End: APIs **/
