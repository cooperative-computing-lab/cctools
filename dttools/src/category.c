/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <math.h>

#include "buffer.h"
#include "debug.h"
#include "itable.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "category.h"

struct category *category_lookup_or_create(struct hash_table *categories, const char *name) {
	struct category *c;

	if(!name)
		name = "default";

	c = hash_table_lookup(categories, name);
	if(c) return c;

	c = calloc(1, sizeof(struct category));

	c->name       = xxstrdup(name);
	c->fast_abort = -1;

	c->total_tasks_complete     = 0;
	c->total_good_execute_time  = 0;
	c->total_good_transfer_time = 0;

	/* autolabeling enabled by default if work_queue_specify_max_worker_resources is used. */
	c->disable_auto_labeling = 0;

	c->first    = make_rmsummary(-1);

	c->cores_histogram     = itable_create(0);
	c->memory_histogram    = itable_create(0);
	c->disk_histogram      = itable_create(0);
	c->wall_time_histogram = itable_create(0);

	hash_table_insert(categories, name, c);

	return c;
}

void category_delete(struct hash_table *categories, const char *name) {
	struct category *c = hash_table_lookup(categories, name);

	if(!c)
		return;

	hash_table_remove(categories, name);

	if(c->name)
		free(c->name);

	itable_delete(c->cores_histogram);
	itable_delete(c->memory_histogram);
	itable_delete(c->disk_histogram);
	itable_delete(c->wall_time_histogram);

	free(c);
}

/* histograms keys are shifted to the right, as 0 cannot be a valid key (thus the bucket + 1). */
#define category_inc_histogram_count(q, c, field, value, bucket_size)\
{\
	if(value >= 0) { \
		uintptr_t bucket = DIV_INT_ROUND_UP(value, bucket_size)*bucket_size;\
		uintptr_t count = (uintptr_t) itable_lookup(c->field##_histogram, bucket + 1) + 1;\
		itable_insert(c->field##_histogram, bucket + 1, (void *) count);\
	}\
}

int cmp_int(const void *a, const void *b) {
	return (*((int64_t *) a) - *((int64_t *) b));
}

int64_t *category_sort_histogram(struct itable *histogram) {
	if(itable_size(histogram) < 1) {
		return NULL;
	}

	int64_t *buckets = malloc(itable_size(histogram)*sizeof(int64_t));

	size_t i = 0;
	uint64_t  key;
	uintptr_t count;
	itable_firstkey(histogram);
	while(itable_nextkey(histogram, &key, (void **) &count)) {
		/* histograms keys are shifted to the right, as 0 cannot be a valid key. */
		buckets[i] = key - 1;
		i++;
	}

	qsort(buckets, itable_size(histogram), sizeof(int64_t), cmp_int);

	return buckets;
}

int64_t category_first_allocation(struct itable *histogram, int64_t top_resource) {
	/* Automatically labeling for memory is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	uint64_t n = itable_size(histogram);

	int64_t *buckets = category_sort_histogram(histogram);
	uintptr_t *accum = malloc(n*sizeof(uintptr_t));

	/* histograms keys are shifted to the right, thus the bucket - 1. */
	accum[0] = (uintptr_t) itable_lookup(histogram, buckets[0] + 1);

	uint64_t i;
	for(i = 1; i < n; i++) {
		accum[i] = accum[i - 1] + (uintptr_t) itable_lookup(histogram, buckets[i] + 1);
	}

	uint64_t a_1 = top_resource;
	uint64_t a_m = top_resource;
	int64_t Ea_1 = INT64_MAX;

	for(i = 0; i < n; i++) {
		uint64_t a  = buckets[i];
		uint64_t Pa = accum[n-1] - accum[i];
		int64_t  Ea = a*accum[n-1] + a_m*Pa;

		if(Ea < Ea_1) {
			Ea_1 = Ea;
			a_1 = a;
		}
	}

	if(a_1 > ceil(top_resource / 2.0)) {
		a_1 = top_resource;
	}

	free(accum);
	free(buckets); /* of popcorn! */

	return a_1;
}

void category_update_first_allocation(struct hash_table *categories, const char *category, struct rmsummary *top) {

	/* buffer used only for debug output. */
	static buffer_t *b = NULL;
	if(!b) {
		b = malloc(sizeof(buffer_t));
		buffer_init(b);
	}

	struct category *c = category_lookup_or_create(categories, category);

	int64_t cores     = category_first_allocation(c->cores_histogram,     top->cores);
	int64_t memory    = category_first_allocation(c->memory_histogram,    top->memory);
	int64_t disk      = category_first_allocation(c->disk_histogram,      top->disk);
	int64_t wall_time = category_first_allocation(c->wall_time_histogram, top->wall_time);

	/* Update values, and print debug message only if something changed. */
	if(cores != c->first->cores ||  memory != c->first->memory || disk != c->first->disk || wall_time != c->first->wall_time) {
		c->first->cores     = cores;
		c->first->memory    = memory;
		c->first->disk      = disk;
		c->first->wall_time = wall_time;

		/* From here on we only print debugging info. */
		buffer_rewind(b, 0);
		buffer_printf(b, "Updating first allocation '%s':", category);

		if(cores > -1) {
			buffer_printf(b, " cores: %" PRId64, cores);
		}

		if(memory > -1) {
			buffer_printf(b, " memory: %" PRId64 " MB", memory);
		}

		if(disk > -1) {
			buffer_printf(b, " disk: %" PRId64 " MB", disk);
		}

		if(wall_time > -1) {
			buffer_printf(b, " wall_time: %" PRId64 " us", wall_time);
		}

		debug(D_DEBUG, "%s\n", buffer_tostring(b));
	}
}


void category_accumulate_summary(struct hash_table *categories, const char *category, struct rmsummary *rs) {
	/* buffer used only for debug output. */
	static buffer_t *b = NULL;
	if(!b) {
		b = malloc(sizeof(buffer_t));
		buffer_init(b);
	}

	const char *name = category ? category : "default";

	struct category *c = hash_table_lookup(categories, name);

	if(!c->disable_auto_labeling && rs) {
		category_inc_histogram_count(q, c, cores,     rs->cores,      1);
		category_inc_histogram_count(q, c, memory,    rs->memory,    25);
		category_inc_histogram_count(q, c, disk,      rs->disk,      25);
		category_inc_histogram_count(q, c, wall_time, rs->wall_time, 30000000);
	}
}
