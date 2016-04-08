/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <float.h>

#include "buffer.h"
#include "debug.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx_print.h"

#include "category_internal.h"

struct peak_count_time {
	int64_t count;
	double  times;
};

static uint64_t memory_bucket_size    = 50;        /* MB */
static uint64_t disk_bucket_size      = 50;        /* MB */
static uint64_t time_bucket_size      = 60000000;  /* 1 minute */
static uint64_t bytes_bucket_size     = MEGABYTE;  /* 1 M */
static uint64_t bandwidth_bucket_size = 1000000;   /* 1 Mbit/s */

struct category *category_lookup_or_create(struct hash_table *categories, const char *name) {
	struct category *c;

	if(!name)
		name = "default";

	c = hash_table_lookup(categories, name);
	if(c) return c;

	c = calloc(1, sizeof(struct category));

	c->name       = xxstrdup(name);
	c->fast_abort = -1;

	c->total_tasks = 0;

	c->first_allocation    = NULL;
	c->max_allocation      = NULL;

	c->cores_histogram          = itable_create(0);
	c->wall_time_histogram      = itable_create(0);
	c->cpu_time_histogram       = itable_create(0);
	c->memory_histogram         = itable_create(0);
	c->swap_memory_histogram    = itable_create(0);
	c->virtual_memory_histogram = itable_create(0);
	c->bytes_read_histogram     = itable_create(0);
	c->bytes_written_histogram  = itable_create(0);
	c->bytes_received_histogram = itable_create(0);
	c->bytes_sent_histogram     = itable_create(0);
	c->bandwidth_histogram      = itable_create(0);
	c->total_files_histogram    = itable_create(0);
	c->disk_histogram           = itable_create(0);
	c->max_concurrent_processes_histogram = itable_create(0);
	c->total_processes_histogram = itable_create(0);

	c->time_peak_independece = 0;

	c->allocation_mode = CATEGORY_ALLOCATION_MODE_MAX;

	hash_table_insert(categories, name, c);

	return c;
}

static void category_clear_histogram(struct itable *h) {
	struct peak_count_time *p;
	uint64_t key;

	itable_firstkey(h);
	while(itable_nextkey(h, &key, (void **) &p)) {
		free(p);
	}

	itable_clear(h);
}

static void category_clear_histograms(struct category *c) {
	if(!c)
		return;

	category_clear_histogram(c->cores_histogram);
	category_clear_histogram(c->wall_time_histogram);
	category_clear_histogram(c->cpu_time_histogram);
	category_clear_histogram(c->max_concurrent_processes_histogram);
	category_clear_histogram(c->total_processes_histogram);
	category_clear_histogram(c->memory_histogram);
	category_clear_histogram(c->swap_memory_histogram);
	category_clear_histogram(c->virtual_memory_histogram);
	category_clear_histogram(c->bytes_read_histogram);
	category_clear_histogram(c->bytes_written_histogram);
	category_clear_histogram(c->bytes_received_histogram);
	category_clear_histogram(c->bytes_sent_histogram);
	category_clear_histogram(c->bandwidth_histogram);
	category_clear_histogram(c->total_files_histogram);
	category_clear_histogram(c->disk_histogram);
}

static void category_delete_histograms(struct category *c) {
	if(!c)
		return;

	category_clear_histograms(c);

	itable_delete(c->cores_histogram);
	itable_delete(c->wall_time_histogram);
	itable_delete(c->cpu_time_histogram);
	itable_delete(c->max_concurrent_processes_histogram);
	itable_delete(c->total_processes_histogram);
	itable_delete(c->memory_histogram);
	itable_delete(c->swap_memory_histogram);
	itable_delete(c->virtual_memory_histogram);
	itable_delete(c->bytes_read_histogram);
	itable_delete(c->bytes_written_histogram);
	itable_delete(c->bytes_received_histogram);
	itable_delete(c->bytes_sent_histogram);
	itable_delete(c->bandwidth_histogram);
	itable_delete(c->total_files_histogram);
	itable_delete(c->disk_histogram);
}

void category_delete(struct hash_table *categories, const char *name) {
	struct category *c = hash_table_lookup(categories, name);

	if(!c)
		return;

	hash_table_remove(categories, name);

	if(c->name)
		free(c->name);

	if(c->wq_stats)
		free(c->wq_stats);

	category_delete_histograms(c);

	rmsummary_delete(c->max_allocation);
	rmsummary_delete(c->first_allocation);

	free(c);
}

#define category_inc_histogram_count(c, field, summary, bucket_size)\
{\
	int64_t value    = (summary)->field;\
	int64_t walltime = (summary)->wall_time;\
	if(value >= 0 && walltime >= 0) { \
		uintptr_t bucket = DIV_INT_ROUND_UP(value, bucket_size)*bucket_size;\
		struct peak_count_time *p = itable_lookup(c->field##_histogram, bucket);\
		if(!p) { p = malloc(sizeof(*p)); p->count = 0; p->times = 0; itable_insert(c->field##_histogram, bucket, p);}\
		p->count++;\
		p->times += ((double) walltime)/time_bucket_size;\
	}\
}

int cmp_int(const void *a, const void *b) {
	int64_t va = *((int64_t *) a);
	int64_t vb = *((int64_t *) b);

	if(va > vb)
		return 1;

	if(vb > va)
		return -1;

	return 0;
}

int64_t *category_sort_histogram(struct itable *histogram, int64_t *keys) {
	if(itable_size(histogram) < 1) {
		return NULL;
	}

	size_t i = 0;
	uint64_t  key;
	struct peak_count_time *p;
	itable_firstkey(histogram);
	while(itable_nextkey(histogram, &key, (void **) &p)) {
		/* histograms keys are shifted to the right, as 0 cannot be a valid key. */
		keys[i] = key;
		i++;
	}

	qsort(keys, itable_size(histogram), sizeof(int64_t), cmp_int);

	return keys;
}

void category_first_allocation_accum_times(struct itable *histogram, double *tau_mean, int64_t *keys, int64_t *counts_accum, double *times_mean, double *times_accum) {

	int64_t n = itable_size(histogram);

	category_sort_histogram(histogram, keys);

	struct peak_count_time *p;
	p = itable_lookup(histogram, keys[0]);
	counts_accum[0] = p->count;
	times_mean[0]   = (((double) time_bucket_size)/USECOND)*(p->times/p->count);

	int64_t i;
	for(i = 1; i < n; i++) {
		p = itable_lookup(histogram, keys[i]);
		counts_accum[i] = counts_accum[i - 1] + p->count;
		times_mean[i]   = (((double) time_bucket_size)/USECOND)*(p->times/p->count);
	}

	p = itable_lookup(histogram, keys[n-1]);
	times_accum[n-1]  = 0;

	for(i = n-2; i >= 0; i--) {
		p = itable_lookup(histogram, keys[i+1]);
		times_accum[i] = times_accum[i + 1] + times_mean[i+1] * ((double) p->count)/counts_accum[n-1];
	}

	p = itable_lookup(histogram, keys[0]);
	*tau_mean = times_accum[0] + times_mean[0] * ((double) p->count)/counts_accum[n-1];

	return;
}

int64_t category_first_allocation_min_waste(struct itable *histogram, int assume_independence, int64_t top_resource) {
	/* Automatically labeling for resource is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	int64_t n = itable_size(histogram);

	if(n < 1)
		return -1;

	int64_t *keys         = malloc(n*sizeof(intptr_t));
	int64_t *counts_accum = malloc(n*sizeof(intptr_t));
	double  *times_mean   = malloc(n*sizeof(intptr_t));
	double  *times_accum  = malloc(n*sizeof(intptr_t));

	double tau_mean;

	category_first_allocation_accum_times(histogram, &tau_mean, keys, counts_accum, times_mean, times_accum);

	int64_t a_1 = top_resource;
	int64_t a_m = top_resource;

	double Ea_1 = DBL_MAX;

	int i;
	for(i = 0; i < n; i++) {
		int64_t a  = keys[i];
		double  Ea;

		double Pa = 1 - ((double) counts_accum[i])/counts_accum[n-1];

		if(assume_independence) {
			Ea = a + a_m*Pa;
			Ea *= tau_mean;
		} else {
			Ea = a*tau_mean + a_m*times_accum[i];
		}

		if(Ea < Ea_1) {
			Ea_1 = Ea;
			a_1 = a;
		}
	}

	if(a_1 > top_resource) {
		a_1 = top_resource;
	}

	free(counts_accum);
	free(times_mean);
	free(times_accum);
	free(keys);

	return a_1;
}

int64_t category_first_allocation_max_throughput(struct itable *histogram, int64_t top_resource) {
	/* Automatically labeling for resource is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	int64_t n = itable_size(histogram);

	if(n < 1)
		return -1;

	int64_t *keys         = malloc(n*sizeof(intptr_t));
	int64_t *counts_accum = malloc(n*sizeof(intptr_t));
	double  *times_mean   = malloc(n*sizeof(intptr_t));
	double  *times_accum  = malloc(n*sizeof(intptr_t));

	double tau_mean;

	category_first_allocation_accum_times(histogram, &tau_mean, keys, counts_accum, times_mean, times_accum);

	int64_t a_1 = top_resource;
	int64_t a_m = top_resource;

	double Ta_1 = 0;

	int i;
	for(i = 0; i < n; i++) {
		int64_t a  = keys[i];

		if(a < 1) {
			continue;
		}

		double Pbef = ((double) counts_accum[i])/counts_accum[n-1];
		double Paft = 1 - Pbef;

		double numerator   = (Pbef*a_m)/a + Paft;
		double denominator = tau_mean + times_accum[i];

		double  Ta = numerator/denominator;


		if(Ta > Ta_1) {
			Ta_1 = Ta;
			a_1 = a;
		}
	}

	if(a_1 > top_resource) {
		a_1 = top_resource;
	}

	free(counts_accum);
	free(times_mean);
	free(times_accum);
	free(keys);

	return a_1;
}

int64_t category_first_allocation(struct itable *histogram, int assume_independence, category_mode_t mode,  int64_t top_resource) {
	switch(mode) {
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			return category_first_allocation_min_waste(histogram, assume_independence, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			return category_first_allocation_max_throughput(histogram, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX:
		default:
			return top_resource;
			break;
	}
}

#define update_first_allocation_field(c, top, independence, field)\
	(c)->first_allocation->field = category_first_allocation((c)->field##_histogram, independence, (c)->allocation_mode, top->field)

void category_update_first_allocation(struct hash_table *categories, const char *category) {
	/* buffer used only for debug output. */
	static buffer_t *b = NULL;
	if(!b) {
		b = malloc(sizeof(buffer_t));
		buffer_init(b);
	}

	struct category *c = category_lookup_or_create(categories, category);
	struct rmsummary *top = c->max_allocation;

	if(!top)
		return;

	if(!c->first_allocation) {
		c->first_allocation = rmsummary_create(-1);
	}

	c->first_allocation->cores = c->max_allocation->cores;

	update_first_allocation_field(c, top, 1, cpu_time);
	update_first_allocation_field(c, top, 1, wall_time);
	update_first_allocation_field(c, top, c->time_peak_independece, virtual_memory);
	update_first_allocation_field(c, top, c->time_peak_independece, memory);
	update_first_allocation_field(c, top, c->time_peak_independece, swap_memory);
	update_first_allocation_field(c, top, c->time_peak_independece, bytes_read);
	update_first_allocation_field(c, top, c->time_peak_independece, bytes_written);
	update_first_allocation_field(c, top, c->time_peak_independece, bytes_received);
	update_first_allocation_field(c, top, c->time_peak_independece, bytes_sent);
	update_first_allocation_field(c, top, c->time_peak_independece, bandwidth);
	update_first_allocation_field(c, top, c->time_peak_independece, total_files);
	update_first_allocation_field(c, top, c->time_peak_independece, disk);
	update_first_allocation_field(c, top, c->time_peak_independece, max_concurrent_processes);
	update_first_allocation_field(c, top, c->time_peak_independece, total_processes);

	/* From here on we only print debugging info. */
	struct jx *jsum = rmsummary_to_json(c->first_allocation, 1);

	if(jsum) {
		char *str = jx_print_string(jsum);
		debug(D_DEBUG, "Updating first allocation '%s':", category);
		debug(D_DEBUG, "%s", str);
		jx_delete(jsum);
		free(str);
	}
}


void category_accumulate_summary(struct hash_table *categories, const char *category, struct rmsummary *rs) {
	const char *name = category ? category : "default";

	struct category *c = category_lookup_or_create(categories, name);

	if(rs) {
		category_inc_histogram_count(c, cores,          rs, 1);
		category_inc_histogram_count(c, cpu_time,       rs, time_bucket_size);
		category_inc_histogram_count(c, wall_time,      rs, time_bucket_size);
		category_inc_histogram_count(c, virtual_memory, rs, memory_bucket_size);
		category_inc_histogram_count(c, memory,         rs, memory_bucket_size);
		category_inc_histogram_count(c, swap_memory,    rs, memory_bucket_size);
		category_inc_histogram_count(c, bytes_read,     rs, bytes_bucket_size);
		category_inc_histogram_count(c, bytes_written,  rs, bytes_bucket_size);
		category_inc_histogram_count(c, bytes_sent,     rs, bytes_bucket_size);
		category_inc_histogram_count(c, bytes_received, rs, bytes_bucket_size);
		category_inc_histogram_count(c, bandwidth,      rs, bandwidth_bucket_size);
		category_inc_histogram_count(c, total_files,    rs, 1);
		category_inc_histogram_count(c, disk,           rs, disk_bucket_size);
		category_inc_histogram_count(c, max_concurrent_processes, rs, 1);
		category_inc_histogram_count(c, total_processes,rs, 1);
	}

}

void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file) {
	struct list *summaries = rmsummary_parse_file_multiple(summaries_file);

	if(!summaries) {
		fatal("Could not read '%s' file: %s\n", strerror(errno));
	}


	char *name;
	struct category *c;
	hash_table_firstkey(categories);
	while(hash_table_nextkey(categories, &name, (void **) &c)) {
		category_clear_histograms(c);
		if(c->first_allocation) {
			rmsummary_delete(c->first_allocation);
			c->first_allocation = rmsummary_create(-1);
		}
	}

	struct rmsummary *s;
	list_first_item(summaries);
	while((s = list_pop_head(summaries))) {
		if(s->category) {
			category_accumulate_summary(categories, s->category, s);
		}
		rmsummary_delete(s);
	}

	hash_table_firstkey(categories);
	while(hash_table_nextkey(categories, &name, (void **) &c)) {
		category_update_first_allocation(categories, name);
		category_clear_histograms(c);
	}
}

/* returns the next allocation state. */
category_allocation_t category_next_label(struct hash_table *categories, const char *category, category_allocation_t current_label, int resource_overflow) {

	if(resource_overflow) {
		if(current_label == CATEGORY_ALLOCATION_USER || current_label == CATEGORY_ALLOCATION_UNLABELED || current_label == CATEGORY_ALLOCATION_AUTO_MAX) {
			return CATEGORY_ALLOCATION_ERROR;
		} else {
			return CATEGORY_ALLOCATION_AUTO_MAX;
		}
	}

	/* If user specified resources manually, respect the label. */
	if(current_label == CATEGORY_ALLOCATION_USER) {
			return CATEGORY_ALLOCATION_USER;
	}

	struct category *c = category_lookup_or_create(categories, category);
	/* If category is not labeling, and user is not labeling, return unlabeled. */
	if(!c->max_allocation) {
		return CATEGORY_ALLOCATION_UNLABELED;
	}

	/* Never downgrade max allocation */
	if(current_label == CATEGORY_ALLOCATION_AUTO_MAX) {
		return CATEGORY_ALLOCATION_AUTO_MAX;
	}

	if(c->first_allocation) {
		/* Use first allocation when it is available. */
		return CATEGORY_ALLOCATION_AUTO_FIRST;
	} else {
		/* Use default when no enough information is available. */
		return CATEGORY_ALLOCATION_AUTO_ZERO;
	}
}

void category_tune_bucket_size(const char *resource, uint64_t size) {
	if(strcmp(resource, "memory") == 0) {
		memory_bucket_size = size;
	} else if(strcmp(resource, "disk") == 0) {
		disk_bucket_size = size;
	} else if(strcmp(resource, "time") == 0) {
		time_bucket_size = size;
	} else if(strcmp(resource, "io") == 0) {
		bytes_bucket_size = size;
	} else if(strcmp(resource, "bandwidth") == 0) {
		bandwidth_bucket_size = size;
	}
}
