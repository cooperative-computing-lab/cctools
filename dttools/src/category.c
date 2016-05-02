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

static uint64_t first_allocation_every_n_tasks = 50; /* tasks */

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
	c->max_allocation      = rmsummary_create(-1);
	c->autolabel_resource  = rmsummary_create(0);

	c->max_resources_seen      = rmsummary_create(-1);

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

	c->completions_since_last_reset = 0;

	c->allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;

	hash_table_insert(categories, name, c);

	return c;
}

void category_specify_max_allocation(struct category *c, const struct rmsummary *s) {
	rmsummary_delete(c->max_allocation);
	c->max_allocation = rmsummary_create(-1);

	rmsummary_merge_max(c->max_allocation, s);
}

void category_specify_first_allocation_guess(struct category *c, const struct rmsummary *s) {

	/* assume user knows what they are doing. */
	c->completions_since_last_reset = first_allocation_every_n_tasks;

	if(c->first_allocation)
		rmsummary_delete(c->first_allocation);

	c->first_allocation = rmsummary_create(-1);

	rmsummary_merge_max(c->first_allocation, s);
}

/* set autoallocation mode for cores, memory, and disk.  To add other resources see category_enable_auto_resource. */
void category_specify_allocation_mode(struct category *c, int mode) {
	struct rmsummary *r = c->autolabel_resource;

	c->allocation_mode = mode;

	int autolabel = 1;
	if(c->allocation_mode == CATEGORY_ALLOCATION_MODE_FIXED) {
		autolabel = 0;
	}

	r->wall_time      = 0;
	r->cpu_time       = 0;
	r->swap_memory     = 0;
	r->virtual_memory  = 0;
	r->bytes_read      = 0;
	r->bytes_written   = 0;
	r->bytes_received  = 0;
	r->bytes_sent      = 0;
	r->bandwidth       = 0;
	r->total_files     = 0;
	r->total_processes = 0;
	r->max_concurrent_processes = 0;

	r->cores           = autolabel;
	r->memory          = autolabel;
	r->disk            = autolabel;
}

/* set autolabel per resource. */
int category_enable_auto_resource(struct category *c, const char *resource_name, int autolabel) {
	return rmsummary_assign_int_field(c->autolabel_resource, resource_name, autolabel);
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
	rmsummary_delete(c->autolabel_resource);
	rmsummary_delete(c->max_resources_seen);

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

		if(a < 1) {
			continue;
		}

		if(a > top_resource) {
			a_1 = top_resource;
			break;
		}

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

		if(a > top_resource) {
			a_1 = top_resource;
			break;
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

	int64_t alloc;

	switch(mode) {
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			alloc = category_first_allocation_min_waste(histogram, assume_independence, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			alloc = category_first_allocation_max_throughput(histogram, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_FIXED:
		case CATEGORY_ALLOCATION_MODE_MAX:
		default:
			alloc = top_resource;
			break;
	}

	return alloc;
}

#define update_first_allocation_field(c, top, independence, field)\
	if(c->autolabel_resource->field) {\
		(c)->first_allocation->field = category_first_allocation((c)->field##_histogram, independence, (c)->allocation_mode, top->field);\
	}

int category_update_first_allocation(struct category *c, const struct rmsummary *max_worker) {
	/* buffer used only for debug output. */
	static buffer_t *b = NULL;
	if(!b) {
		b = malloc(sizeof(buffer_t));
		buffer_init(b);
	}

	if(c->allocation_mode == CATEGORY_ALLOCATION_MODE_FIXED)
		return 0;

	if(c->total_tasks < 1)
		return 0;

	struct rmsummary *top = rmsummary_create(-1);
	rmsummary_merge_override(top, max_worker);
	rmsummary_merge_override(top, c->max_resources_seen);
	rmsummary_merge_override(top, c->max_allocation);

	if(!c->first_allocation) {
		c->first_allocation = rmsummary_create(-1);
	}

	update_first_allocation_field(c, top, 1, cpu_time);
	update_first_allocation_field(c, top, 1, wall_time);
	update_first_allocation_field(c, top, c->time_peak_independece, cores);
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
		debug(D_DEBUG, "Updating first allocation '%s':", c->name);
		debug(D_DEBUG, "%s", str);
		jx_delete(jsum);
		free(str);
	}

	jsum = rmsummary_to_json(top, 1);
	if(jsum) {
		char *str = jx_print_string(jsum);
		debug(D_DEBUG, "From max resources '%s':", c->name);
		debug(D_DEBUG, "%s", str);
		jx_delete(jsum);
		free(str);
	}

	rmsummary_delete(top);

	return 1;
}


int category_accumulate_summary(struct category *c, const struct rmsummary *rs, const struct rmsummary *max_worker) {

	int update = 0;

	const struct rmsummary *max  = c->max_allocation;
	const struct rmsummary *seen = c->max_resources_seen;

	if(!rs || ((max->cores < 0 && (rs->cores > seen->cores))
				|| (max->memory < 0 && (rs->memory > seen->memory))
				|| (max->disk < 0 && (rs->disk > seen->disk)))) {
		rmsummary_delete(c->first_allocation);
		c->first_allocation =  NULL;
		c->completions_since_last_reset = 0;

		update = 1;
	}

	rmsummary_merge_max(c->max_resources_seen, rs);

	if(rs && (!rs->exit_type || !strcmp(rs->exit_type, "normal"))) {
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

		c->completions_since_last_reset++;

		if(c->completions_since_last_reset % first_allocation_every_n_tasks == 0) {
			update |= category_update_first_allocation(c, max_worker);
		}
	}

	return update;
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
			c = category_lookup_or_create(categories, s->category);
			category_accumulate_summary(c, s, NULL);
		}
		rmsummary_delete(s);
	}

	hash_table_firstkey(categories);
	while(hash_table_nextkey(categories, &name, (void **) &c)) {
		category_update_first_allocation(c, NULL);
		category_clear_histograms(c);
	}
}

#define check_hard_limits(max, user, measured, field, flag)\
	if(!flag) {\
		if(user && user->field > -1) {\
			if(measured->field > user->field) {\
				flag = 1;\
			}\
		}\
		else if(max && max->field > -1) {\
			if(measured->field > max->field) {\
				flag = 1;\
			}\
		}\
	}

/* returns the next allocation state. */
category_allocation_t category_next_label(struct category *c, category_allocation_t current_label, int resource_overflow, struct rmsummary *user, struct rmsummary *measured) {
	if(resource_overflow) {
		/* not autolabeling, so we return error. */
		if(c->allocation_mode ==  CATEGORY_ALLOCATION_MODE_FIXED) {
			return CATEGORY_ALLOCATION_ERROR;
		}

		int over = 0;
		if(measured) {
			check_hard_limits(c->max_allocation, user, measured, cores,                    over);
			check_hard_limits(c->max_allocation, user, measured, cpu_time,                 over);
			check_hard_limits(c->max_allocation, user, measured, wall_time,                over);
			check_hard_limits(c->max_allocation, user, measured, virtual_memory,           over);
			check_hard_limits(c->max_allocation, user, measured, memory,                   over);
			check_hard_limits(c->max_allocation, user, measured, swap_memory,              over);
			check_hard_limits(c->max_allocation, user, measured, bytes_read,               over);
			check_hard_limits(c->max_allocation, user, measured, bytes_written,            over);
			check_hard_limits(c->max_allocation, user, measured, bytes_sent,               over);
			check_hard_limits(c->max_allocation, user, measured, bytes_received,           over);
			check_hard_limits(c->max_allocation, user, measured, bandwidth,                over);
			check_hard_limits(c->max_allocation, user, measured, total_files,              over);
			check_hard_limits(c->max_allocation, user, measured, disk,                     over);
			check_hard_limits(c->max_allocation, user, measured, max_concurrent_processes, over);
			check_hard_limits(c->max_allocation, user, measured, total_processes,          over);
		}

		return over ? CATEGORY_ALLOCATION_ERROR : CATEGORY_ALLOCATION_MAX;
	}

	/* else... not overflow, no label change */

	return current_label;
}

const struct rmsummary *category_dynamic_task_max_resources(struct category *c, struct rmsummary *user, category_allocation_t request) {
	/* we keep an internal label so that the caller does not have to worry
	 * about memory leaks. */
	static struct rmsummary *internal = NULL;

	if(internal) {
		rmsummary_delete(internal);
	}

	internal = rmsummary_create(-1);

	struct rmsummary *max   = c->max_allocation;
	struct rmsummary *first = c->first_allocation;
	struct rmsummary *seen  = c->max_resources_seen;

	if(c->completions_since_last_reset >= first_allocation_every_n_tasks) {
		internal->cores  = seen->cores;
		internal->memory = seen->memory;
		internal->disk   = seen->disk;
	}

	/* load max values */
	rmsummary_merge_override(internal, max);

	if(c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED
			&& request == CATEGORY_ALLOCATION_FIRST) {
		rmsummary_merge_override(internal, first);
	}

	/* chip in user values */
	rmsummary_merge_override(internal, user);

	return internal;
}

const struct rmsummary *category_dynamic_task_min_resources(struct category *c, struct rmsummary *user, category_allocation_t request) {

	static struct rmsummary *internal = NULL;

	const struct rmsummary *max = category_dynamic_task_max_resources(c, user, request);

	if(internal) {
		rmsummary_delete(internal);
	}

	internal = rmsummary_create(-1);

	/* load seen values */
	struct rmsummary *seen = c->max_resources_seen;

	if(c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED) {
			internal->cores  = seen->cores;
			internal->memory = seen->memory;
			internal->disk   = seen->disk;
	}

	rmsummary_merge_override(internal, max);

	return internal;
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
