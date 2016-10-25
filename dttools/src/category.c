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

static uint64_t first_allocation_every_n_tasks = 25; /* tasks */

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

	c->cores_histogram           = histogram_create(1);
	c->wall_time_histogram       = histogram_create(time_bucket_size);
	c->cpu_time_histogram        = histogram_create(time_bucket_size);
	c->memory_histogram          = histogram_create(memory_bucket_size);
	c->swap_memory_histogram     = histogram_create(memory_bucket_size);
	c->virtual_memory_histogram  = histogram_create(memory_bucket_size);
	c->bytes_read_histogram      = histogram_create(bytes_bucket_size);
	c->bytes_written_histogram   = histogram_create(bytes_bucket_size);
	c->bytes_received_histogram  = histogram_create(bytes_bucket_size);
	c->bytes_sent_histogram      = histogram_create(bytes_bucket_size);
	c->bandwidth_histogram       = histogram_create(bandwidth_bucket_size);
	c->total_files_histogram     = histogram_create(1);
	c->disk_histogram            = histogram_create(disk_bucket_size);
	c->total_processes_histogram = histogram_create(1);
	c->max_concurrent_processes_histogram = histogram_create(1);

	c->time_peak_independece = 0;

	c->steady_state = 0;
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
	c->steady_state = 1;
	rmsummary_merge_max(c->max_resources_seen, s);

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

static void category_clear_histogram(struct histogram *h) {
	double *buckets = histogram_buckets(h);

	int i;
	for(i = 0; i < histogram_size(h); i++) {
		double start   = buckets[i];
		void *time_accum = histogram_get_data(h, start);

		if(time_accum) {
			free(time_accum);
		}
	}

	histogram_clear(h);
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

	histogram_delete(c->cores_histogram);
	histogram_delete(c->wall_time_histogram);
	histogram_delete(c->cpu_time_histogram);
	histogram_delete(c->max_concurrent_processes_histogram);
	histogram_delete(c->total_processes_histogram);
	histogram_delete(c->memory_histogram);
	histogram_delete(c->swap_memory_histogram);
	histogram_delete(c->virtual_memory_histogram);
	histogram_delete(c->bytes_read_histogram);
	histogram_delete(c->bytes_written_histogram);
	histogram_delete(c->bytes_received_histogram);
	histogram_delete(c->bytes_sent_histogram);
	histogram_delete(c->bandwidth_histogram);
	histogram_delete(c->total_files_histogram);
	histogram_delete(c->disk_histogram);
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

void category_inc_histogram_count_aux(struct histogram *h, double value, double wall_time) {
	if(value >= 0 && wall_time >= 0) {

		histogram_insert(h, value);
		double *time_accum = (double *) histogram_get_data(h, value);

		if(!time_accum) {
			time_accum = malloc(sizeof(double));
			histogram_attach_data(h, value, time_accum);

			*time_accum = 0;
		}

		// accumulate time (in seconds) for this bucket
		*time_accum += wall_time/USECOND;
	}
}

#define category_inc_histogram_count(c, field, summary)\
{\
	double value        = (summary)->field;\
	double wall_time    = (summary)->wall_time;\
	struct histogram *h = c->field##_histogram;\
	category_inc_histogram_count_aux(h, value, wall_time);\
}

void category_first_allocation_accum_times(struct histogram *h, double *keys, double *tau_mean, double *counts_accum, double *times_accum) {

	int n = histogram_size(h);

	double *times_values = malloc(n*sizeof(double));
	double *counts       = malloc(n*sizeof(double));

	// accumulate counts...
	int i;
	for(i = 0; i < n; i++) {
		int count          = histogram_count(h, keys[i]);
		double *time_value = (double *) histogram_get_data(h, keys[i]);
		counts[i]          = count;
		times_values[i]    = *time_value;
	}
	for(i = 0; i < n; i++) {
		counts_accum[i]  = (i > 0 ? counts_accum[i-1] : 0) + counts[i];
	}

	// compute proportion of mean time for buckets larger than i, for each i.
	for(i = n-1; i >= 0; i--) {
		// base case
		if(i == n-1) {
			times_accum[i] = 0;
		} else {

			/* formula is:
			 * times_accum[i] = times_accum[i+1] + (time_average[i+1] * p(keys[i+1])
			 * with:
			 * time_average[j] = times_accum[j] / counts[j]
			 * p(keys[j])      = counts[j]/counts_accum[n-1]
			 *
			 * which simplifies to:
			 */
			times_accum[i] = times_accum[i+1] + (times_values[i+1] / counts_accum[n-1]);
		}
	}

	// set overall mean time
	*tau_mean = times_accum[0] + (times_values[0] / counts_accum[n-1]);

	free(counts);
	free(times_values);
}

int64_t category_first_allocation_min_waste(struct histogram *h, int assume_independence, int64_t top_resource) {
	/* Automatically labeling for resource is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	int64_t n = histogram_size(h);

	if(n < 1)
		return -1;

	double *keys = histogram_buckets(h);
	double tau_mean;
	double *counts_accum  = malloc(n*sizeof(double));
	double *times_accum = malloc(n*sizeof(double));

	category_first_allocation_accum_times(h, keys, &tau_mean, counts_accum, times_accum);

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

		double Pa = 1 - (counts_accum[i]/counts_accum[n-1]);

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
	free(times_accum);
	free(keys);

	return a_1;
}

int64_t category_first_allocation_max_throughput(struct histogram *h, int64_t top_resource) {
	/* Automatically labeling for resource is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	int64_t n = histogram_size(h);

	if(n < 1)
		return -1;

	double *keys = histogram_buckets(h);
	double tau_mean;
	double *counts_accum = malloc(n*sizeof(double));
	double *times_accum  = malloc(n*sizeof(double));

	category_first_allocation_accum_times(h, keys, &tau_mean, counts_accum, times_accum);

	int64_t a_1 = top_resource;
	int64_t a_m = top_resource;

	double Ta_1 = 0;

	int i;
	for(i = 0; i < n; i++) {
		int64_t a  = keys[i];

		if(a < 1) {
			continue;
		}

		/*
		 * Formula is:
		 * numerator  = (a_m/a) * P(r \le a) + P(r \ge a)
		 * denomintor = time_mean + Sum{r \ge a} time_mean(r) p(r)
		 * argmax_{a_1} = numerator/denominator
		 *
		 * Multiplying by total_count (value of counts_accum[n-1]), the argmax
		 * does not change, but we eliminate two divisons in the numerator:
		 *
		 * numerator  = (a_m/a) * counts_accum(r \le a) + counts_accum(r \ge a)
		 *
		 * which is what we compute below.
		 */

		double Pbef = counts_accum[i];
		double Paft = counts_accum[n-1] - Pbef;

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
	free(times_accum);
	free(keys);

	return a_1;
}

int64_t category_first_allocation(struct histogram *h, int assume_independence, category_mode_t mode,  int64_t top_resource) {

	int64_t alloc;

	switch(mode) {
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			alloc = category_first_allocation_min_waste(h, assume_independence, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			alloc = category_first_allocation_max_throughput(h, top_resource);
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

	int new_maximum ;
	if(rs
			&& (max->cores  > 0 || rs->cores  <= seen->cores)
			&& (max->memory > 0 || rs->memory <= seen->memory)
			&& (max->disk   > 0 || rs->disk   <= seen->disk)) {
		new_maximum = 0;
	} else {
		new_maximum = 1;
	}

	/* a new maximum has been seen, first-allocation is obsolete. */
	if(new_maximum) {
		rmsummary_delete(c->first_allocation);
		c->first_allocation =  NULL;
		c->completions_since_last_reset = 0;
		update = 1;
	}

	c->steady_state = c->completions_since_last_reset >= first_allocation_every_n_tasks;

	rmsummary_merge_max(c->max_resources_seen, rs);
	if(rs && (!rs->exit_type || !strcmp(rs->exit_type, "normal"))) {
		category_inc_histogram_count(c, cores,          rs);
		category_inc_histogram_count(c, cpu_time,       rs);
		category_inc_histogram_count(c, wall_time,      rs);
		category_inc_histogram_count(c, virtual_memory, rs);
		category_inc_histogram_count(c, memory,         rs);
		category_inc_histogram_count(c, swap_memory,    rs);
		category_inc_histogram_count(c, bytes_read,     rs);
		category_inc_histogram_count(c, bytes_written,  rs);
		category_inc_histogram_count(c, bytes_sent,     rs);
		category_inc_histogram_count(c, bytes_received, rs);
		category_inc_histogram_count(c, bandwidth,      rs);
		category_inc_histogram_count(c, total_files,    rs);
		category_inc_histogram_count(c, disk,           rs);
		category_inc_histogram_count(c, max_concurrent_processes, rs);
		category_inc_histogram_count(c, total_processes,rs);

		c->completions_since_last_reset++;

		if(new_maximum || c->completions_since_last_reset % first_allocation_every_n_tasks == 0) {
			update |= category_update_first_allocation(c, max_worker);
		}

		/* a task completed using a new maximum, so we consider that the new steady_state. */
		if(new_maximum) {
			c->steady_state = 1;
		}

        c->total_tasks++;
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

	if(c->steady_state && c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED) {
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

int category_in_steady_state(struct category *c) {
	return c->first_allocation != NULL;
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
	} else if(strcmp(resource, "category-steady-n-tasks") == 0) {
		first_allocation_every_n_tasks = size;
	}
}
