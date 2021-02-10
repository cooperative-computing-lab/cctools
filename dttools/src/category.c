/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <float.h>

#include "assert.h"
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

static int64_t first_allocation_every_n_tasks = 25; /* tasks */

static const char *resources[] = {"cores", "memory", "disk", "gpus", NULL};

/* map from resoure name to int bucket size. Initialized in category_create first time it is called. */
static struct rmsummary *bucket_sizes = NULL;

struct category *category_create(const char *name) {
	if(!name)
		name = "default";

	struct category *c = calloc(1, sizeof(struct category));

	c->name       = xxstrdup(name);
	c->fast_abort = -1;

	c->total_tasks = 0;

	c->first_allocation    = NULL;
	c->max_allocation      = rmsummary_create(-1);
	c->min_allocation      = rmsummary_create(-1);
	c->autolabel_resource  = rmsummary_create(0);

    c->time_peak_independece = 0;

	c->max_resources_seen = rmsummary_create(-1);

    c->histograms = hash_table_create(0,0);

    if(!bucket_sizes) {
        bucket_sizes = rmsummary_create(-1);
        bucket_sizes->cores = 1;
        bucket_sizes->gpus  = 1;
        bucket_sizes->memory = 250;   /* 250 MB */
        bucket_sizes->disk = 250;     /* 250 MB */
    }

    size_t i;
    for(i = 0; resources[i]; i++) {
        const char *r = resources[i];

        int64_t bucket_size = rmsummary_get_int_field(bucket_sizes, r);
        assert(bucket_size > 0);

        hash_table_insert(c->histograms, r, histogram_create(bucket_size));
    }

	c->steady_state = 0;
	c->completions_since_last_reset = 0;

	c->allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;

    return c;
}

struct category *category_lookup_or_create(struct hash_table *categories, const char *name) {
	struct category *c;

	if(!name)
		name = "default";

	c = hash_table_lookup(categories, name);
	if(c) return c;

    c = category_create(name);
	hash_table_insert(categories, name, c);

	return c;
}

void category_specify_max_allocation(struct category *c, const struct rmsummary *s) {
	rmsummary_delete(c->max_allocation);
	c->max_allocation = rmsummary_create(-1);

	rmsummary_merge_max(c->max_allocation, s);
}

void category_specify_min_allocation(struct category *c, const struct rmsummary *s) {
	rmsummary_delete(c->min_allocation);
	c->min_allocation = rmsummary_create(-1);

	rmsummary_merge_max(c->min_allocation, s);

    /* consider the minimum allocation as a measurement. This ensures that max
     * dynamic allocation is never below min dynamic allocation */
	rmsummary_merge_max(c->max_resources_seen, s);
}

void category_specify_first_allocation_guess(struct category *c, const struct rmsummary *s) {

	/* assume user knows what they are doing. */
	c->steady_state = 1;
	rmsummary_merge_max(c->max_resources_seen, s);

	rmsummary_delete(c->first_allocation);

	c->first_allocation = rmsummary_create(-1);

	rmsummary_merge_max(c->first_allocation, s);
}

/* set autoallocation mode for cores, memory, and disk.  See category_enable_auto_resource to disable per resource. */
void category_specify_allocation_mode(struct category *c, int mode) {
	struct rmsummary *r = c->autolabel_resource;

	c->allocation_mode = mode;

	int autolabel = 1;
	if(c->allocation_mode == CATEGORY_ALLOCATION_MODE_FIXED) {
		autolabel = 0;
	}

	r->cores     = autolabel;
	r->memory    = autolabel;
	r->disk      = autolabel;
    r->gpus      = 0;
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

    size_t i;
    for(i = 0; resources[i]; i++) {
        const char *r = resources[i];

        struct histogram *h = hash_table_lookup(c->histograms, r);
        assert(h);

        category_clear_histogram(h);
    }
}

static void category_delete_histograms(struct category *c) {
	if(!c)
		return;

	category_clear_histograms(c);

    size_t i;
    for(i = 0; resources[i]; i++) {
        const char *r = resources[i];

        struct histogram *h = hash_table_lookup(c->histograms, r);
        assert(h);

        histogram_delete(h);
    }
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
	rmsummary_delete(c->min_allocation);
	rmsummary_delete(c->first_allocation);
	rmsummary_delete(c->autolabel_resource);
	rmsummary_delete(c->max_resources_seen);

	free(c);
}

void category_inc_histogram_count(struct histogram *h, double value, double wall_time) {
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

    size_t i;
    for(i = 0; resources[i]; i++) {
        const char *r = resources[i];

        int64_t should_update = rmsummary_get_int_field(c->autolabel_resource, r);
        if(should_update) {
            struct histogram *h = hash_table_lookup(c->histograms, r);
            assert(h);

            int64_t top_value = rmsummary_get_int_field(top, r);
            int64_t new_value = category_first_allocation(h, /* assume time independence */ 1, c->allocation_mode, top_value);

            rmsummary_assign_int_field(c->first_allocation, r, new_value);
        }
    }

    /* don't go below min allocation */
    rmsummary_merge_max(c->first_allocation, c->min_allocation);

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

    int new_maximum = 0;
    if(rs && (max->cores  > 0 || rs->cores  <= seen->cores)
          && (max->memory > 0 || rs->memory <= seen->memory)
          && (max->disk > 0 || rs->disk <= seen->disk)
          && (max->gpus   > 0 || rs->gpus   <= seen->gpus)) {
        new_maximum = 0;
    } else {
        if(c->steady_state) {
            /* count new maximums only in steady state. */
            new_maximum = 1;
        }
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
        size_t i;
        for(i = 0; resources[i]; i++) {
            const char *r = resources[i];

            struct histogram *h = hash_table_lookup(c->histograms, r);
            assert(h);

            double value = rmsummary_get_int_field(rs, r);
            double wall_time = rmsummary_get_int_field(rs, "wall_time");

            category_inc_histogram_count(h, value, wall_time);
        }

		c->completions_since_last_reset++;

        if(first_allocation_every_n_tasks > 0) {
            if(c->completions_since_last_reset % first_allocation_every_n_tasks == 0) {
                update |= category_update_first_allocation(c, max_worker);
            }
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


/* returns the next allocation state. */
category_allocation_t category_next_label(struct category *c, category_allocation_t current_label, int resource_overflow, struct rmsummary *user, struct rmsummary *measured) {
	if(resource_overflow) {
		/* not autolabeling, so we return error. */
		if(c->allocation_mode ==  CATEGORY_ALLOCATION_MODE_FIXED) {
			return CATEGORY_ALLOCATION_ERROR;
		}

        /* We check per resource if the measured allocation went over the
         * maximum user specified per task or per category.
         * If so, we return error, as there is nothing else we can do.
         * Otherwise, we go to the maximum allocation. */
		int over = 0;
        if(measured) {
            size_t i;
            for(i = 0; resources[i]; i++) {
                const char *r = resources[i];
                if(!over) {
                    int64_t meas_value = rmsummary_get_int_field(measured, r);
                    if(user) {
                        int64_t user_value = rmsummary_get_int_field(user, r);
                        if(user_value > -1 && meas_value > user_value) {
                            over = 1;
                        }
                    } else if(c->max_allocation) {
                        int64_t max_value = rmsummary_get_int_field(c->max_allocation, r);
                        if(max_value > -1 && meas_value > max_value) {
                            over = 1;
                        }
                    }
                }
            }
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

    if(category_in_steady_state(c) && c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED
            && request == CATEGORY_ALLOCATION_FIRST) {
		rmsummary_merge_override(internal, first);
	}

	/* chip in user values if explicitely given */
	rmsummary_merge_override(internal, user);

	return internal;
}

const struct rmsummary *category_dynamic_task_min_resources(struct category *c, struct rmsummary *user, category_allocation_t request) {

	static struct rmsummary *internal = NULL;

	const struct rmsummary *allocation = category_dynamic_task_max_resources(c, user, request);

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

    /* prefer first allocation (if available) to maximum seen. */
	rmsummary_merge_override(internal, allocation);

    /* but don't go below the minimum defined for the category. */
	rmsummary_merge_max(internal, c->min_allocation);

	return internal;
}

int category_in_steady_state(struct category *c) {
	return c->steady_state;
}

void category_tune_bucket_size(const char *resource, int64_t size) {
    if(strcmp(resource, "category-steady-n-tasks") == 0) {
        first_allocation_every_n_tasks = size;
    } else {
        rmsummary_assign_int_field(bucket_sizes, resource, size);
    }
}

int64_t category_get_bucket_size(const char *resource) {
    if(strcmp(resource, "category-steady-n-tasks") == 0) {
        return first_allocation_every_n_tasks;
    } else {
        return rmsummary_get_int_field(bucket_sizes, resource);
    }
}
