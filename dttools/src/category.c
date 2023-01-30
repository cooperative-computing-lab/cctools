/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <float.h>
#include <stddef.h>

#include "assert.h"
#include "buffer.h"
#include "debug.h"
#include "itable.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "jx_print.h"
#include "bucketing_manager.h"

#include "category_internal.h"

struct peak_count_time {
	int64_t count;
	double  times;
};

static int64_t first_allocation_every_n_tasks = 25; /* tasks */

static const size_t labeled_resources[] = {
    offsetof(struct rmsummary, cores),
    offsetof(struct rmsummary, gpus),
    offsetof(struct rmsummary, memory),
    offsetof(struct rmsummary, disk),
    0
};

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

	c->max_resources_seen = rmsummary_create(-1);

    c->histograms = itable_create(0);

    if(!bucket_sizes) {
        bucket_sizes = rmsummary_create(-1);
        bucket_sizes->cores  = 1;
        bucket_sizes->gpus   = 1;
        bucket_sizes->memory = 250; /* 250 MB */
        bucket_sizes->disk   = 250; /* 250 MB */
    }

    size_t i;
    for(i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];

        int64_t bucket_size = rmsummary_get_by_offset(bucket_sizes, o);
        assert(bucket_size > 0);

        itable_insert(c->histograms, o, histogram_create(bucket_size));
    }

	c->steady_state = 0;
	c->completions_since_last_reset = 0;

	c->allocation_mode = CATEGORY_ALLOCATION_MODE_FIXED;

    c->bucketing_manager = NULL;

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

int category_in_bucketing_mode(struct category* c)
{
    if (c->allocation_mode == CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING ||
        c->allocation_mode == CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING)
        return 1;
    return 0;
}

/* set autoallocation mode for cores, memory, and disk.  See category_enable_auto_resource to disable per resource. */
void category_specify_allocation_mode(struct category *c, int mode) {
	c->allocation_mode = mode;

	int autolabel = 1;
	if(c->allocation_mode == CATEGORY_ALLOCATION_MODE_FIXED) {
		autolabel = 0;
	}

    if (category_in_bucketing_mode(c))
    {
        if (!c->bucketing_manager)
        {
            bucketing_mode_t bmode = c->allocation_mode == CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING ? BUCKETING_MODE_GREEDY : BUCKETING_MODE_EXHAUSTIVE;
            c->bucketing_manager = bucketing_manager_initialize(bmode);
        }
    }

    c->autolabel_resource->cores  = autolabel;
    c->autolabel_resource->memory = autolabel;
    c->autolabel_resource->disk   = autolabel;
    c->autolabel_resource->gpus   = 0;
}

/* set autolabel per resource. */
int category_enable_auto_resource(struct category *c, const char *resource_name, int autolabel) {
	return rmsummary_set(c->autolabel_resource, resource_name, autolabel);
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
    for(i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];

        struct histogram *h = itable_lookup(c->histograms, o);
        assert(h);

        category_clear_histogram(h);
    }
}

static void category_delete_histograms(struct category *c) {
	if(!c)
		return;

	category_clear_histograms(c);

    size_t i;
    for(i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];

        struct histogram *h = itable_lookup(c->histograms, o);
        assert(h);

        histogram_delete(h);
    }

    itable_delete(c->histograms);
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

	if(c->vine_stats)
        free(c->vine_stats);

    if(c->bucketing_manager)
    {
        bucketing_manager_delete(c->bucketing_manager);
    }

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

int64_t category_first_allocation_min_waste(struct histogram *h, int64_t top_resource) {
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

        Ea = a*tau_mean + a_m*times_accum[i];

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

int64_t category_first_allocation_max_seen(struct histogram *h, int64_t top_resource, int64_t max_worker, int64_t max_explicit) {
	/* Automatically labeling for resource is not activated. */
	if(top_resource < 0) {
		return -1;
	}

	int64_t n = histogram_size(h);
	if(n < 1) {
		return -1;
    }

    double max_seen = histogram_max_value(h);
    double rounded = max_seen;
    double bucket_size = histogram_bucket_size(h);

    rounded = histogram_round_up(h, rounded + floor(bucket_size/3));

    double to_cmp = -1;
    if(max_explicit > -1 && max_worker > -1) {
        to_cmp = MIN(max_explicit, max_worker);
    } else if(max_explicit > -1) {
        to_cmp = max_explicit;
    } else if(max_worker > -1) {
        to_cmp = max_worker;
    }

    if(to_cmp > -1) {
        return MIN(rounded, to_cmp);
    } else {
        return rounded;
    }
}

int64_t category_first_allocation(struct histogram *h, category_mode_t mode, int64_t top_resource, int64_t max_worker, int64_t max_explicit) {

	int64_t alloc;

	switch(mode) {
		case CATEGORY_ALLOCATION_MODE_MIN_WASTE:
			alloc = category_first_allocation_min_waste(h, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT:
			alloc = category_first_allocation_max_throughput(h, top_resource);
			break;
		case CATEGORY_ALLOCATION_MODE_MAX:
			alloc = category_first_allocation_max_seen(h, top_resource, max_worker, max_explicit);
			break;
		case CATEGORY_ALLOCATION_MODE_FIXED:
		default:
			alloc = top_resource;
			break;
	}

	return alloc;
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
    for(i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];
        int64_t should_update = rmsummary_get_by_offset(c->autolabel_resource, o);

        if(should_update) {
            struct histogram *h = itable_lookup(c->histograms, o);
            assert(h);

            int64_t top_value = rmsummary_get_by_offset(top, o);
            int64_t max_explicit = rmsummary_get_by_offset(c->max_allocation, o);

            int64_t worker = -1;
            if(max_worker) {
                worker = rmsummary_get_by_offset(max_worker, o);
            }

            int64_t new_value = category_first_allocation(h, c->allocation_mode, top_value, worker, max_explicit);

            rmsummary_set_by_offset(c->first_allocation, o, new_value);
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

    /* if task doesn't have resources measured, return 0 */
    if(!rs) {
        return update;
    }

    /* get user explicitly given maximum value per resource */
	const struct rmsummary *max  = c->max_allocation;

    int new_maximum = 0;
    if(!c->steady_state) {
        /* count new maximums only in steady state. */
        size_t i;

        /* loop to consider labeled resources defined above */
        for(i = 0; labeled_resources[i]; i++) {
            const size_t o = labeled_resources[i];

            if(rmsummary_get_by_offset(max, o) > 0) {
                // an explicit maximum was given, so this resource r cannot
                // trigger a new maximum
                continue;
            }

            struct histogram *h = itable_lookup(c->histograms, o);
            double max_seen = histogram_round_up(h, histogram_max_value(h));
            if(rmsummary_get_by_offset(rs, o) > max_seen) {
                // the measured is larger than what we have seen, thus we need
                // to reset the first allocation.
                new_maximum = 1;
                break;
            }
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

	int i;
    for (i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];
        double max = MAX(rmsummary_get_by_offset(rs, o), rmsummary_get_by_offset(c->max_resources_seen, o));
        rmsummary_set_by_offset(c->max_resources_seen, o, max);
    }
    if(rs && (!rs->exit_type || !strcmp(rs->exit_type, "normal"))) {
        size_t i;
        for(i = 0; labeled_resources[i]; i++) {
            const size_t o = labeled_resources[i];

            struct histogram *h = itable_lookup(c->histograms, o);
            assert(h);

            double value = rmsummary_get_by_offset(rs, o);
            double wall_time = rs->wall_time;

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

int category_bucketing_accumulate_summary(struct category *c, const struct rmsummary *rs, const struct rmsummary *max_worker, int taskid, int success) {
	int update = 0;

    /* if task doesn't have resources measured, return 0 */
    if(!rs) {
        return update;
    }

    //if category is in bucketing modes
    if (category_in_bucketing_mode(c))
    {
        //only add resource report when resources are exhausted (success = 0) or task succeeds (success = 1)
        if (success != -1)
            bucketing_manager_add_resource_report(c->bucketing_manager, taskid, (struct rmsummary*) rs, success);
    }

    /* get user explicitly given maximum value per resource */
	const struct rmsummary *max  = c->max_allocation;

    int new_maximum = 0;
    if(!c->steady_state) {
        /* count new maximums only in steady state. */
        size_t i;

        /* loop to consider labeled resources defined above */
        for(i = 0; labeled_resources[i]; i++) {
            const size_t o = labeled_resources[i];

            if(rmsummary_get_by_offset(max, o) > 0) {
                // an explicit maximum was given, so this resource r cannot
                // trigger a new maximum
                continue;
            }

            struct histogram *h = itable_lookup(c->histograms, o);
            double max_seen = histogram_round_up(h, histogram_max_value(h));
            if(rmsummary_get_by_offset(rs, o) > max_seen) {
                // the measured is larger than what we have seen, thus we need
                // to reset the first allocation.
                new_maximum = 1;
                break;
            }
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

    /* load new max values */
    int i;
    for(i = 0; labeled_resources[i]; i++) {
        const size_t o = labeled_resources[i];
        double max = MAX(rmsummary_get_by_offset(rs, o), rmsummary_get_by_offset(c->max_resources_seen, o));
        rmsummary_set_by_offset(c->max_resources_seen, o, max);
    }
	if(rs && (!rs->exit_type || !strcmp(rs->exit_type, "normal"))) {
        size_t i;
        for(i = 0; labeled_resources[i]; i++) {
            const size_t o = labeled_resources[i];

            struct histogram *h = itable_lookup(c->histograms, o);
            assert(h);

            double value = rmsummary_get_by_offset(rs, o);
            double wall_time = rs->wall_time;

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
			category_bucketing_accumulate_summary(c, s, NULL, -1, -1);
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
            for(i = 0; labeled_resources[i]; i++) {
                const size_t o = labeled_resources[i];
                if(!over) {
                    int64_t meas_value = rmsummary_get_by_offset(measured, o);
                    int64_t user_value = -1;
                    int64_t max_value  = -1;

                    if(user) {
                        user_value = rmsummary_get_by_offset(user, o);
                    }

                    if(c->max_allocation) {
                        max_value = rmsummary_get_by_offset(c->max_allocation, o);
                    }

                    if(user_value > -1) {
                        if(meas_value > user_value) {
                            over = 1;
                            break;
                        }
                    } else if(max_value > -1) {
                        if(meas_value > max_value) {
                            over = 1;
                            break;
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

    if(c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED &&
        c->allocation_mode != CATEGORY_ALLOCATION_MODE_MAX) {
        if (category_in_steady_state(c) && 
            (c->allocation_mode == CATEGORY_ALLOCATION_MODE_MIN_WASTE ||
            c->allocation_mode == CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT))
        {
            /* load max seen values, but only if not in fixed or max mode.
             * In max mode, max seen is the first allocation, and next allocation
             * is to use whole workers. */
            rmsummary_merge_override(internal, c->max_resources_seen);

            /* Never go below what first_allocation computer */
            rmsummary_merge_max(internal, c->first_allocation);
        }
    }

    /* load explicit category max values */
    rmsummary_merge_override(internal, c->max_allocation);

    if(category_in_steady_state(c) &&
            (c->allocation_mode == CATEGORY_ALLOCATION_MODE_MIN_WASTE ||
            c->allocation_mode ==CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT) &&
            request == CATEGORY_ALLOCATION_FIRST) {
		rmsummary_merge_override(internal, c->first_allocation);
	}

	/* chip in user values if explicitly given */
	rmsummary_merge_override(internal, user);

	return internal;
}

//taskid >=0 means real task needs prediction, -1 means function called for other purposes
const struct rmsummary *category_bucketing_dynamic_task_max_resources(struct category *c, struct rmsummary *user, category_allocation_t request, int taskid) {
	/* we keep an internal label so that the caller does not have to worry
	 * about memory leaks. */
	static struct rmsummary *internal = NULL;

	if(internal) {
		rmsummary_delete(internal);
	}

	internal = rmsummary_create(-1);

    if(c->allocation_mode != CATEGORY_ALLOCATION_MODE_FIXED &&
        c->allocation_mode != CATEGORY_ALLOCATION_MODE_MAX) {
        if (category_in_steady_state(c) && 
            (c->allocation_mode == CATEGORY_ALLOCATION_MODE_MIN_WASTE ||
            c->allocation_mode == CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT))
        {
            /* load max seen values, but only if not in fixed or max mode.
             * In max mode, max seen is the first allocation, and next allocation
             * is to use whole workers. */
            rmsummary_merge_override(internal, c->max_resources_seen);

            /* Never go below what first_allocation computer */
            rmsummary_merge_max(internal, c->first_allocation);
        }
        else if (taskid >= 0 && category_in_bucketing_mode(c))
        {
            struct rmsummary* bucketing_prediction = bucketing_manager_predict(c->bucketing_manager, taskid);
            rmsummary_merge_override(internal, bucketing_prediction);
            rmsummary_delete(bucketing_prediction);
        }
    }

    /* load explicit category max values */
    rmsummary_merge_override(internal, c->max_allocation);

    if(category_in_steady_state(c) &&
            (c->allocation_mode == CATEGORY_ALLOCATION_MODE_MIN_WASTE ||
            c->allocation_mode ==CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT) &&
            request == CATEGORY_ALLOCATION_FIRST) {
		rmsummary_merge_override(internal, c->first_allocation);
	}

	/* chip in user values if explicitly given */
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
        size_t i;
        for(i = 0; labeled_resources[i]; i++) {
            const size_t o = labeled_resources[i];
            /* set internal to seen value */
            rmsummary_set_by_offset(internal, o, rmsummary_get_by_offset(seen, o));
        }
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
        rmsummary_set(bucket_sizes, resource, size);
    }
}

int64_t category_get_bucket_size(const char *resource) {
    if(strcmp(resource, "category-steady-n-tasks") == 0) {
        return first_allocation_every_n_tasks;
    } else {
        return rmsummary_get(bucket_sizes, resource);
    }
}
