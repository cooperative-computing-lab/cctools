/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_H
#define CCTOOLS_CATEGORY_H

/** @file category.h Implements bookkeeping for categories of tasks and their resources.
*/

#include "hash_table.h"
#include "itable.h"
#include "histogram.h"
#include "timestamp.h"
#include "bucketing_manager.h"

/** \enum category_allocation_t
  Valid states for the lifetime of automatic resource allocations for a single task.
*/
typedef enum {
	CATEGORY_ALLOCATION_FIRST = 0,       /**< No automatic allocation, or using first step value of the two-step policy. */
	CATEGORY_ALLOCATION_AUTO  = 0,       /**< Same as FIRST, FIRST is deprecated */
	CATEGORY_ALLOCATION_MAX,             /**< Using max of category. (2nd step of two-step policy) */
	CATEGORY_ALLOCATION_ERROR,            /**< No valid resources could be found. (E.g., after 2nd step fails) */
    CATEGORY_ALLOCATION_GREEDY_BUCKETING,    /**< Use the greedy bucketing algorithm to label resources */
    CATEGORY_ALLOCATION_EXHAUSTIVE_BUCKETING, /**< Use the exhaustive bucketing algorithm to label resources */
} category_allocation_t;


/** \enum category_mode_t
  Valid modes for computing automatic resource allocations.
*/
typedef enum {
    CATEGORY_ALLOCATION_MODE_FIXED = 0, /**< When monitoring is disabled, all
                                          tasks run as
                                          WORK_QUEUE_ALLOCATION_MODE_FIXED. If
                                          monitoring is enabled and resource
                                          exhaustion occurs for specified
                                          resources values, then the task
                                          permanently fails. */
    CATEGORY_ALLOCATION_MODE_MAX, /**< When monitoring is enabled, tasks are
                                    tried with maximum specified values of
                                    cores, memory, disk or gpus until enough
                                    statistics are collected.  Then, further
                                    tasks are first tried using the maximum
                                    values observed, and in case of resource
                                    exhaustion, they are retried using the
                                    maximum specified values. The task
                                    permanently fails when there is an
                                    exhaustion using the maximum values. If no
                                    maximum values are specified, the task will
                                    wait until a larger worker connects. */
    CATEGORY_ALLOCATION_MODE_MIN_WASTE, /**< As above, but tasks are first
                                          tried with an automatically computed
                                         * allocation to minimize resource
                                         * waste. */
    CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT, /**< As above, but maximizing
                                              throughput. */
    CATEGORY_ALLOCATION_MODE_GREEDY_BUCKETING, /**< Use the greedy bucketing 
                                               algorithm to label resources */
    
    CATEGORY_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING   /**< Use the exhaustive 
                                                      bucketing algorithm to label resources */
} category_mode_t;


struct category {
	char *name;
	category_mode_t allocation_mode;

	double fast_abort;

	struct rmsummary *first_allocation;
	struct rmsummary *max_allocation;
	struct rmsummary *min_allocation;
	struct rmsummary *max_resources_seen;

	/* if 1, use first allocations. 0, use max fixed (if given) */
	struct rmsummary *autolabel_resource;

	struct itable *histograms;

    /* manager for bucketing mode, if applicable */
    bucketing_manager_t* bucketing_manager;

	int64_t total_tasks;

	/* completions since last time first-allocation was updated. */
	int64_t completions_since_last_reset;

	/* category is somewhat confident of the maximum seen value. */
	int steady_state;

	/* stats for work queue */
	uint64_t average_task_time;
	struct work_queue_stats *wq_stats;

	/* stats for taskvine */
	struct vine_stats *vine_stats;

	/* variables for makeflow */
	/* Mappings between variable names defined in the makeflow file and their values. */
	struct hash_table *mf_variables;
};

/* set autoallocation mode cores, memory, and disk. For other resources see category_enable_auto_resource. */
void category_specify_allocation_mode(struct category *c, int mode);
/* enable/disable autoallocation for the resource */
int category_enable_auto_resource(struct category *c, const char *resource_name, int autolabel);

void category_specify_max_allocation(struct category *c, const struct rmsummary *s);
void category_specify_min_allocation(struct category *c, const struct rmsummary *s);
void category_specify_first_allocation_guess(struct category *c, const struct rmsummary *s);

struct category *category_create(const char *name);
struct category *category_lookup_or_create(struct hash_table *categories, const char *name);
void category_delete(struct hash_table *categories, const char *name);
void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file);

int category_accumulate_summary(struct category *c, const struct rmsummary *rs, const struct rmsummary *max_worker);

int category_bucketing_accumulate_summary(struct category *c, const struct rmsummary *rs, const struct rmsummary *max_worker, int taskid, int success);

int category_in_bucketing_mode(struct category* c);

int category_update_first_allocation(struct category *c, const struct rmsummary *max_worker);

int category_in_steady_state(struct category *c);

category_allocation_t category_next_label(struct category *c, category_allocation_t current_label, int resource_overflow, struct rmsummary *user, struct rmsummary *measured);

const struct rmsummary *category_dynamic_task_max_resources(struct category *c, struct rmsummary *user, category_allocation_t request);

const struct rmsummary *category_bucketing_dynamic_task_max_resources(struct category *c, struct rmsummary *user, category_allocation_t request, int taskid);

const struct rmsummary *category_dynamic_task_min_resources(struct category *c, struct rmsummary *user, category_allocation_t request);

#endif
