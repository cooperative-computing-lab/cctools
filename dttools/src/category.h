/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_H
#define CCTOOLS_CATEGORY_H

#include "hash_table.h"
#include "histogram.h"
#include "timestamp.h"

typedef enum {
	CATEGORY_ALLOCATION_FIRST = 0,       /**< No automatic allocation, or using first step value of the two-step policy. */
	CATEGORY_ALLOCATION_MAX,         /**< Using max of category. (2nd step of two-step policy) */
	CATEGORY_ALLOCATION_ERROR             /**< No valid resources could be found. (E.g., after 2nd step fails) */
} category_allocation_t;


/* Names here a little ugly. We call them 'WORK_QUEUE_' to present a uniform
name pattern for work queue applications, even when this is specific to
categories. We add macro definitions, with nicer names. */

typedef enum {
/**< When monitoring is disabled, all tasks run as
  WORK_QUEUE_ALLOCATION_MODE_FIXED. If monitoring is enabled and resource
  exhaustion occurs: */
	WORK_QUEUE_ALLOCATION_MODE_FIXED = 0,   /**< Task fails. (default) */
	WORK_QUEUE_ALLOCATION_MODE_MAX,         /**< If maximum values are specified for cores, memory,
											  or disk (either a user-label or category-label) and
											  one of those resources is exceeded, the task fails.
											  Otherwise it is retried until a large enough worker
											  connects to the master, using the maximum values
											  specified, and the maximum values so far seen for
											  resources not specified. */

	WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE,   /**< As above, but tasks are tried with an automatically
											  computed first-allocation to minimize resource waste. */
	WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT /**< As above, but maximizing throughput. */
} category_mode_t;

#define	CATEGORY_ALLOCATION_MODE_FIXED          WORK_QUEUE_ALLOCATION_MODE_FIXED
#define	CATEGORY_ALLOCATION_MODE_MAX            WORK_QUEUE_ALLOCATION_MODE_MAX
#define	CATEGORY_ALLOCATION_MODE_MIN_WASTE      WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE
#define	CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT

struct category {
	char *name;
	category_mode_t allocation_mode;

	double fast_abort;

	struct rmsummary *first_allocation;
	struct rmsummary *max_allocation;
	struct rmsummary *max_resources_seen;

	/* if 1, use first allocations. 0, use max fixed (if given) */
	struct rmsummary *autolabel_resource;

	struct histogram *cores_histogram;
	struct histogram *cores_avg_histogram;
	struct histogram *wall_time_histogram;
	struct histogram *cpu_time_histogram;
	struct histogram *max_concurrent_processes_histogram;
	struct histogram *total_processes_histogram;
	struct histogram *memory_histogram;
	struct histogram *swap_memory_histogram;
	struct histogram *virtual_memory_histogram;
	struct histogram *bytes_read_histogram;
	struct histogram *bytes_written_histogram;
	struct histogram *bytes_received_histogram;
	struct histogram *bytes_sent_histogram;
	struct histogram *bandwidth_histogram;
	struct histogram *total_files_histogram;
	struct histogram *disk_histogram;

	int64_t total_tasks;

	/* assume that peak usage is independent of wall time */
	int time_peak_independece;

	/* completions since last time first-allocation was updated. */
	int64_t completions_since_last_reset;


	/* category is somewhat confident of the maximum seen value. */
	int steady_state;

	/* stats for wq */
	uint64_t average_task_time;
	struct work_queue_stats *wq_stats;

	/* variables for makeflow */
	/* Mappings between variable names defined in the makeflow file and their values. */
	struct hash_table *mf_variables;
};

/* set autoallocation mode cores, memory, and disk. For other resources see category_enable_auto_resource. */
void category_specify_allocation_mode(struct category *c, category_mode_t mode);
/* enable/disable autoallocation for the resource */
int category_enable_auto_resource(struct category *c, const char *resource_name, int autolabel);

void category_specify_max_allocation(struct category *c, const struct rmsummary *s);
void category_specify_first_allocation_guess(struct category *c, const struct rmsummary *s);

struct category *category_create(const char *name);
struct category *category_lookup_or_create(struct hash_table *categories, const char *name);
void category_delete(struct hash_table *categories, const char *name);
void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file);

int category_accumulate_summary(struct category *c, const struct rmsummary *rs, const struct rmsummary *max_worker);
int category_update_first_allocation(struct category *c, const struct rmsummary *max_worker);

int category_in_steady_state(struct category *c);

category_allocation_t category_next_label(struct category *c, category_allocation_t current_label, int resource_overflow, struct rmsummary *user, struct rmsummary *measured);

const struct rmsummary *category_dynamic_task_max_resources(struct category *c, struct rmsummary *user, category_allocation_t request);

const struct rmsummary *category_dynamic_task_min_resources(struct category *c, struct rmsummary *user, category_allocation_t request);

#endif
