/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_H
#define CCTOOLS_CATEGORY_H

#include "hash_table.h"
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
	double fast_abort;

	struct rmsummary *first_allocation;
	struct rmsummary *max_allocation;

	struct rmsummary *max_resources_seen;
	struct rmsummary *max_resources_completed;

	/* if 1, use first allocations. 0, use max fixed (if given) */
	struct rmsummary *autolabel_resource;

	/* All keys are assumed positive. Thus, we shift them to the right so that
	 * we can have a "0" key. 0->1, 1->2, etc. */
	struct itable *cores_histogram;
	struct itable *wall_time_histogram;
	struct itable *cpu_time_histogram;
	struct itable *max_concurrent_processes_histogram;
	struct itable *total_processes_histogram;
	struct itable *memory_histogram;
	struct itable *swap_memory_histogram;
	struct itable *virtual_memory_histogram;
	struct itable *bytes_read_histogram;
	struct itable *bytes_written_histogram;
	struct itable *bytes_received_histogram;
	struct itable *bytes_sent_histogram;
	struct itable *bandwidth_histogram;
	struct itable *total_files_histogram;
	struct itable *disk_histogram;

	category_mode_t allocation_mode;

	uint64_t total_tasks;

	/* assume that peak usage is independent of wall time */
	int time_peak_independece;

	/* stats for wq */
	uint64_t average_task_time;
	struct work_queue_stats *wq_stats;

	/* variables for makeflow */
	/* Mappings between variable names defined in the makeflow file and their values. */
	struct hash_table *mf_variables;
};

/* enable/disable autoallocation for the resource */
int category_enable_auto_resource(struct category *c, const char *resource_name, int autolabel);

struct category *category_lookup_or_create(struct hash_table *categories, const char *name);
void category_delete(struct hash_table *categories, const char *name);
void category_accumulate_summary(struct hash_table *categories, const char *category, struct rmsummary *rs);
void category_update_first_allocation(struct hash_table *categories, const char *category);
void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file);


category_allocation_t category_next_label(struct category *c, category_allocation_t current_label, int resource_overflow, struct rmsummary *user, struct rmsummary *measured);


const struct rmsummary *category_task_dynamic_label(struct rmsummary *max, struct rmsummary *first, struct rmsummary *user, category_allocation_t requested);
#endif
