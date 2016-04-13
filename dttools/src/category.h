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
	CATEGORY_ALLOCATION_UNLABELED = 0, /**< No resources are explicitely requested. */
	CATEGORY_ALLOCATION_USER,          /**< Using values explicitely requested. */
	CATEGORY_ALLOCATION_AUTO_ZERO,     /**< Pre-step for autolabeling, when the first allocation has not been computed. */
	CATEGORY_ALLOCATION_AUTO_FIRST,    /**< Using first step value of the two-step policy. */
	CATEGORY_ALLOCATION_AUTO_MAX,      /**< Using max of category. (2nd step of two-step policy) */
	CATEGORY_ALLOCATION_ERROR          /**< No valid resources could be found. (E.g., after 2nd step fails) */
} category_allocation_t;

typedef enum {
	CATEGORY_ALLOCATION_MODE_MAX = 0,
	CATEGORY_ALLOCATION_MODE_MIN_WASTE,
	CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT
} category_mode_t;

struct category {
	char *name;
	double fast_abort;

	struct rmsummary *first_allocation;
	struct rmsummary *max_allocation;

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

struct category *category_lookup_or_create(struct hash_table *categories, const char *name);
void category_delete(struct hash_table *categories, const char *name);
void category_accumulate_summary(struct hash_table *categories, const char *category, struct rmsummary *rs);
void category_update_first_allocation(struct hash_table *categories, const char *category);
void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file);
category_allocation_t category_next_label(struct hash_table *categories, const char *category, category_allocation_t current_label, int resource_overflow);

const struct rmsummary *category_task_dynamic_label(category_allocation_t request, struct rmsummary *max, struct rmsummary *first, struct rmsummary *user);
#endif
