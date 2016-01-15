/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_CATEGORY_H
#define CCTOOLS_CATEGORY_H

#include "hash_table.h"
#include "timestamp.h"

struct category {
	char *name;
	double fast_abort;

	struct rmsummary *first_allocation;
	struct rmsummary *max_allocation;

	/* All keys are assumed positive. Thus, we shift them to the right so that
	 * we can have a "0" key. 0->1, 1->2, etc. */
	struct itable *cores_histogram;
	struct itable *memory_histogram;
	struct itable *disk_histogram;
	struct itable *wall_time_histogram;

	uint64_t total_tasks;

	/* stats for wq */
	uint64_t average_task_time;
	struct work_queue_stats *wq_stats;
};

struct category *category_lookup_or_create(struct hash_table *categories, const char *name);
void category_delete(struct hash_table *categories, const char *name);
int64_t category_first_allocation(struct itable *histogram, int64_t top_resource);
void category_accumulate_summary(struct hash_table *categories, const char *category, struct rmsummary *rs);
void category_update_first_allocation(struct hash_table *categories, const char *category, struct rmsummary *top);
void categories_initialize(struct hash_table *categories, struct rmsummary *top, const char *summaries_file);

#endif
