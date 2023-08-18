/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_task_info.h"
#include "vine_manager.h"

#include "macros.h"

#include <math.h>

// The default tasks capacity reported before information is available.
// Default capacity also implies 1 core, 1024 MB of disk and 512 memory per task.
#define VINE_DEFAULT_CAPACITY_TASKS 10

// The minimum number of task reports to keep
#define VINE_TASK_INFO_MIN_SIZE 50

struct vine_task_info *vine_task_info_create(struct vine_task *t)
{
	struct vine_task_info *ti = calloc(1, sizeof(*ti));

	ti->transfer_time = (t->time_when_commit_end - t->time_when_commit_start) +
			    (t->time_when_done - t->time_when_retrieval);
	ti->exec_time = t->time_workers_execute_last;
	ti->manager_time = (((t->time_when_done - t->time_when_commit_start) - ti->transfer_time) - ti->exec_time);
	ti->resources = rmsummary_copy(t->resources_allocated, 0);

	return ti;
}

void vine_task_info_delete(struct vine_task_info *ti)
{
	rmsummary_delete(ti->resources);
	free(ti);
}

void vine_task_info_add(struct vine_manager *q, struct vine_task *t)
{
	if (!t->resources_allocated) {
		return;
	}

	struct vine_stats s;
	vine_get_stats(q, &s);

	struct vine_task_info *ti = vine_task_info_create(t);

	list_push_tail(q->task_info_list, ti);

	// Trim the list, but never below its previous size.
	static int count = VINE_TASK_INFO_MIN_SIZE;
	count = MAX(count, 2 * q->stats->tasks_on_workers);

	while (list_size(q->task_info_list) >= count) {
		ti = list_pop_head(q->task_info_list);
		vine_task_info_delete(ti);
	}
}

/*
Compute queue capacity based on stored task reports
and the summary of manager activity.
*/

void vine_task_info_compute_capacity(const struct vine_manager *q, struct vine_stats *s)
{
	struct vine_task_info *capacity = calloc(1, sizeof(*capacity));
	capacity->resources = rmsummary_create(0);

	struct vine_task_info *ti;
	double alpha = 0.05;
	int count = list_size(q->task_info_list);
	int capacity_instantaneous = 0;

	// Compute the average task properties.
	if (count < 1) {
		capacity->resources->cores = 1;
		capacity->resources->memory = 512;
		capacity->resources->disk = 1024;
		capacity->resources->gpus = 0;

		capacity->exec_time = VINE_DEFAULT_CAPACITY_TASKS;
		capacity->transfer_time = 1;

		q->stats->capacity_weighted = VINE_DEFAULT_CAPACITY_TASKS;
		capacity_instantaneous = VINE_DEFAULT_CAPACITY_TASKS;

		count = 1;
	} else {
		// Sum up the task reports available.

		LIST_ITERATE(q->task_info_list, ti)
		{
			capacity->transfer_time += ti->transfer_time;
			capacity->exec_time += ti->exec_time;
			capacity->manager_time += ti->manager_time;

			if (ti->resources) {
				capacity->resources->cores += ti->resources ? ti->resources->cores : 1;
				capacity->resources->memory += ti->resources ? ti->resources->memory : 512;
				capacity->resources->disk += ti->resources ? ti->resources->disk : 1024;
				capacity->resources->gpus += ti->resources ? ti->resources->gpus : 0;
			}
		}

		ti = list_peek_tail(q->task_info_list);
		if (ti->transfer_time > 0) {
			capacity_instantaneous =
					DIV_INT_ROUND_UP(ti->exec_time, (ti->transfer_time + ti->manager_time));
			q->stats->capacity_weighted = (int)ceil((alpha * (float)capacity_instantaneous) +
								((1.0 - alpha) * q->stats->capacity_weighted));
			time_t ts;
			time(&ts);
		}
	}

	capacity->transfer_time = MAX(1, capacity->transfer_time);
	capacity->exec_time = MAX(1, capacity->exec_time);
	capacity->manager_time = MAX(1, capacity->manager_time);

	// Never go below the default capacity
	int64_t ratio = MAX(VINE_DEFAULT_CAPACITY_TASKS,
			DIV_INT_ROUND_UP(capacity->exec_time, (capacity->transfer_time + capacity->manager_time)));

	q->stats->capacity_tasks = ratio;
	q->stats->capacity_cores = DIV_INT_ROUND_UP(capacity->resources->cores * ratio, count);
	q->stats->capacity_memory = DIV_INT_ROUND_UP(capacity->resources->memory * ratio, count);
	q->stats->capacity_disk = DIV_INT_ROUND_UP(capacity->resources->disk * ratio, count);
	q->stats->capacity_gpus = DIV_INT_ROUND_UP(capacity->resources->gpus * ratio, count);
	q->stats->capacity_instantaneous = DIV_INT_ROUND_UP(capacity_instantaneous, 1);

	vine_task_info_delete(capacity);
}
