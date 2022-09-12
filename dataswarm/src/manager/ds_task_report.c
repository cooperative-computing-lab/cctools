
#include "ds_manager.h"
#include "ds_task_report.h"

#include "macros.h"

#include <math.h>

// The default tasks capacity reported before information is available.
// Default capacity also implies 1 core, 1024 MB of disk and 512 memory per task.
#define DS_DEFAULT_CAPACITY_TASKS 10

// The minimum number of task reports to keep
#define DS_TASK_REPORT_MIN_SIZE 50

struct ds_task_report * ds_task_report_create( struct ds_task *t )
{
	struct ds_task_report *tr = calloc(1, sizeof(*tr));

	tr->transfer_time = (t->time_when_commit_end - t->time_when_commit_start) + (t->time_when_done - t->time_when_retrieval);
	tr->exec_time     = t->time_workers_execute_last;
	tr->manager_time  = (((t->time_when_done - t->time_when_commit_start) - tr->transfer_time) - tr->exec_time);
	tr->resources     = rmsummary_copy(t->resources_allocated, 0);

	return tr;
}

void ds_task_report_delete(struct ds_task_report *tr)
{
	rmsummary_delete(tr->resources);
	free(tr);
}

void ds_task_report_add(struct ds_manager *q, struct ds_task *t)
{
	if(!t->resources_allocated) {
		return;
	}

	struct ds_stats s;
	ds_get_stats(q, &s);

	struct ds_task_report *tr = ds_task_report_create(t);

	list_push_tail(q->task_reports, tr);

	// Trim the list, but never below its previous size.
	static int count = DS_TASK_REPORT_MIN_SIZE;
	count = MAX(count, 2*q->stats->tasks_on_workers);

	while(list_size(q->task_reports) >= count) {
		tr = list_pop_head(q->task_reports);
		ds_task_report_delete(tr);
	}

	resource_monitor_append_report(q, t);
}

/*
Compute queue capacity based on stored task reports
and the summary of manager activity.
*/

void ds_task_report_compute_capacity(const struct ds_manager *q, struct ds_stats *s)
{
	struct ds_task_report *capacity = calloc(1, sizeof(*capacity));
	capacity->resources = rmsummary_create(0);

	struct ds_task_report *tr;
	double alpha = 0.05;
	int count = list_size(q->task_reports);
	int capacity_instantaneous = 0;

	// Compute the average task properties.
	if(count < 1) {
		capacity->resources->cores  = 1;
		capacity->resources->memory = 512;
		capacity->resources->disk   = 1024;
		capacity->resources->gpus   = 0;

		capacity->exec_time     = DS_DEFAULT_CAPACITY_TASKS;
		capacity->transfer_time = 1;

		q->stats->capacity_weighted = DS_DEFAULT_CAPACITY_TASKS;
		capacity_instantaneous = DS_DEFAULT_CAPACITY_TASKS;

		count = 1;
	} else {
		// Sum up the task reports available.
		list_first_item(q->task_reports);
		while((tr = list_next_item(q->task_reports))) {
			capacity->transfer_time += tr->transfer_time;
			capacity->exec_time     += tr->exec_time;
			capacity->manager_time   += tr->manager_time;

			if(tr->resources) {
				capacity->resources->cores  += tr->resources ? tr->resources->cores  : 1;
				capacity->resources->memory += tr->resources ? tr->resources->memory : 512;
				capacity->resources->disk   += tr->resources ? tr->resources->disk   : 1024;
				capacity->resources->gpus   += tr->resources ? tr->resources->gpus   : 0;
			}
		}

		tr = list_peek_tail(q->task_reports);
		if(tr->transfer_time > 0) {
			capacity_instantaneous = DIV_INT_ROUND_UP(tr->exec_time, (tr->transfer_time + tr->manager_time));
			q->stats->capacity_weighted = (int) ceil((alpha * (float) capacity_instantaneous) + ((1.0 - alpha) * q->stats->capacity_weighted));
			time_t ts;
			time(&ts);
		}
	}

	capacity->transfer_time = MAX(1, capacity->transfer_time);
	capacity->exec_time     = MAX(1, capacity->exec_time);
	capacity->manager_time   = MAX(1, capacity->manager_time);

	// Never go below the default capacity
	int64_t ratio = MAX(DS_DEFAULT_CAPACITY_TASKS, DIV_INT_ROUND_UP(capacity->exec_time, (capacity->transfer_time + capacity->manager_time)));

	q->stats->capacity_tasks  = ratio;
	q->stats->capacity_cores  = DIV_INT_ROUND_UP(capacity->resources->cores  * ratio, count);
	q->stats->capacity_memory = DIV_INT_ROUND_UP(capacity->resources->memory * ratio, count);
	q->stats->capacity_disk   = DIV_INT_ROUND_UP(capacity->resources->disk   * ratio, count);
	q->stats->capacity_gpus   = DIV_INT_ROUND_UP(capacity->resources->gpus   * ratio, count);
	q->stats->capacity_instantaneous = DIV_INT_ROUND_UP(capacity_instantaneous, 1);

	ds_task_report_delete(capacity);
}

