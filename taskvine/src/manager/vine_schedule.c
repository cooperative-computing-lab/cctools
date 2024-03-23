/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_schedule.h"
#include "vine_blocklist.h"
#include "vine_factory_info.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_mount.h"

#include "debug.h"
#include "hash_table.h"
#include "list.h"
#include "rmonitor_types.h"
#include "rmsummary.h"

#include <limits.h>
#include <math.h>

/* check whether worker has all fixed locations required for task */
int check_fixed_location_worker(struct vine_manager *m, struct vine_worker_info *w, struct vine_task *t)
{
	int all_present = 1;
	struct vine_mount *mt;
	struct vine_file_replica *replica;

	if (t->has_fixed_locations) {
		LIST_ITERATE(t->input_mounts, mt)
		{
			if (mt->file->flags & VINE_FIXED_LOCATION) {
				replica = hash_table_lookup(w->current_files, mt->file->cached_name);
				if (!replica) {
					all_present = 0;
					break;
				}
			}
		}
	}

	return all_present;
}

/* Check if queue has entered ramp_down mode (more workers than waiting tasks).
 * @param q The manager structure.
 * @return 1 if in ramp down mode, 0 otherwise.
 */

int vine_schedule_in_ramp_down(struct vine_manager *q)
{
	if (!(q->monitor_mode & VINE_MON_WATCHDOG)) {
		/* if monitoring is not terminating tasks because of resources, ramp down heuristic does not have any
		 * effect. */
		return 0;
	}

	if (!q->ramp_down_heuristic) {
		return 0;
	}

	if (hash_table_size(q->worker_table) > list_size(q->ready_list)) {
		return 1;
	}

	return 0;
}

/* Check if worker resources are enough to run the task.
 * Note that empty libraries are not *real* tasks and can be
 * killed as needed to reclaim unused resources and
 * make space for other libraries or tasks.
 * @param q     Manager info structure.
 * @param w     Worker info structure.
 * @param t     Task info structure.
 * @param tr    Chosen task resources.
 * @return 1 if yes, 0 otherwise. */
int check_worker_have_enough_resources(
		struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *tr)
{
	struct vine_resources *worker_net_resources = vine_resources_copy(w->resources);

	/* Pretend to reclaim resources from empty libraries. */
	uint64_t task_id;
	struct vine_task *ti;
	ITABLE_ITERATE(w->current_tasks, task_id, ti)
	{
		if (ti->provides_library && ti->function_slots_inuse == 0 &&
				(!t->needs_library || strcmp(t->needs_library, ti->provides_library))) {
			worker_net_resources->disk.inuse -= ti->current_resource_box->disk;
			worker_net_resources->cores.inuse -= ti->current_resource_box->cores;
			worker_net_resources->memory.inuse -= ti->current_resource_box->memory;
			worker_net_resources->gpus.inuse -= ti->current_resource_box->gpus;
		}
	}

	int ok = 1;
	if (worker_net_resources->disk.inuse + tr->disk > worker_net_resources->disk.total) { /* No overcommit disk */
		ok = 0;
	}

	if ((tr->cores > worker_net_resources->cores.total) ||
			(worker_net_resources->cores.inuse + tr->cores >
					overcommitted_resource_total(q, worker_net_resources->cores.total))) {
		ok = 0;
	}

	if ((tr->memory > worker_net_resources->memory.total) ||
			(worker_net_resources->memory.inuse + tr->memory >
					overcommitted_resource_total(q, worker_net_resources->memory.total))) {
		ok = 0;
	}

	if ((tr->gpus > worker_net_resources->gpus.total) ||
			(worker_net_resources->gpus.inuse + tr->gpus >
					overcommitted_resource_total(q, worker_net_resources->gpus.total))) {
		ok = 0;
	}
	vine_resources_delete(worker_net_resources);
	return ok;
}

/* Check if this task is compatible with this given worker by considering
 * resources availability, features, blocklist, and all other relevant factors.
 * Used by all scheduling methods for basic compatibility.
 * @param q The manager structure.
 * @param w The worker info structure.
 * @param t The task structure.
 * @return 0 if the task is not compatible with the worker, 1 otherwise.
 */

int check_worker_against_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	/* worker has not reported any resources yet */
	if (w->resources->tag < 0 || w->resources->workers.total < 1) {
		return 0;
	}

	/* Don't send tasks to this worker if it is in draining mode (no more tasks). */
	if (w->draining) {
		return 0;
	}

	/* Don't send tasks if the factory is used and has too many connected workers. */
	if (w->factory_name) {
		struct vine_factory_info *f = vine_factory_info_lookup(q, w->factory_name);
		if (f && f->connected_workers > f->max_workers)
			return 0;
	}

	/* Check if worker is blocked from the manager. */
	if (vine_blocklist_is_blocked(q, w->hostname)) {
		return 0;
	}

	/* Compute the resources to allocate to this task. */
	struct rmsummary *l = vine_manager_choose_resources_for_task(q, w, t);

	if (!check_worker_have_enough_resources(q, w, t, l)) {
		rmsummary_delete(l);
		return 0;
	}
	rmsummary_delete(l);

	/* If this is a function task, check if the worker can run it.
	 * May require the manager to send a library to the worker first. */
	if (t->needs_library && !vine_manager_check_worker_can_run_function_task(q, w, t)) {
		/* Careful: If this failed, then the worker object may longer be valid! */
		return 0;
	}

	// if worker's end time has not been received
	if (w->end_time < 0) {
		return 0;
	}

	// if wall time for worker is specified and there's not enough time for task, then not ok
	if (w->end_time > 0) {
		double current_time = timestamp_get() / ONE_SECOND;
		if (t->resources_requested->end > 0 && w->end_time < t->resources_requested->end) {
			return 0;
		}
		if (t->min_running_time > 0 && w->end_time - current_time < t->min_running_time) {
			return 0;
		}
	}

	/* If the worker is not the one the task wants. */
	if (t->has_fixed_locations && !check_fixed_location_worker(q, w, t)) {
		return 0;
	}

	/* If the worker doesn't have the features the task requires. */
	if (t->feature_list) {
		if (!w->features) {
			return 0;
		} else {
			char *feature;
			LIST_ITERATE(t->feature_list, feature)
			{
				if (!hash_table_lookup(w->features, feature)) {
					return 0;
				}
			}
		}
	}

	return 1;
}

// 0 if current_best has more free resources than candidate, 1 else.
static int candidate_has_worse_fit(struct vine_worker_info *current_best, struct vine_worker_info *candidate)
{
	struct vine_resources *b = current_best->resources;
	struct vine_resources *o = candidate->resources;

	// Total worker order: free cores > free memory > free disk > free gpus
	int free_delta = (b->cores.total - b->cores.inuse) - (o->cores.total - o->cores.inuse);
	if (free_delta > 0) {
		return 1;
	} else if (free_delta < 0) {
		return 0;
	}

	// Same number of free cores...
	free_delta = (b->memory.total - b->memory.inuse) - (o->memory.total - o->memory.inuse);
	if (free_delta > 0) {
		return 1;
	} else if (free_delta < 0) {
		return 0;
	}

	// Same number of free disk...
	free_delta = (b->disk.total - b->disk.inuse) - (o->disk.total - o->disk.inuse);
	if (free_delta > 0) {
		return 1;
	} else if (free_delta < 0) {
		return 0;
	}

	// Number of free resources are the same.
	return 0;
}

/*
Find the worker that has the largest quantity of cached data needed
by this task, so as to minimize transfer work that must be done
by the manager.
*/

static struct vine_worker_info *find_worker_by_files(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;
	struct vine_worker_info *best_worker = 0;
	int offset_bookkeep;
	int64_t most_task_cached_bytes = 0;
	int64_t task_cached_bytes;
	uint8_t has_all_files;
	struct vine_file_replica *replica;
	struct vine_mount *m;

	int ramp_down = vine_schedule_in_ramp_down(q);

	HASH_TABLE_ITERATE_RANDOM_START(q->worker_table, offset_bookkeep, key, w)
	{
		/* Careful: If check_worker_against task fails, then w may no longer be valid. */
		if (check_worker_against_task(q, w, t)) {
			task_cached_bytes = 0;
			has_all_files = 1;

			LIST_ITERATE(t->input_mounts, m)
			{
				replica = hash_table_lookup(w->current_files, m->file->cached_name);

				if (replica && m->file->type == VINE_FILE) {
					task_cached_bytes += replica->size;
				} else if (m->file->cache_level > VINE_CACHE_LEVEL_TASK) {
					has_all_files = 0;
				}
			}

			/* Return the worker if it was in possession of all cacheable files */
			if (has_all_files && !ramp_down) {
				return w;
			}

			if (!best_worker || task_cached_bytes > most_task_cached_bytes ||
					(ramp_down && task_cached_bytes == most_task_cached_bytes &&
							candidate_has_worse_fit(best_worker, w))) {
				best_worker = w;
				most_task_cached_bytes = task_cached_bytes;
			}
		}
	}

	return best_worker;
}

/*
Find the first available worker in first-come, first-served order.
Since the order of workers in the hashtable is somewhat arbitrary,
this amounts to simply "find the first available worker".
*/

static struct vine_worker_info *find_worker_by_fcfs(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* Careful: If check_worker_against task fails, then w may no longer be valid. */
		if (check_worker_against_task(q, w, t)) {
			return w;
		}
	}
	return NULL;
}

/*
Select an available worker at random.
This works by finding all compatible workers,
putting them in a list, and then choosing from the list at random.
*/

static struct vine_worker_info *find_worker_by_random(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w = NULL;
	int random_worker;
	struct list *valid_workers = list_create();

	// avoid the temptation to use HASH_TABLE_ITERATE_RANDOM_START for this loop.
	// HASH_TABLE_ITERATE_RANDOM_START would give preference to workers that appear first in a bucket.
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* Careful: If check_worker_against task fails, then w may no longer be valid. */
		if (check_worker_against_task(q, w, t)) {
			list_push_tail(valid_workers, w);
		}
	}

	w = NULL;
	if (list_size(valid_workers) > 0) {
		random_worker = (rand() % list_size(valid_workers)) + 1;

		while (random_worker && list_size(valid_workers)) {
			w = list_pop_head(valid_workers);
			random_worker--;
		}
	}

	list_delete(valid_workers);
	return w;
}

/*
Find the worker that is the "worst fit" for this task,
meaning the worker that will have the most resources
unused once this task is placed there.
*/

static struct vine_worker_info *find_worker_by_worst_fit(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;
	struct vine_worker_info *best_worker = NULL;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* Careful: If check_worker_against task fails, then w may no longer be valid. */
		if (check_worker_against_task(q, w, t)) {
			if (!best_worker || candidate_has_worse_fit(best_worker, w)) {
				best_worker = w;
			}
		}
	}

	return best_worker;
}

/*
Find the worker that produced the fastest runtime of prior tasks.
If there are no workers avialable that have previously run a task,
then pick one FCFS.
*/

static struct vine_worker_info *find_worker_by_time(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;
	struct vine_worker_info *best_worker = 0;
	double best_time = HUGE_VAL;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* Careful: If check_worker_against task fails, then w may no longer be valid. */
		if (check_worker_against_task(q, w, t)) {
			if (w->total_tasks_complete > 0) {
				double t = (w->total_task_time + w->total_transfer_time) / w->total_tasks_complete;
				if (!best_worker || t < best_time ||
						(t == best_time && vine_schedule_in_ramp_down(q) &&
								candidate_has_worse_fit(best_worker, w))) {
					best_worker = w;
					best_time = t;
				}
			}
		}
	}

	if (best_worker) {
		return best_worker;
	} else if (vine_schedule_in_ramp_down(q)) {
		return find_worker_by_worst_fit(q, t);
	} else {
		return find_worker_by_fcfs(q, t);
	}
}

/*
Select the best worker for this task, based on the current scheduling mode.
*/

struct vine_worker_info *vine_schedule_task_to_worker(struct vine_manager *q, struct vine_task *t)
{
	int a = t->worker_selection_algorithm;

	if (a == VINE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	switch (a) {
	case VINE_SCHEDULE_FILES:
		return find_worker_by_files(q, t);
	case VINE_SCHEDULE_TIME:
		return find_worker_by_time(q, t);
	case VINE_SCHEDULE_WORST:
		return find_worker_by_worst_fit(q, t);
	case VINE_SCHEDULE_FCFS:
		return find_worker_by_fcfs(q, t);
	case VINE_SCHEDULE_RAND:
	default:
		return find_worker_by_random(q, t);
	}
}

typedef enum {
	CORES_BIT = (1 << 0),
	MEMORY_BIT = (1 << 1),
	DISK_BIT = (1 << 2),
	GPUS_BIT = (1 << 3),
} vine_resource_bitmask_t;

/*
Compares the resources needed by a task to a given worker.
Returns a bitmask that indicates which resource of the task, if any, cannot
be met by the worker. If the task fits in the worker, it returns 0.
*/

static vine_resource_bitmask_t is_task_larger_than_worker(
		struct vine_manager *q, struct vine_task *t, struct vine_worker_info *w)
{
	if (w->resources->tag < 0) {
		/* quickly return if worker has not sent its resources yet */
		return 0;
	}

	vine_resource_bitmask_t set = 0;
	struct rmsummary *l = vine_manager_choose_resources_for_task(q, w, t);

	// baseline resurce comparison of worker total resources and a task requested resorces

	if ((double)w->resources->cores.total < l->cores) {
		set = set | CORES_BIT;
	}

	if ((double)w->resources->memory.total < l->memory) {
		set = set | MEMORY_BIT;
	}

	if ((double)w->resources->disk.total < l->disk) {
		set = set | DISK_BIT;
	}

	if ((double)w->resources->gpus.total < l->gpus) {
		set = set | GPUS_BIT;
	}
	rmsummary_delete(l);

	return set;
}

/*
Compares the resources needed by a task to all connected workers.
Returns 0 if there is worker than can fit the task. Otherwise it returns a bitmask
that indicates that there was at least one worker that could not fit that task resource.
*/

static vine_resource_bitmask_t is_task_larger_than_any_worker(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;

	int bit_set = 0;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		vine_resource_bitmask_t new_set = is_task_larger_than_worker(q, t, w);
		if (new_set == 0) {
			// Task could run on a currently connected worker, immediately
			// return
			return 0;
		}

		// Inherit the unfit criteria for this task
		bit_set = bit_set | new_set;
	}

	return bit_set;
}

/*
Determine if there exists a ready task that cannot be satisfied
by *any* connected worker, even if all other tasks finish.
If so, then display a suitable message to the user.
This is quite an expensive function and so is invoked only periodically.
*/

void vine_schedule_check_for_large_tasks(struct vine_manager *q)
{
	struct vine_task *t;
	int unfit_core = 0;
	int unfit_mem = 0;
	int unfit_disk = 0;
	int unfit_gpu = 0;

	struct rmsummary *largest_unfit_task = rmsummary_create(-1);

	LIST_ITERATE(q->ready_list, t)
	{

		// check each task against the queue of connected workers
		vine_resource_bitmask_t bit_set = is_task_larger_than_any_worker(q, t);
		if (bit_set) {
			rmsummary_merge_max(largest_unfit_task, vine_manager_task_resources_max(q, t));
			rmsummary_merge_max(largest_unfit_task, vine_manager_task_resources_min(q, t));
		}
		if (bit_set & CORES_BIT) {
			unfit_core++;
		}
		if (bit_set & MEMORY_BIT) {
			unfit_mem++;
		}
		if (bit_set & DISK_BIT) {
			unfit_disk++;
		}
		if (bit_set & GPUS_BIT) {
			unfit_gpu++;
		}
	}

	if (unfit_core || unfit_mem || unfit_disk || unfit_gpu) {
		notice(D_VINE, "There are tasks that cannot fit any currently connected worker:\n");
	}

	if (unfit_core) {
		notice(D_VINE,
				"    %d waiting task(s) need more than %s",
				unfit_core,
				rmsummary_resource_to_str("cores", largest_unfit_task->cores, 1));
	}

	if (unfit_mem) {
		notice(D_VINE,
				"    %d waiting task(s) need more than %s of memory",
				unfit_mem,
				rmsummary_resource_to_str("memory", largest_unfit_task->memory, 1));
	}

	if (unfit_disk) {
		notice(D_VINE,
				"    %d waiting task(s) need more than %s of disk",
				unfit_disk,
				rmsummary_resource_to_str("disk", largest_unfit_task->disk, 1));
	}

	if (unfit_gpu) {
		notice(D_VINE,
				"    %d waiting task(s) need more than %s",
				unfit_gpu,
				rmsummary_resource_to_str("gpus", largest_unfit_task->gpus, 1));
	}

	rmsummary_delete(largest_unfit_task);
}

/*
Determine whether there is a worker that can fit the task and that has all its strict inputs.
*/
int vine_schedule_check_fixed_location(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		if (check_fixed_location_worker(q, w, t)) {
			return 1;
		}
	}

	return 0;
}
