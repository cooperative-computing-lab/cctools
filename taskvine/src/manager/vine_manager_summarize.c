/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_summarize.h"
#include "vine_worker_info.h"

#include "debug.h"
#include "hash_table.h"
#include "rmsummary.h"
#include "stringtools.h"

#include <math.h>
#include <stddef.h>
#include <stdlib.h>

size_t sort_ds_worker_summary_offset = 0;

static int sort_ds_worker_cmp(const void *a, const void *b)
{
	const struct rmsummary *x = *((const struct rmsummary **)a);
	const struct rmsummary *y = *((const struct rmsummary **)b);

	double count_x = x->workers;
	double count_y = y->workers;

	double res_x = rmsummary_get_by_offset(x, sort_ds_worker_summary_offset);
	double res_y = rmsummary_get_by_offset(y, sort_ds_worker_summary_offset);

	if (res_x == res_y) {
		return count_y - count_x;
	} else {
		return res_y - res_x;
	}
}

// function used by other functions
static void sort_ds_worker_summary(struct rmsummary **worker_data, int count, const char *sortby)
{
	if (!strcmp(sortby, "cores")) {
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, cores);
	} else if (!strcmp(sortby, "memory")) {
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, memory);
	} else if (!strcmp(sortby, "disk")) {
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, disk);
	} else if (!strcmp(sortby, "gpus")) {
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, gpus);
	} else if (!strcmp(sortby, "workers")) {
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, workers);
	} else {
		debug(D_NOTICE, "Invalid field to sort worker summaries. Valid fields are: cores, memory, disk, gpus, and workers.");
		sort_ds_worker_summary_offset = offsetof(struct rmsummary, memory);
	}

	qsort(&worker_data[0], count, sizeof(struct rmsummary *), sort_ds_worker_cmp);
}

// round to powers of two log scale with 1/n divisions
static double round_to_nice_power_of_2(double value, int n)
{
	double exp_org = log2(value);
	double below = pow(2, floor(exp_org));

	double rest = value - below;
	double fact = below / n;

	double rounded = below + floor(rest / fact) * fact;

	return rounded;
}

struct rmsummary **vine_manager_summarize_workers(struct vine_manager *q)
{
	struct vine_worker_info *w;
	struct rmsummary *s;
	char *id;
	char *resources_key;

	struct hash_table *workers_count = hash_table_create(0, 0);

	HASH_TABLE_ITERATE(q->worker_table, id, w)
	{

		if (w->resources->tag < 0) {
			// worker has not yet declared resources
			continue;
		}

		int cores = w->resources->cores.total;
		int memory = round_to_nice_power_of_2(w->resources->memory.total, 8);
		int disk = round_to_nice_power_of_2(w->resources->disk.total, 8);
		int gpus = w->resources->gpus.total;

		char *resources_key = string_format("%d_%d_%d_%d", cores, memory, disk, gpus);

		struct rmsummary *s = hash_table_lookup(workers_count, resources_key);
		if (!s) {
			s = rmsummary_create(-1);
			s->cores = cores;
			s->memory = memory;
			s->disk = disk;
			s->gpus = gpus;
			s->workers = 0;

			hash_table_insert(workers_count, resources_key, (void *)s);
		}
		free(resources_key);

		s->workers++;
	}

	int count = 0;
	struct rmsummary **worker_data =
			(struct rmsummary **)malloc((hash_table_size(workers_count) + 1) * sizeof(struct rmsummary *));

	HASH_TABLE_ITERATE(workers_count, resources_key, s)
	{
		worker_data[count] = s;
		count++;
	}

	worker_data[count] = NULL;

	hash_table_delete(workers_count);

	sort_ds_worker_summary(worker_data, count, "disk");
	sort_ds_worker_summary(worker_data, count, "memory");
	sort_ds_worker_summary(worker_data, count, "gpus");
	sort_ds_worker_summary(worker_data, count, "cores");
	sort_ds_worker_summary(worker_data, count, "workers");

	return worker_data;
}
