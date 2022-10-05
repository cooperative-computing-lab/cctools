/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TASK_INFO_H
#define VINE_TASK_INFO_H

/*
Store a report summarizing the performance of a completed task.
Keep a list of reports equal to the number of workers connected.
Used for computing queue capacity below.
*/

#include "vine_task.h"
#include "vine_manager.h"
#include "timestamp.h"
#include "rmsummary.h"

struct vine_task_info {
	timestamp_t transfer_time;
	timestamp_t exec_time;
	timestamp_t manager_time;
	struct rmsummary *resources;
};

struct vine_task_info * vine_task_info_create( struct vine_task *t );
void vine_task_info_delete(struct vine_task_info *tr);

void vine_task_info_add( struct vine_manager *q, struct vine_task *t );
void vine_task_info_compute_capacity(const struct vine_manager *q, struct vine_stats *s);

#endif

