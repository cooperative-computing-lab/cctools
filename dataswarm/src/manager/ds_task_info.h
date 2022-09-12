/*
Store a report summarizing the performance of a completed task.
Keep a list of reports equal to the number of workers connected.
Used for computing queue capacity below.
*/

#ifndef DS_TASK_REPORT_H
#define DS_TASK_REPORT_H

#include "ds_task.h"
#include "ds_manager.h"
#include "timestamp.h"
#include "rmsummary.h"

struct ds_task_report {
	timestamp_t transfer_time;
	timestamp_t exec_time;
	timestamp_t manager_time;
	struct rmsummary *resources;
};

struct ds_task_report * ds_task_report_create( struct ds_task *t );
void ds_task_report_delete(struct ds_task_report *tr);

void ds_task_report_add( struct ds_manager *q, struct ds_task *t );
void ds_task_report_compute_capacity(const struct ds_manager *q, struct ds_stats *s);

#endif

