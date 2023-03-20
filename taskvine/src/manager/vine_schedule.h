/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_SCHEDULE_H
#define VINE_SCHEDULE_H

/*
Implementation of the manager's scheduling algorithm.
A single entry point maps a given task to the best available
worker, taking into account all scheduling priorities and constraints.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"
#include "vine_task.h"
#include "vine_worker_info.h"

struct vine_worker_info *vine_schedule_task_to_worker( struct vine_manager *q, struct vine_task *t );
void vine_schedule_check_for_large_tasks( struct vine_manager *q );
int vine_schedule_check_inputs(struct vine_manager *q, struct vine_task *t);

#endif
