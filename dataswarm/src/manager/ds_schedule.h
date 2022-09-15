/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_SCHEDULE_H
#define DS_SCHEDULE_H

#include "ds_manager.h"
#include "ds_task.h"
#include "ds_worker_info.h"

struct ds_worker_info *ds_schedule_task_to_worker( struct ds_manager *q, struct ds_task *t );
void ds_schedule_check_for_large_tasks( struct ds_manager *q );

#endif
