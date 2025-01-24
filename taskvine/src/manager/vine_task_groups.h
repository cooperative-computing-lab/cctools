/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "taskvine.h"
#include "vine_manager.h"
#include "uuid.h"


int vine_task_groups_assign_task(struct vine_manager *q, struct vine_task *t);

void vine_task_groups_clear(struct vine_manager *q);
