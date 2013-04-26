/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"
#include "list.h"

struct work_queue_task *work_queue_wait_internal(struct work_queue *q, int timeout, struct list *aux_links, struct list *active_aux_links);


