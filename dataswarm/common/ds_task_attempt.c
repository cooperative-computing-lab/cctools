#include "ds_task_attempt.h"

#include <stdlib.h>
#include <string.h>

struct ds_task_attempt * ds_task_attempt_create( struct ds_task *task )
{
	struct ds_task_attempt *t = calloc(1, sizeof(*t));
	t->state = DS_TASK_TRY_NEW;
	t->in_transition = DS_TASK_TRY_NEW;
	t->result = DS_RESULT_PENDING;

	t->task = task;
	t->next = task->attempts;
	task->attempts = t;

	return t;
}
