/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_task_groups.h"
#include "debug.h"
#include "vine_mount.h"
#include "vine_task.h"
#include "stringtools.h"

// create a new task group for this task based on the temp mount file
static int vine_task_groups_create_group(struct vine_manager *q, struct vine_task *t, struct vine_mount *m)
{
	int id = q->group_id_counter++;
	struct list *l = list_create();

	t->group_id = id;

	struct vine_task *tc = vine_task_addref(t);

	list_push_head(l, tc);
	itable_insert(q->task_group_table, id, l);
	return 1;
}

// locate the group with the task which outputs the desired file, and add the new task
static int vine_task_groups_add_to_group(struct vine_manager *q, struct vine_task *t, struct vine_mount *m)
{
	int id = m->file->recovery_task->group_id;

	if (id) {
		struct list *group = itable_lookup(q->task_group_table, id);
		t->group_id = id;
		struct vine_task *tc = vine_task_addref(t);
		list_push_tail(group, tc);
	}

	return 0;
}

/*
When a task comes in through vine_submit, look for temp files in its inputs/outputs
If there is a temp file on the input there is already a task group it should be assigned to.
If there is only a temp output it would be the first of a new group.
*/
int vine_task_groups_assign_task(struct vine_manager *q, struct vine_task *t)
{
	struct vine_mount *input_mount;
	struct vine_mount *output_mount;

	int inputs_present = 0;
	int outputs_present = 0;

	LIST_ITERATE(t->input_mounts, input_mount)
	{
		if (input_mount->file->type == VINE_TEMP) {
			inputs_present++;
			break;
		}
	}

	LIST_ITERATE(t->output_mounts, output_mount)
	{
		if (output_mount->file->type == VINE_TEMP) {
			outputs_present++;
			break;
		}
	}

	// could also be inputs_present && outputs_present
	if (inputs_present) {
		vine_task_groups_add_to_group(q, t, input_mount);
		debug(D_VINE, "Assigned task to group %d", t->group_id);
	} else if (outputs_present) {
		vine_task_groups_create_group(q, t, output_mount);
		debug(D_VINE, "Create task with group %d", t->group_id);
	}

	return inputs_present || outputs_present;
}

static void vine_task_group_delete(struct list *l)
{
	if (l) {
		list_delete(l);
	}
}

void vine_task_groups_clear(struct vine_manager *q)
{
	itable_clear(q->task_group_table, (void *)vine_task_group_delete);
}
