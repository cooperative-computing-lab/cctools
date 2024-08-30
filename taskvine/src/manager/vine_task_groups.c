/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_task_groups.h"
#include "debug.h"
#include "vine_mount.h"
#include "vine_task.h"

// create a new task group for this task based on the temp mount file
static int vine_task_groups_create_group(struct vine_manager *q, struct vine_task *t, struct vine_mount *m)
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);
	char *id = strdup(uuid.str);
	struct list *l = list_create();

	t->group_id = id;

	struct vine_task *tc = vine_task_clone(t);

	list_push_head(l, tc);
	hash_table_insert(q->task_group_table, id, l);
	return 1;
}

// locate the group with the task which outputs the desired file, and add the new task
static int vine_task_groups_add_to_group(struct vine_manager *q, struct vine_task *t, struct vine_mount *m)
{
	struct list *l;
	char *id;
	HASH_TABLE_ITERATE(q->task_group_table, id, l)
	{
		struct vine_task *lt;
		LIST_ITERATE(l, lt)
		{
			struct vine_mount *lm;
			LIST_ITERATE(lt->output_mounts, lm)
			{
				if (m->file == lm->file) {
					t->group_id = lt->group_id;
					struct vine_task *tc = vine_task_clone(t);
					list_push_tail(l, tc);
					return 1;
				}
			}
		}
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
		debug(D_VINE, "Assigned task to group %s", t->group_id);
	} else if (outputs_present) {
		vine_task_groups_create_group(q, t, output_mount);
		debug(D_VINE, "Create task with group %s", t->group_id);
	}

	return inputs_present || outputs_present;
}
