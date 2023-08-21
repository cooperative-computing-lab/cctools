/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_gpus.h"
#include "buffer.h"
#include "debug.h"
#include "vine_resources.h"

extern struct vine_resources *total_resources;

/* Array tracks which task is assigned to each GPU. */
static int *gpu_to_task = 0;

/*
Initialize the GPU tracking state.
Note that this may be called many times,
but should only initialized once.
*/

void vine_gpus_init(int ngpus)
{
	if (!gpu_to_task)
		gpu_to_task = calloc(ngpus, sizeof(int));
}

/*
Display the GPUs associated with each task.
*/

void vine_gpus_debug()
{
	buffer_t b;
	buffer_init(&b);
	buffer_putfstring(&b, "GPUs Assigned to Tasks: [ ");
	int i;
	for (i = 0; i < total_resources->gpus.total; i++) {
		buffer_putfstring(&b, "%d ", gpu_to_task[i]);
	}
	buffer_putfstring(&b, " ]");
	debug(D_VINE, "%s", buffer_tostring(&b));
	buffer_free(&b);
}

/*
Free all of the GPUs associated with this task_id.
*/

void vine_gpus_free(int task_id)
{
	int i;
	for (i = 0; i < total_resources->gpus.total; i++) {
		if (gpu_to_task[i] == task_id) {
			gpu_to_task[i] = 0;
		}
	}
}

/*
Allocate n specific GPUs to the given task.
This assumes the total number of GPUs has been
accurately tracked: this function will fatal()
if not enough are available.
*/

void vine_gpus_allocate(int n, int task)
{
	int i;
	for (i = 0; i < total_resources->gpus.total && n > 0; i++) {
		if (gpu_to_task[i] == 0) {
			gpu_to_task[i] = task;
			n--;
		}
	}

	if (n > 0)
		fatal("vine_gpus_allocate: accounting error: ran out of gpus to assign!");

	vine_gpus_debug();
}

/*
Return a string representing the GPUs allocated to task_id.
For example, if GPUs 1 and 3 are allocated, return "1,3"
This string must be freed after use.
*/

char *vine_gpus_to_string(int task_id)
{
	int i;
	int first = 1;
	buffer_t b;
	buffer_init(&b);
	for (i = 0; i < total_resources->gpus.total; i++) {
		if (gpu_to_task[i] == task_id) {
			if (first) {
				first = 0;
			} else {
				buffer_putfstring(&b, ",");
			}
			buffer_putfstring(&b, "%d", i);
		}
	}
	char *str = strdup(buffer_tostring(&b));
	buffer_free(&b);
	return str;
}
