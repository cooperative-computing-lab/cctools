/*OA
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_xpus.h"
#include "buffer.h"
#include "debug.h"

struct vine_xpus {
	const char *name;
	/* Total number of initialized cores */
	int count;
	/* Array tracks which task is assigned to each core. */
	int *core_to_task;
};

struct vine_xpus *vine_xpus_create(const char *name, int count)
{
	struct vine_xpus *x = malloc(sizeof(*x));
	x->name = strdup(name);
	x->count = count;
	x->core_to_task = calloc(count, sizeof(int));
	return x;
}

/*
Display the cores associated with each task.
*/

void vine_xpus_debug(struct vine_xpus *x)
{
	buffer_t b;
	buffer_init(&b);
	buffer_putfstring(&b, "%s Assigned to Tasks: [ ", x->name);
	int i;
	for (i = 0; i < x->count; i++) {
		buffer_putfstring(&b, "%d ", x->core_to_task[i]);
	}
	buffer_putfstring(&b, " ]");
	debug(D_VINE, "%s", buffer_tostring(&b));
	buffer_free(&b);
}

/*
Free all of the cores associated with this taskid.
*/

void vine_xpus_free(struct vine_xpus *x, int taskid)
{
	int i;
	for (i = 0; i < x->count; i++) {
		if (x->core_to_task[i] == taskid) {
			x->core_to_task[i] = 0;
		}
	}
}

/*
Allocate n specific cores to the given task.
This assumes the total number of cores has been
accurately tracked: this function will fatal()
if not enough are available.
*/

void vine_xpus_alloc(struct vine_xpus *x, int n, int taskid)
{
	int i;
	for (i = 0; i < x->count && n > 0; i++) {
		if (x->core_to_task[i] == 0) {
			x->core_to_task[i] = taskid;
			n--;
		}
	}

	if (n > 0)
		fatal("vine_xpus_allocate: accounting error: ran out of cores to assign!");

	vine_xpus_debug(x);
}

/*
Return a string representing the cores allocated to taskid.
For example, if cores 1 and 3 are allocated, return "1,3"
This string must be freed after use.
*/

char *vine_xpus_to_string(struct vine_xpus *x, int taskid)
{
	int i;
	int first = 1;
	buffer_t b;
	buffer_init(&b);
	for (i = 0; i < x->count; i++) {
		if (x->core_to_task[i] == taskid) {
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
