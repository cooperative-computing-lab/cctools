/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_xpu_tracker.h"
#include "buffer.h"
#include "debug.h"

struct vine_xpu_tracker {
	const char *name;	// name of units: cpus, gpus, etc.
	int count;		// number of available units
	int *unit_to_task;	// array indicating task assigned to each unit.
};

/*
Create a new tracker for units of type "name".
*/

struct vine_xpu_tracker *vine_xpu_tracker_create(const char *name, int count)
{
	struct vine_xpu_tracker *x = malloc(sizeof(*x));
	x->name = strdup(name);
	x->count = count;
	x->unit_to_task = calloc(count, sizeof(int));
	return x;
}

/*
Display the units associated with each task to the debug log
*/

void vine_xpu_tracker_debug(struct vine_xpu_tracker *x)
{
	buffer_t b;
	buffer_init(&b);
	buffer_putfstring(&b, "%s assigned to tasks: [ ", x->name);
	int i;
	for (i = 0; i < x->count; i++) {
		buffer_putfstring(&b, "%d ", x->unit_to_task[i]);
	}
	buffer_putfstring(&b, " ]");
	debug(D_VINE, "%s", buffer_tostring(&b));
	buffer_free(&b);
}

/*
Free all of the units associated with this taskid.
*/

void vine_xpu_tracker_free(struct vine_xpu_tracker *x, int taskid)
{
	int i;
	for (i = 0; i < x->count; i++) {
		if (x->unit_to_task[i] == taskid) {
			x->unit_to_task[i] = 0;
		}
	}
}

/*
Allocate n specific units to the given task.
This assumes the total number of units has been
accurately tracked: this function will fatal()
if not enough are available.
*/

void vine_xpu_tracker_alloc(struct vine_xpu_tracker *x, int n, int taskid)
{
	int i;
	for (i = 0; i < x->count && n > 0; i++) {
		if (x->unit_to_task[i] == 0) {
			x->unit_to_task[i] = taskid;
			n--;
		}
	}

	if (n > 0)
		fatal("vine_xpu_tracker_alloc: accounting error: ran out of %ss to assign!",x->name);

	vine_xpu_tracker_debug(x);
}

/*
Return a string representing the units allocated to taskid.
For example, if units 1 and 3 are allocated, return "1,3"
This string must be freed after use.
*/

char *vine_xpu_tracker_to_string(struct vine_xpu_tracker *x, int taskid)
{
	int i;
	int first = 1;
	buffer_t b;
	buffer_init(&b);
	for (i = 0; i < x->count; i++) {
		if (x->unit_to_task[i] == taskid) {
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
