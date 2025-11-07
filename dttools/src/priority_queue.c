/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "priority_queue.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <stddef.h>

#define DEFAULT_CAPACITY 127

/* This is the maximum number of priorities defined in struct element. */
#define MAX_PRIORITY_COUNT 3

struct element {
	void *data;

	// Priorities are stored as three separate variables, compared in order.
	// For two elements e1 and e2, e1 is considered to have higher priority than e2 if
	// e1->priority_0 > e2->priority_0 or e1->priority_0 == e2->priority_0 and e1->priority_1 > e2->priority_1 or ...
	// Larger numbers have higher priority.

	// WARNING: This attributes should be the last defined in this struct.
	double priority_0;
	double priority_1;
	double priority_2;
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;

	int priority_count; // Number of priorities per element. Priorities are compared
			    // in the order priority0 -> priority1 -> ...

	/* The following three cursors are used to iterate over the elements in the numerical order they are stored in the array, which is
	   different from the order of priorities.  Each of them has different concerns when traverse the queue Though the typical priority-based
	   traversal is done by the repeated invocation of priority_queue_peek_top and priority_queue_pop APIs, rather than using any cursors. */
	int base_cursor;   // Used in PRIORITY_QUEUE_BASE_ITERATE. It iterates from the first position and never be reset automatically.
	int static_cursor; // Used in PRIORITY_QUEUE_STATIC_ITERATE. It iterates from the last position and never be reset automatically.
	int rotate_cursor; // Used in PRIORITY_QUEUE_ROTATE_ITERATE. It iterates from the last position and can be reset when certain events happen.
};

/****** Static Methods ******/
static int cmp_left_right(struct priority_queue *pq, int left_idx, int right_idx)
{
	struct element *left = pq->elements[left_idx];
	struct element *right = pq->elements[right_idx];

	// Compare priorities in order
	for (int i = 0; i < pq->priority_count; i++) {
		double left_priority = *((double *)((char *)left + offsetof(struct element, priority_0) + i * sizeof(double)));
		double right_priority = *((double *)((char *)right + offsetof(struct element, priority_0) + i * sizeof(double)));

		if (left_priority < right_priority) {
			return -1;
		}
		if (left_priority > right_priority) {
			return 1;
		}
	}

	// If all priorities are equal, return 0
	return 0;
}

#define LE(p, l, r) (cmp_left_right(p, l, r) <= 0)
#define LT(p, l, r) (cmp_left_right(p, l, r) < 0)
#define GT(p, l, r) (cmp_left_right(p, l, r) > 0)
#define GE(p, l, r) (cmp_left_right(p, l, r) >= 0)
#define EQ(p, l, r) (cmp_left_right(p, l, r) == 0)

static void swap_elements(struct priority_queue *pq, int i, int j)
{
	struct element *temp = pq->elements[i];
	pq->elements[i] = pq->elements[j];
	pq->elements[j] = temp;
}

static int swim(struct priority_queue *pq, int k)
{
	if (!pq) {
		return 1;
	}

	while (k > 0 && LE(pq, (k - 1) / 2, k)) {
		swap_elements(pq, k, (k - 1) / 2);
		k = (k - 1) / 2;
	}

	return k;
}

static int sink(struct priority_queue *pq, int k)
{
	if (!pq) {
		return -1;
	}

	while (2 * k + 1 < pq->size) {
		int j = 2 * k + 1;
		if (j + 1 < pq->size && LE(pq, j, j + 1)) {
			j++;
		}

		if (GE(pq, k, j)) {
			break;
		}

		swap_elements(pq, k, j);
		k = j;
	}

	return k;
}

static int priority_queue_double_capacity(struct priority_queue *pq)
{
	if (!pq) {
		return 0;
	}

	int new_capacity = pq->capacity * 2;
	struct element **new_elements = (struct element **)malloc(sizeof(struct element *) * new_capacity);
	if (!new_elements) {
		return 0;
	}

	memcpy(new_elements, pq->elements, sizeof(struct element *) * pq->size);

	free(pq->elements);
	pq->elements = new_elements;
	pq->capacity = new_capacity;

	return 1;
}

/****** External Methods ******/

struct priority_queue *priority_queue_create(int init_capacity, int priority_count)
{
	struct priority_queue *pq = (struct priority_queue *)malloc(sizeof(struct priority_queue));
	if (!pq) {
		return NULL;
	}

	if (init_capacity < 1) {
		init_capacity = DEFAULT_CAPACITY;
	}

	if (priority_count < 1 || priority_count > MAX_PRIORITY_COUNT) {
		fatal("Priority count must be at least 1 and at most %d.\n", MAX_PRIORITY_COUNT);
		return NULL;
	}

	pq->elements = (struct element **)calloc(init_capacity, sizeof(struct element *));
	if (!pq->elements) {
		fatal("Priority queue memory allocation failed.\n");
		return NULL;
	}

	pq->capacity = init_capacity;
	pq->size = 0;

	pq->priority_count = priority_count;

	pq->static_cursor = 0;
	pq->base_cursor = 0;
	pq->rotate_cursor = 0;

	return pq;
}

int priority_queue_size(struct priority_queue *pq)
{
	if (!pq) {
		return -1;
	}

	return pq->size;
}

int priority_queue_push_array(struct priority_queue *pq, void *data, const double *priorities, size_t priority_count)
{
	if (!pq) {
		return -1;
	}

	if (priority_count != (size_t)pq->priority_count) {
		return -1;
	}

	if (pq->size >= pq->capacity) {
		if (!priority_queue_double_capacity(pq)) {
			return -1;
		}
	}

	struct element *e = (struct element *)calloc(1, sizeof(struct element));
	if (!e) {
		return -1;
	}

	e->data = data;

	// Copy priorities from the array
	memcpy((char *)e + offsetof(struct element, priority_0), priorities, priority_count * sizeof(double));

	pq->elements[pq->size++] = e;

	int new_idx = swim(pq, pq->size - 1);

	if (new_idx <= pq->rotate_cursor) {
		// reset the rotate cursor if the new element is inserted before/equal to it
		priority_queue_rotate_reset(pq);
	}

	return new_idx;
}

void *priority_queue_pop(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return NULL;
	}

	struct element *e = pq->elements[0];
	void *data = e->data;
	pq->elements[0] = pq->elements[--pq->size];
	pq->elements[pq->size] = NULL;
	sink(pq, 0);
	free(e);

	return data;
}

void *priority_queue_peek_top(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return NULL;
	}

	return pq->elements[0]->data;
}

double priority_queue_get_priority_at(struct priority_queue *pq, int priority_idx, int element_index)
{
	if (!pq || pq->size < 1 || element_index < 0 || element_index > pq->size - 1 || priority_idx < 0 || priority_idx >= pq->priority_count) {
		return 0;
	}

	struct element *e = pq->elements[element_index];
	return *((double *)((char *)e + offsetof(struct element, priority_0) + priority_idx * sizeof(double)));
}

double priority_queue_get_top_priority(struct priority_queue *pq)
{
	if (!pq || pq->size < 1) {
		return 0;
	}

	struct element *e = pq->elements[0];
	return *((double *)((char *)e + offsetof(struct element, priority_0) + 0 * sizeof(double)));
}

void *priority_queue_peek_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NULL;
	}

	return pq->elements[idx]->data;
}

int priority_queue_update_priority(struct priority_queue *pq, void *data, int priority_idx, double new_priority)
{
	if (!pq) {
		return -1;
	}

	int idx = -1;
	for (int i = 0; i < pq->size; i++) {
		if (pq->elements[i]->data == data) {
			idx = i;
			break;
		}
	}

	if (idx == -1) {
		return -1;
	}

	struct element *e = pq->elements[idx];

	// Get old priority and set new priority based on priority_idx
	if (priority_idx < 0 || priority_idx >= pq->priority_count) {
		return -1;
	}

	double old_priority = *((double *)((char *)e + offsetof(struct element, priority_0) + priority_idx * sizeof(double)));
	*((double *)((char *)e + offsetof(struct element, priority_0) + priority_idx * sizeof(double))) = new_priority;

	int new_idx = -1;

	if (new_priority > old_priority) {
		new_idx = swim(pq, idx);
	} else if (new_priority < old_priority) {
		new_idx = sink(pq, idx);
	}

	return new_idx;
}

int priority_queue_find_idx(struct priority_queue *pq, void *data)
{
	if (!pq) {
		return -1;
	}

	for (int i = 0; i < pq->size; i++) {
		if (pq->elements[i]->data == data) {
			return i;
		}
	}

	return -1;
}

int priority_queue_static_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int static_idx = pq->static_cursor;
	pq->static_cursor++;

	if (pq->static_cursor > pq->size - 1) {
		pq->static_cursor = 0;
	}

	return static_idx;
}

void priority_queue_base_reset(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	pq->base_cursor = 0;
}

/*
Advance the base cursor and return it, should be used only in PRIORITY_QUEUE_BASE_ITERATE
*/

int priority_queue_base_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int base_idx = pq->base_cursor;
	pq->base_cursor++;

	if (pq->base_cursor > pq->size - 1) {
		priority_queue_base_reset(pq);
	}

	return base_idx;
}

void priority_queue_rotate_reset(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	pq->rotate_cursor = 0;
}

int priority_queue_rotate_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int rotate_idx = pq->rotate_cursor;
	pq->rotate_cursor++;

	if (pq->rotate_cursor > pq->size - 1) {
		priority_queue_rotate_reset(pq);
	}

	return rotate_idx;
}

int priority_queue_remove(struct priority_queue *pq, int idx)
{
	if (!pq || idx < 0 || idx > pq->size - 1) {
		return 0;
	}

	struct element *to_delete = pq->elements[idx];
	struct element *last_elem = pq->elements[pq->size - 1];

	pq->size--;
	if (idx != pq->size) {
		pq->elements[idx] = last_elem;
		pq->elements[pq->size] = NULL;

		if (GT(pq, idx, pq->size - 1)) {
			swim(pq, idx);
		} else if (LT(pq, idx, pq->size - 1)) {
			sink(pq, idx);
		}
	} else {
		pq->elements[pq->size] = NULL;
	}

	if (pq->static_cursor == idx && pq->static_cursor > 0) {
		pq->static_cursor--;
	}
	if (pq->base_cursor == idx && pq->base_cursor > 0) {
		pq->base_cursor--;
	}
	if (pq->rotate_cursor == idx && pq->rotate_cursor > 0) {
		pq->rotate_cursor--;
	}

	// reset the rotate cursor if the removed element is before/equal to it
	if (idx <= pq->rotate_cursor) {
		priority_queue_rotate_reset(pq);
	}

	free(to_delete);

	return 1;
}

void priority_queue_delete(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	for (int i = 0; i < pq->size; i++) {
		if (pq->elements[i]) {
			free(pq->elements[i]);
		}
	}
	free(pq->elements);
	free(pq);
}
