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
#include <stdarg.h>

#define DEFAULT_CAPACITY 127

struct element {
	void *data;

	// Priorities ar keps in this array, compared in order.
	// For two elements e1 and e2, e1 is considered to have higher priority than e2 if
	// e1->priority[0] > e2->priority[0] or e1->priority[0] == e2->priority[0] and e1->priority[1] > e2->priority[1] or ...
	// Larger numbers have higher priority.
	double *priority;
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;

	int priority_count; // Number of priorities per element. Priorities are compared
			    // in the order priority[] -> priority[1] -> ... -> priority[priority_count-1]

	/* The following three cursors are used to iterate over the elements in the numerical order they are stored in the array, which is
	   different from the order of priorities.  Each of them has different concerns when traverse the queue Though the typical priority-based
	   traversal is done by the repeated invocation of priority_queue_peek_top and priority_queue_pop APIs, rather than using any cursors. */
	int base_cursor;   // Used in PRIORITY_QUEUE_BASE_ITERATE. It iterates from the first position and never be reset automatically.
	int static_cursor; // Used in PRIORITY_QUEUE_STATIC_ITERATE. It iterates from the last position and never be reset automatically.
	int rotate_cursor; // Used in PRIORITY_QUEUE_ROTATE_ITERATE. It iterates from the last position and can be reset when certain events happen.
};

/****** Static Methods ******/

static struct element *element_create(void *data, int priority_count, const double *priority)
{
	struct element *e = (struct element *)malloc(sizeof(struct element));

	e->data = data;

	if (priority) {
		e->priority = (double *)malloc(priority_count * sizeof(double));
		if (!e->priority) {
			free(e);
			return NULL;
		}
		memcpy(e->priority, priority, priority_count * sizeof(double));
	} else {
		e->priority = NULL;
	}

	return e;
}

static void element_delete(struct element *e)
{
	free(e->priority);
	free(e);
}

static int cmp_left_right(struct priority_queue *pq, int left_idx, int right_idx)
{
	double *left = pq->elements[left_idx]->priority;
	double *right = pq->elements[right_idx]->priority;

	for (int i = 0; i < pq->priority_count; i++) {
		if (left[i] < right[i]) {
			return -1;
		}

		if (left[i] > right[i]) {
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

	if (priority_count < 1) {
		fatal("Priority count must be at least 1.\n");
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

/** Push to the queue without duplicating priotiry array. */
int push_internal(struct priority_queue *pq, void *data, double *priority)
{
	if (!pq) {
		return -1;
	}

	if (pq->size >= pq->capacity) {
		if (!priority_queue_double_capacity(pq)) {
			return -1;
		}
	}

	struct element *e = element_create(data, pq->priority_count, NULL);
	if (!e) {
		return -1;
	}
	e->priority = priority;

	pq->elements[pq->size++] = e;

	int new_idx = swim(pq, pq->size - 1);

	if (new_idx <= pq->rotate_cursor) {
		// reset the rotate cursor if the new element is inserted before/equal to it
		priority_queue_rotate_reset(pq);
	}

	return new_idx;
}

int priority_queue_push(struct priority_queue *pq, void *data, const double *priority)
{
	if (!pq) {
		return -1;
	}

	double *priority_copy = (double *)malloc(pq->priority_count * sizeof(double));
	if (!priority_copy) {
		return -1;
	}

	memcpy(priority_copy, priority, pq->priority_count * sizeof(double));
	return push_internal(pq, data, priority_copy);
}

int priority_queue_push_varargs(struct priority_queue *pq, void *data, ...)
{
	if (!pq) {
		return -1;
	}

	// Allocate array for priorities
	double *priority = (double *)malloc(pq->priority_count * sizeof(double));
	if (!priority) {
		return -1;
	}

	// Collect variable arguments into the priority array
	va_list args;
	va_start(args, data);
	for (int i = 0; i < pq->priority_count; i++) {
		priority[i] = va_arg(args, double);
	}
	va_end(args);

	// Call the standard push function
	// Note: push_internal takes ownership of the priority array, so we don't free it here
	return push_internal(pq, data, priority);
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
	element_delete(e);

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

	return pq->elements[element_index]->priority[priority_idx];
}

double priority_queue_get_top_priority(struct priority_queue *pq)
{
	if (!pq || pq->size < 1) {
		return 0;
	}

	return pq->elements[0]->priority[0];
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

	double old_priority = pq->elements[idx]->priority[priority_idx];
	pq->elements[idx]->priority[priority_idx] = new_priority;

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

	element_delete(to_delete);

	return 1;
}

void priority_queue_delete(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	for (int i = 0; i < pq->size; i++) {
		if (pq->elements[i]) {
			element_delete(pq->elements[i]);
		}
	}
	free(pq->elements);
	free(pq);
}
