/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "priority_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>

#define DEFAULT_CAPACITY 127
#define MAX_PRIORITY DBL_MAX
#define MIN_PRIORITY DBL_MIN

struct element {
	void *data;
	double priority;
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;
	int base_cursor;   // iterate from the left to the right, reset at each iteration
	int static_cursor; // iterate from the left to the right, never reset
	int rotate_cursor; // iterate from the left to the right, reset when needed
};

/****** Static Methods ******/

static void swap_elements(struct priority_queue *pq, int i, int j)
{
	struct element *temp = pq->elements[i];
	pq->elements[i] = pq->elements[j];
	pq->elements[j] = temp;
}

static int swim(struct priority_queue *pq, int k)
{
	if (!pq)
		return 1;

	while (k > 1 && pq->elements[k / 2]->priority < pq->elements[k]->priority) {
		swap_elements(pq, k, k / 2);
		k /= 2;
	}

	return k;
}

static int swim_upward(struct priority_queue *pq, int k)
{
	if (!pq)
		return 1;

	while (k > 1 && pq->elements[k / 2]->priority <= pq->elements[k]->priority) {
		swap_elements(pq, k, k / 2);
		k /= 2;
	}

	return k;
}

static int sink(struct priority_queue *pq, int k)
{
	if (!pq)
		return -1;

	while (2 * k <= pq->size) {
		int j = 2 * k;
		if (j < pq->size && pq->elements[j]->priority < pq->elements[j + 1]->priority) {
			j++;
		}
		if (pq->elements[k]->priority >= pq->elements[j]->priority) {
			break;
		}
		swap_elements(pq, k, j);
		k = j;
	}

	return k;
}

static int priority_queue_double_capacity(struct priority_queue *pq)
{
	if (!pq)
		return 0;

	int new_capacity = pq->capacity * 2;
	struct element **new_elements = (struct element **)malloc(sizeof(struct element *) * (new_capacity + 1));
	if (!new_elements) {
		return 0;
	}

	memcpy(new_elements, pq->elements, sizeof(struct element *) * (pq->size + 1));

	free(pq->elements);
	pq->elements = new_elements;
	pq->capacity = new_capacity;

	return 1;
}

/****** External Methods ******/

struct priority_queue *priority_queue_create(double init_capacity)
{
	struct priority_queue *pq = (struct priority_queue *)malloc(sizeof(struct priority_queue));
	if (!pq)
		return NULL;

	if (init_capacity < 1) {
		init_capacity = DEFAULT_CAPACITY;
	}

	pq->elements = (struct element **)calloc(init_capacity + 1, sizeof(struct element *));
	if (!pq->elements) {
		free(pq);
		return NULL;
	}

	pq->capacity = init_capacity;
	pq->size = 0;

	/* The 0th element is used as a sentinel with the highest priority,
	    which is in order to simplify boundary checks in heap operations like swim and sink. */
	pq->elements[0] = (struct element *)calloc(1, sizeof(struct element));
	if (!pq->elements[0]) {
		free(pq->elements);
		free(pq);
		return NULL;
	}

	pq->elements[0]->priority = MAX_PRIORITY;

	pq->static_cursor = 0;
	pq->base_cursor = 0;
	pq->rotate_cursor = 0;

	return pq;
}

int priority_queue_size(struct priority_queue *pq)
{
	if (!pq)
		return -1;

	return pq->size;
}

int priority_queue_push(struct priority_queue *pq, void *data, double priority)
{
	if (!pq)
		return 0;

	if (pq->size >= pq->capacity) {
		if (!priority_queue_double_capacity(pq)) {
			return 0;
		}
	}
	struct element *e = (struct element *)malloc(sizeof(struct element));
	if (!e) {
		return 0;
	}
	e->data = data;
	e->priority = priority;

	pq->elements[++pq->size] = e;

	int new_idx = swim(pq, pq->size);

	if (new_idx <= pq->rotate_cursor) {
		// reset the rotate cursor if the new element is inserted before/equal to it
		priority_queue_rotate_reset(pq);
	}

	return new_idx;
}

int priority_queue_push_upward(struct priority_queue *pq, void *data, double priority)
{
	if (!pq)
		return 0;

	if (pq->size >= pq->capacity) {
		if (!priority_queue_double_capacity(pq)) {
			return 0;
		}
	}
	struct element *e = (struct element *)malloc(sizeof(struct element));
	if (!e) {
		return 0;
	}
	e->data = data;
	e->priority = priority;

	pq->elements[++pq->size] = e;

	int new_idx = swim_upward(pq, pq->size);

	if (new_idx <= pq->rotate_cursor) {
		// reset the rotate cursor if the new element is inserted before/equal to it
		priority_queue_rotate_reset(pq);
	}

	return new_idx;
}

void *priority_queue_pop(struct priority_queue *pq)
{
	if (!pq || pq->size == 0)
		return NULL;

	struct element *e = pq->elements[1];
	void *data = e->data;
	pq->elements[1] = pq->elements[pq->size];
	pq->elements[pq->size--] = NULL;
	sink(pq, 1);
	free(e);

	return data;
}

void *priority_queue_get_head(struct priority_queue *pq)
{
	if (!pq || pq->size == 0)
		return NULL;

	return pq->elements[1]->data;
}

double priority_queue_get_priority(struct priority_queue *pq, int index)
{
	if (!pq || pq->size < 1 || index < 1 || index > pq->size)
		return NAN;

	return pq->elements[index]->priority;
}

void *priority_queue_get_element(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 1 || idx > pq->size)
		return NULL;

	return pq->elements[idx]->data;
}

int priority_queue_update_priority(struct priority_queue *pq, void *data, double new_priority)
{
	if (!pq)
		return 0;

	int idx = -1;
	for (int i = 1; i <= pq->size; i++) {
		if (pq->elements[i]->data == data) {
			idx = i;
			break;
		}
	}

	if (idx == -1) {
		return 0;
	}

	double old_priority = pq->elements[idx]->priority;
	pq->elements[idx]->priority = new_priority;

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
	if (!pq)
		return 0;

	for (int i = 1; i <= pq->size; i++) {
		if (pq->elements[i]->data == data) {
			return i;
		}
	}

	return 0;
}

int priority_queue_static_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0)
		return 0;

	pq->static_cursor++;
	if (pq->static_cursor > pq->size) {
		pq->static_cursor = 1;
	}

	return pq->static_cursor;
}

void priority_queue_base_reset(struct priority_queue *pq)
{
	if (!pq)
		return;

	pq->base_cursor = 0;
}

/*
Advance the base cursor and return it, should be used only in PRIORITY_QUEUE_BASE_ITERATE
*/

int priority_queue_base_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0)
		return 0;

	pq->base_cursor++;
	if (pq->base_cursor > pq->size) {
		priority_queue_base_reset(pq);
		return 0;
	}

	return pq->base_cursor;
}

void priority_queue_rotate_reset(struct priority_queue *pq)
{
	if (!pq)
		return;

	pq->rotate_cursor = 0;
}

int priority_queue_rotate_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0)
		return 0;

	pq->rotate_cursor++;
	if (pq->rotate_cursor > pq->size) {
		pq->rotate_cursor = 1;
	}

	return pq->rotate_cursor;
}

int priority_queue_remove(struct priority_queue *pq, int idx)
{
	if (!pq || idx < 1 || idx > pq->size)
		return 0;

	struct element *e = pq->elements[idx];
	pq->elements[idx] = pq->elements[pq->size];
	pq->elements[pq->size--] = NULL;

	sink(pq, idx);

	if (pq->static_cursor == idx) {
		pq->static_cursor--;
	}
	if (pq->base_cursor == idx) {
		pq->base_cursor--;
	}
	if (pq->rotate_cursor == idx) {
		pq->rotate_cursor--;
	}
	free(e);

	if (idx <= pq->rotate_cursor) {
		// reset the rotate cursor if the removed element is before/equal to it
		priority_queue_rotate_reset(pq);
	}

	return 1;
}

void priority_queue_delete(struct priority_queue *pq)
{
	if (!pq)
		return;

	for (int i = 0; i <= pq->size; i++) {
		free(pq->elements[i]);
	}
	free(pq->elements);
	free(pq);
}
