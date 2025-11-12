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

struct element {
	void *data;
	double priority; // In this implementation, elements with bigger priorities are considered to be privileged.
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;

	int base_cursor; // Used in PRIORITY_QUEUE_BASE_ITERATE. It iterates from the first position and never be reset automatically.
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
	if (!pq) {
		return 1;
	}

	while (k > 0 && pq->elements[(k - 1) / 2]->priority <= pq->elements[k]->priority) {
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
		if (j + 1 < pq->size && pq->elements[j]->priority <= pq->elements[j + 1]->priority) {
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

struct priority_queue *priority_queue_create(int init_capacity)
{
	struct priority_queue *pq = (struct priority_queue *)malloc(sizeof(struct priority_queue));
	if (!pq) {
		return NULL;
	}

	if (init_capacity < 1) {
		init_capacity = DEFAULT_CAPACITY;
	}

	pq->elements = (struct element **)calloc(init_capacity, sizeof(struct element *));
	if (!pq->elements) {
		free(pq);
		fprintf(stderr, "Fatal error: Memory allocation failed.\n");
		exit(EXIT_FAILURE);
		return NULL;
	}

	pq->capacity = init_capacity;
	pq->size = 0;

	pq->base_cursor = 0;

	return pq;
}

int priority_queue_size(struct priority_queue *pq)
{
	if (!pq) {
		return -1;
	}

	return pq->size;
}

int priority_queue_push(struct priority_queue *pq, void *data, double priority)
{
	if (!pq) {
		return -1;
	}

	if (pq->size >= pq->capacity) {
		if (!priority_queue_double_capacity(pq)) {
			return -1;
		}
	}
	struct element *e = (struct element *)malloc(sizeof(struct element));
	if (!e) {
		return -1;
	}
	e->data = data;
	e->priority = priority;

	pq->elements[pq->size++] = e;

	int new_idx = swim(pq, pq->size - 1);

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

double priority_queue_get_priority_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return 0;
	}

	return pq->elements[idx]->priority;
}

double priority_queue_get_top_priority(struct priority_queue *pq)
{
	if (!pq || pq->size < 1) {
		return 0;
	}

	return pq->elements[0]->priority;
}

void *priority_queue_peek_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NULL;
	}

	return pq->elements[idx]->data;
}

int priority_queue_update_priority(struct priority_queue *pq, void *data, double new_priority)
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

	/* If the data isn’t already in the queue, enqueue it. */
	if (idx == -1) {
		return priority_queue_push(pq, data, new_priority);
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

int priority_queue_remove(struct priority_queue *pq, int idx)
{
	if (!pq || idx < 0 || idx > pq->size - 1) {
		return 0;
	}

	struct element *to_delete = pq->elements[idx];
	struct element *last_elem = pq->elements[pq->size - 1];

	double old_priority = to_delete->priority;
	double new_priority = last_elem->priority;

	free(to_delete);

	pq->size--;
	if (idx != pq->size) {
		pq->elements[idx] = last_elem;
		pq->elements[pq->size] = NULL;

		if (new_priority > old_priority) {
			swim(pq, idx);
		} else if (new_priority < old_priority) {
			sink(pq, idx);
		}
	} else {
		pq->elements[pq->size] = NULL;
	}

	if (pq->base_cursor == idx && pq->base_cursor > 0) {
		pq->base_cursor--;
	}

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
