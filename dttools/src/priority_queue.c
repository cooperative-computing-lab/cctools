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

int priority_queue_update_priority_at(struct priority_queue *pq, int idx, double new_priority)
{
	if (!pq || idx < 0 || idx >= pq->size) {
		return -1;
	}

	double old_priority = pq->elements[idx]->priority;
	pq->elements[idx]->priority = new_priority;

	if (new_priority > old_priority) {
		return swim(pq, idx);
	} else if (new_priority < old_priority) {
		return sink(pq, idx);
	}
	return idx;
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
		return NAN;
	}

	return pq->elements[idx]->priority;
}

void *priority_queue_peek_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NULL;
	}

	return pq->elements[idx]->data;
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

int priority_queue_remove_at(struct priority_queue *pq, int idx)
{
	if (!pq || idx < 0 || idx > pq->size - 1) {
		return 0;
	}

	struct element *to_delete = pq->elements[idx];
	struct element *last_elem = pq->elements[pq->size - 1];

	double old_priority = to_delete->priority;
	double new_priority = last_elem->priority;

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

	free(to_delete);

	return 1;
}

struct priority_queue *priority_queue_duplicate(struct priority_queue *src)
{
	if (!src) {
		return NULL;
	}

	struct priority_queue *copy = priority_queue_create(src->capacity);
	if (!copy) {
		return NULL;
	}

	copy->size = src->size;

	for (int i = 0; i < src->size; i++) {
		copy->elements[i] = malloc(sizeof(struct element));
		if (!copy->elements[i]) {
			priority_queue_delete(copy);
			return NULL;
		}
		copy->elements[i]->data = src->elements[i]->data;
		copy->elements[i]->priority = src->elements[i]->priority;
	}

	return copy;
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

void *priority_queue_internal_peek_data(struct priority_queue *pq, int idx)
{
	if (!pq || idx < 0 || idx >= pq->size) {
		return NULL;
	}
	return pq->elements[idx]->data;
}