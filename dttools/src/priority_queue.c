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
#include <stdint.h>

#define DEFAULT_CAPACITY 127
#define MAX_PRIORITY DBL_MAX
#define MIN_PRIORITY DBL_MIN

struct element {
	void *data;
	double priority; // In this implementation, elements with bigger priorities are considered to be privileged.
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;
	struct hash_table *index_table; // Hash table for data to index mapping

	/* The following three cursors are used to iterate over the elements in the numerical order they are stored in the array, which is
	   different from the order of priorities.  Each of them has different concerns when traverse the queue Though the tipical priority-based
	   traversal is done by the repeated invocation of priority_queue_peak_top and priority_queue_pop APIs, rather than using any cursors. */
	int base_cursor;   // Used in PRIORITY_QUEUE_BASE_ITERATE. It iterates from the first position and never be reset automatically.
	int static_cursor; // Used in PRIORITY_QUEUE_STATIC_ITERATE. It iterates from the last position and never be reset automatically.
	int rotate_cursor; // Used in PRIORITY_QUEUE_ROTATE_ITERATE. It iterates from the last position and can be reset when certain events happen.
};

/****** Static Methods ******/

static void swap_elements(struct priority_queue *pq, int i, int j)
{
	struct element *temp = pq->elements[i];
	pq->elements[i] = pq->elements[j];
	pq->elements[j] = temp;

	// Update indices in hash table
	hash_table_insert(pq->index_table, (char *)pq->elements[i]->data, (void *)(intptr_t)i);
	hash_table_insert(pq->index_table, (char *)pq->elements[j]->data, (void *)(intptr_t)j);
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

struct priority_queue *priority_queue_create(double init_capacity)
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

	pq->static_cursor = 0;
	pq->base_cursor = 0;
	pq->rotate_cursor = 0;

	pq->index_table = hash_table_create(0, NULL); // Initialize hash table
	if (!pq->index_table) {
		free(pq->elements);
		free(pq);
		return NULL;
	}

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
		return 0;
	}

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

	pq->elements[pq->size] = e;

	// Insert data pointer and index into hash table
	if (!hash_table_insert(pq->index_table, (char *)data, (void *)(intptr_t)pq->size)) {
		free(e);
		return 0;
	}

	int new_idx = swim(pq, pq->size++);
	if (new_idx <= pq->rotate_cursor) {
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

void *priority_queue_peak_top(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return NULL;
	}

	return pq->elements[0]->data;
}

double priority_queue_get_priority(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NAN;
	}

	return pq->elements[idx]->priority;
}

void *priority_queue_peak_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NULL;
	}

	return pq->elements[idx]->data;
}

int priority_queue_update_priority(struct priority_queue *pq, void *data, double new_priority)
{
	if (!pq) {
		return 0;
	}

	int idx = -1;
	for (int i = 0; i < pq->size; i++) {
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
	if (!pq) {
		return -1;
	}

	void *idx_ptr = hash_table_lookup(pq->index_table, (char *)data);
	if (!idx_ptr) {
		return -1;
	}

	return (int)(intptr_t)idx_ptr;
}

int priority_queue_static_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return 0;
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
		return 0;
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
		return 0;
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

	struct element *e = pq->elements[idx];
	pq->size--;
	pq->elements[idx] = pq->elements[pq->size];
	pq->elements[pq->size] = NULL;

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
	if (!pq) {
		return;
	}

	for (int i = 0; i < pq->size; i++) {
		free(pq->elements[i]);
	}
	free(pq->elements);
	hash_table_delete(pq->index_table);
	free(pq);
}
