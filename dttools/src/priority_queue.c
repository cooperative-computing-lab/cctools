/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "priority_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define DEFAULT_CAPACITY 127
#define MAX_PRIORITY DBL_MAX

struct element {
    void *data;
    double priority;
};

struct priority_queue {
    int size;
    int capacity;
    struct element **elements;
};

struct priority_queue *priority_queue_create(double init_capacity) {
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

    return pq;
}

int priority_queue_size(struct priority_queue *pq) {
    if (!pq) return -1;

    return pq->size;
}

void swap_elements(struct priority_queue *pq, int i, int j) {
    struct element *temp = pq->elements[i];
    pq->elements[i] = pq->elements[j];
    pq->elements[j] = temp;
}

void swim(struct priority_queue *pq, int k) {
    if (!pq) return;

    while (k > 1 && pq->elements[k / 2]->priority < pq->elements[k]->priority) {
        swap_elements(pq, k, k / 2);
        k /= 2;
    }
}

void sink(struct priority_queue *pq, int k) {
    if (!pq) return;

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
}

int priority_queue_double_capacity(struct priority_queue *pq) {
    if (!pq) return 0;

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

int priority_queue_push(struct priority_queue *pq, void *data, int priority) {
    if (!pq) return 0;

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
    swim(pq, pq->size);

    return 1;
}

void *priority_queue_pop(struct priority_queue *pq) {
    if (!pq || pq->size == 0) return NULL;

    struct element *e = pq->elements[1];
    void *data = e->data;
    pq->elements[1] = pq->elements[pq->size];
    pq->elements[pq->size--] = NULL;
    sink(pq, 1);
    free(e);

    return data;
}

void *priority_queue_get_head(struct priority_queue *pq) {
    if (!pq || pq->size == 0) return NULL;

    return pq->elements[1]->data;
}

void *priority_queue_get_element(struct priority_queue *pq, int index) {
    if (!pq || pq->size < 1 || index < 1 || index > pq->size) return NULL;

    return pq->elements[index]->data;
}

double priority_queue_get_max_priority(struct priority_queue *pq) {
    if (!pq || pq->size == 0) return NULL;

    return pq->elements[1]->priority;
}

double priority_queue_get_min_priority(struct priority_queue *pq) {
    if (!pq || pq->size == 0) return NULL;

    double min_priority = pq->elements[1]->priority;

    for (int i = 2; i <= pq->size; i++) {
        min_priority = pq->elements[i]->priority < min_priority ? pq->elements[i]->priority : min_priority;
    }

    return min_priority;
}


int priority_queue_remove(struct priority_queue *pq, void *data) {
    if (!pq) return 0;

    for (int i = 1; i <= pq->size; i++) {
        if (pq->elements[i]->data == data) {
            struct element *e = pq->elements[i];
            pq->elements[i] = pq->elements[pq->size];
            pq->elements[pq->size--] = NULL;
            sink(pq, i);
            free(e);
            return 1;
        }
    }
    return 0;
}

void priority_queue_destroy(struct priority_queue *pq) {
    if (!pq) return;

    for (int i = 0; i <= pq->size; i++) {
        free(pq->elements[i]);
    }
    free(pq->elements);
    free(pq);
}