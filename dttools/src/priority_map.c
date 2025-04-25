#include "priority_map.h"
#include "xxmalloc.h"
#include "hash_table.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>

#define DEFAULT_CAPACITY 127

struct element {
	void *data;
	double priority;
	char *key;
};

struct priority_map {
	struct element **elements;
	struct hash_table *key_idx_map;

	int size;
	int capacity;
	pmap_key_generator_t key_generator;
};

struct priority_map *priority_map_create(int init_capacity, pmap_key_generator_t keygen)
{
	struct priority_map *pmap = xxmalloc(sizeof(struct priority_map));
	pmap->size = 0;
	pmap->capacity = init_capacity > 0 ? init_capacity : DEFAULT_CAPACITY;
	pmap->key_generator = keygen;

	pmap->elements = xxmalloc(sizeof(struct element *) * pmap->capacity);
	memset(pmap->elements, 0, sizeof(struct element *) * pmap->capacity);

	pmap->key_idx_map = hash_table_create(pmap->capacity, 0);

	return pmap;
}

static int ensure_capacity(struct priority_map *pmap)
{
	if (pmap->size < pmap->capacity) {
		return 1;
	}

	int new_cap = pmap->capacity * 2;
	struct element **new_elements = xxrealloc(pmap->elements, new_cap * sizeof(struct element *));
	pmap->elements = new_elements;
	pmap->capacity = new_cap;
	return 1;
}

static int update_index(struct priority_map *pmap, const char *key, int idx)
{
	int *idx_ptr = xxmalloc(sizeof(int));
	*idx_ptr = idx;

	int *old_ptr = hash_table_remove(pmap->key_idx_map, key);
	free(old_ptr);

	int ok = hash_table_insert(pmap->key_idx_map, key, idx_ptr);
	assert(ok);

	return ok;
}

static void swap_elements(struct priority_map *pmap, int i, int j)
{
	struct element *ei = pmap->elements[i];
	struct element *ej = pmap->elements[j];

	pmap->elements[i] = ej;
	pmap->elements[j] = ei;

	int ok1 = update_index(pmap, ei->key, j);
	int ok2 = update_index(pmap, ej->key, i);
	assert(ok1 && ok2);
}

static int swim(struct priority_map *pmap, int k)
{
	int original = k;

	while (k > 0) {
		int parent = (k - 1) / 2;
		if (pmap->elements[parent]->priority >= pmap->elements[k]->priority) {
			break;
		}
		swap_elements(pmap, k, parent);
		k = parent;
	}

	if (k == original) {
		struct element *e = pmap->elements[k];
		int ok = update_index(pmap, e->key, k);
		assert(ok);
	}

	return k;
}

static int sink(struct priority_map *pmap, int k)
{
	int original = k;
	int size = pmap->size;

	while (2 * k + 1 < size) {
		int left = 2 * k + 1;
		int right = left + 1;
		int max = left;

		if (right < size && pmap->elements[right]->priority > pmap->elements[left]->priority) {
			max = right;
		}
		if (pmap->elements[k]->priority >= pmap->elements[max]->priority) {
			break;
		}
		swap_elements(pmap, k, max);
		k = max;
	}

	if (k == original) {
		struct element *e = pmap->elements[k];
		int ok = update_index(pmap, e->key, k);
		assert(ok);
	}

	return k;
}

/* by idx */
static int _push_by_idx(struct priority_map *pmap, void *data, double priority, const char *key)
{
	ensure_capacity(pmap);

	struct element *e = xxmalloc(sizeof(struct element));
	e->data = data;
	e->priority = priority;
	e->key = xxstrdup(key);

	int idx = pmap->size++;
	pmap->elements[idx] = e;

	return swim(pmap, idx);
}

static int _update_by_idx(struct priority_map *pmap, int idx, double new_priority)
{
	struct element *e = pmap->elements[idx];
	double old_priority = e->priority;
	e->priority = new_priority;

	if (new_priority > old_priority) {
		return swim(pmap, idx);
	} else if (new_priority < old_priority) {
		return sink(pmap, idx);
	}

	return idx;
}

static double _priority_map_peek_priority_by_idx(struct priority_map *pmap, int idx)
{
	if (!pmap || idx < 0 || idx >= pmap->size) {
		return NAN;
	}

	return pmap->elements[idx]->priority;
}

static void *_remove_by_idx(struct priority_map *pmap, int idx, int return_data)
{
	struct element *e = pmap->elements[idx];
	void *data = return_data ? e->data : NULL;

	hash_table_remove(pmap->key_idx_map, e->key);
	free(e->key);
	free(e);

	int last_idx = --pmap->size;
	if (idx < last_idx) {
		struct element *last = pmap->elements[last_idx];
		pmap->elements[idx] = last;

		int adjusted = swim(pmap, idx);
		if (adjusted == idx) {
			sink(pmap, idx);
		}
	}

	pmap->elements[pmap->size] = NULL;
	return data;
}

/* by key */
static int _push_by_key(struct priority_map *pmap, void *data, double priority, const char *key)
{
	if (hash_table_lookup(pmap->key_idx_map, key)) {
		return 0;
	}
	return _push_by_idx(pmap, data, priority, key);
}

static int _update_by_key(struct priority_map *pmap, const char *key, double new_priority)
{
	int *idx = hash_table_lookup(pmap->key_idx_map, key);
	if (!idx) {
		return 0;
	}

	return _update_by_idx(pmap, *idx, new_priority);
}

static double _priority_map_peek_priority_by_key(struct priority_map *pmap, const char *key)
{
	if (!pmap || !key) {
		return NAN;
	}

	int *idx = hash_table_lookup(pmap->key_idx_map, key);
	if (!idx) {
		return NAN;
	}

	return _priority_map_peek_priority_by_idx(pmap, *idx);
}

static int _remove_by_key(struct priority_map *pmap, const char *key)
{
	int *idx = hash_table_lookup(pmap->key_idx_map, key);
	if (!idx) {
		return 0;
	}
	_remove_by_idx(pmap, *idx, 0);
	return 1;
}

/* by data */
int priority_map_push(struct priority_map *pmap, void *data, double priority)
{
	if (!pmap || !data) {
		return 0;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return 0;
	}

	int ok = _push_by_key(pmap, data, priority, key);
	free(key);
	return ok;
}

int priority_map_update_priority(struct priority_map *pmap, const void *data, double new_priority)
{
	if (!pmap || !data) {
		return 0;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return 0;
	}

	int ok = _update_by_key(pmap, key, new_priority);
	free(key);
	return ok;
}

double priority_map_peek_priority(struct priority_map *pmap, const void *data)
{
	if (!pmap || !data) {
		return NAN;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return NAN;
	}

	double prio = _priority_map_peek_priority_by_key(pmap, key);
	free(key);
	return prio;
}

int priority_map_push_or_update(struct priority_map *pmap, void *data, double priority)
{
	if (!pmap || !data) {
		return 0;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return 0;
	}

	int ok = 0;
	if (hash_table_lookup(pmap->key_idx_map, key)) {
		ok = _update_by_key(pmap, key, priority);
	} else {
		ok = _push_by_key(pmap, data, priority, key);
	}

	free(key);
	return ok;
}

int priority_map_remove(struct priority_map *pmap, const void *data)
{
	if (!pmap || !data) {
		return 0;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return 0;
	}

	int ok = _remove_by_key(pmap, key);
	free(key);
	return ok;
}

void *priority_map_peek_top(struct priority_map *pmap)
{
	if (!pmap || pmap->size == 0) {
		return NULL;
	}

	return pmap->elements[0]->data;
}

void *priority_map_pop(struct priority_map *pmap)
{
	if (!pmap || pmap->size == 0) {
		return NULL;
	}

	return _remove_by_idx(pmap, 0, 1);
}

int priority_map_contains(struct priority_map *pmap, const void *data)
{
	if (!pmap || !data) {
		return 0;
	}

	char *key = pmap->key_generator(data);
	if (!key) {
		return 0;
	}

	int *idx = hash_table_lookup(pmap->key_idx_map, key);
	free(key);
	return idx != NULL;
}

struct priority_map *priority_map_duplicate(struct priority_map *pmap)
{
	if (!pmap) {
		return NULL;
	}

	struct priority_map *copy = xxmalloc(sizeof(struct priority_map));
	copy->size = pmap->size;
	copy->capacity = pmap->capacity;
	copy->key_generator = pmap->key_generator;

	copy->elements = xxmalloc(sizeof(struct element *) * copy->capacity);
	copy->key_idx_map = hash_table_create(copy->capacity, 0);

	for (int i = 0; i < copy->size; i++) {
		struct element *src = pmap->elements[i];
		struct element *dst = xxmalloc(sizeof(struct element));

		dst->priority = src->priority;
		dst->data = NULL;
		dst->key = xxstrdup(src->key);

		copy->elements[i] = dst;

		int *idx_ptr = xxmalloc(sizeof(int));
		*idx_ptr = i;

		int ok = hash_table_insert(copy->key_idx_map, dst->key, idx_ptr);
		assert(ok);
	}

	return copy;
}

int priority_map_size(struct priority_map *pmap)
{
	return pmap ? pmap->size : 0;
}

void priority_map_delete(struct priority_map *pmap)
{
	if (!pmap) {
		return;
	}

	for (int i = 0; i < pmap->size; i++) {
		struct element *e = pmap->elements[i];
		free(e->key);
		free(e);
	}

	free(pmap->elements);
	hash_table_clear(pmap->key_idx_map, free);
	hash_table_delete(pmap->key_idx_map);
	free(pmap);
}

int priority_map_validate(struct priority_map *pmap)
{
	if (!pmap) {
		return 0;
	}

	for (int i = 0; i < pmap->size; i++) {
		int left = 2 * i + 1;
		int right = 2 * i + 2;
		double p = pmap->elements[i]->priority;

		if (isnan(p)) {
			fprintf(stderr, "[heap] NaN priority at idx %d\n", i);
			return 0;
		}

		if (!pmap->elements[i]->key || strlen(pmap->elements[i]->key) == 0) {
			fprintf(stderr, "[heap] null or empty key at idx %d\n", i);
			return 0;
		}

		if (left < pmap->size && p < pmap->elements[left]->priority) {
			fprintf(stderr, "[heap] violation: parent[%d] < left[%d]\n", i, left);
			return 0;
		}

		if (right < pmap->size && p < pmap->elements[right]->priority) {
			fprintf(stderr, "[heap] violation: parent[%d] < right[%d]\n", i, right);
			return 0;
		}
	}

	for (int i = 0; i < pmap->size; i++) {
		const char *key = pmap->elements[i]->key;
		int *idx = hash_table_lookup(pmap->key_idx_map, key);
		if (!idx) {
			fprintf(stderr, "[map] missing key for element[%d]\n", i);
			return 0;
		}

		if (*idx != i) {
			fprintf(stderr, "[map] mismatch: key %s -> %d, expected %d\n", key, *idx, i);
			return 0;
		}
	}

	for (int i = 0; i < pmap->size; i++) {
		for (int j = i + 1; j < pmap->size; j++) {
			if (strcmp(pmap->elements[i]->key, pmap->elements[j]->key) == 0) {
				fprintf(stderr, "[heap] duplicate key: %s at idx %d and %d\n", pmap->elements[i]->key, i, j);
				return 0;
			}
		}
	}

	int key_count = 0;
	char *key;
	int *idx;

	HASH_TABLE_ITERATE(pmap->key_idx_map, key, idx)
	{
		if (!key || strlen(key) == 0) {
			fprintf(stderr, "[map] empty or null key in iteration\n");
			return 0;
		}

		if (*idx < 0 || *idx >= pmap->size) {
			fprintf(stderr, "[map->heap] out of range index %d for key %s\n", *idx, key);
			return 0;
		}

		if (strcmp(pmap->elements[*idx]->key, key) != 0) {
			fprintf(stderr, "[map->heap] key mismatch at idx %d: %s vs %s\n", *idx, key, pmap->elements[*idx]->key);
			return 0;
		}

		key_count++;
	}

	if (key_count != pmap->size) {
		fprintf(stderr, "[map] inconsistent size: map has %d keys, heap has %d\n", key_count, pmap->size);
		return 0;
	}

	return 1;
}

void *priority_map_internal_peek_data(struct priority_map *pmap, int idx)
{
	if (!pmap || idx < 0 || idx >= pmap->size) {
		return NULL;
	}
	return pmap->elements[idx]->data;
}