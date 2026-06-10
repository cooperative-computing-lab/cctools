/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "itable.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_MAX_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125

#define ITERATION_MAX 1000000 /* very conservative overflow protection */

struct entry {
	UINT64_T key;
	void *value;
	struct entry *next;
	int deleted;
};

struct itable {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
	int iteration_index;
	int need_compact;
};

struct itable *itable_create(int bucket_count)
{
	struct itable *h;

	h = (struct itable *)malloc(sizeof(struct itable));
	if (!h)
		return 0;

	if (bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	h->bucket_count = bucket_count;
	h->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!h->buckets) {
		free(h);
		return 0;
	}

	h->size = 0;
	h->iteration_index = 0;
	h->need_compact = 0;
	return h;
}

void itable_clear(struct itable *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			if (delete_func && !e->deleted)
				delete_func(e->value);
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}

	h->size = 0;
	h->need_compact = 0;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
}

static int itable_insert_to_buckets_aux(struct entry **buckets, int bucket_count, struct entry *new_entry)
{
	UINT64_T index;
	struct entry *e;

	index = new_entry->key % bucket_count;
	e = buckets[index];

	while (e) {
		/* check that if this key already exist in the table */
		if (new_entry->key == e->key) {
			e->value = new_entry->value;
			e->deleted = 0;
			free(new_entry);
			return 1;
		}
		e = e->next;
	}

	new_entry->next = buckets[index];
	buckets[index] = new_entry;

	return 1;
}

static int itable_reduce_buckets(struct itable *h)
{
	int new_count = ((h->bucket_count + 1) / 2) - 1;

	/* DEFAULT_SIZE is the minimum size */
	if (new_count < DEFAULT_SIZE) {
		return 0;
	}

	/* Table cannot be reduced above DEFAULT_MAX_LOAD */
	if (((float)h->size / new_count) > DEFAULT_MAX_LOAD) {
		return 1;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			if (!e->deleted) {
				e->next = NULL;
				itable_insert_to_buckets_aux(new_buckets, new_count, e);
			}
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;
	h->need_compact = 0;

	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

/* Compact the table by removing deleted entries. This does not change the size of the table, or the number of buckets. */
void itable_compact(struct itable *h)
{
	struct entry *e, *f;
	int i;

	if (!h->need_compact) {
		return;
	}

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];

		/* find the head of the bucket and remove deleted entries*/
		while (e && e->deleted) {
			f = e->next;
			free(e);
			e = f;
		}
		h->buckets[i] = e;

		/* remove deleted entries from the rest of the bucket */
		while (e) {
			f = e->next;
			while (f && f->deleted) {
				e->next = f->next;
				free(f);
				f = e->next;
			}
			e = f;
		}
	}

	h->need_compact = 0;
	if (((float)h->size / h->bucket_count) < DEFAULT_MIN_LOAD) {
		itable_reduce_buckets(h);
	}
}

void itable_delete(struct itable *h)
{
	itable_clear(h, 0);
	free(h->buckets);
	free(h);
}

int itable_size(struct itable *h)
{
	return h->size;
}

void *itable_lookup(struct itable *h, UINT64_T key)
{
	struct entry *e;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			if (!e->deleted) {
				return e->value;
			}
			return 0;
		}
		e = e->next;
	}

	return 0;
}

static int itable_double_buckets(struct itable *h)
{
	int new_count = (2 * (h->bucket_count + 1)) - 1;
	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			if (!e->deleted) {
				e->next = NULL;
				itable_insert_to_buckets_aux(new_buckets, new_count, e);
			}
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;
	h->need_compact = 0;

	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD)
		itable_double_buckets(h);

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			int was_deleted = e->deleted;
			e->value = (void *)value;
			e->deleted = 0;
			if (was_deleted) {
				h->size++;
			}
			h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->key = key;
	e->value = (void *)value;
	e->deleted = 0;

	int inserted = itable_insert_to_buckets_aux(h->buckets, h->bucket_count, e);
	if (inserted) {
		h->size++;
		h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
	} else {
		free(e);
		return 0;
	}

	return 1;
}

/* keys are not really removed, just marked as deleted. A call to itable_compact will remove them. */
void *itable_remove(struct itable *h, UINT64_T key)
{
	struct entry *e;
	void *value;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			if (e->deleted) {
				return 0;
			}
			e->deleted = 1;
			h->need_compact = 1;
			value = e->value;

			h->size--;
			return value;
		}

		e = e->next;
	}

	return 0;
}

void *itable_pop(struct itable *t)
{
	UINT64_T key;
	void *value;
	int iteration;

	ITABLE_ITERATE(t, iteration, key, value)
	{
		return itable_remove(t, key);
	}

	return 0;
}

int itable_firstkey(struct itable *h)
{
	itable_compact(h);
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	h->ientry = 0;
	for (h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			/* we don't need to check for deleted entries here because itable_compact removes them */
			break;
		}
	}

	return h->iteration_index;
}

int itable_nextkey(struct itable *h, int iteration, UINT64_T *key, void **value)
{
	if (iteration != h->iteration_index) {
		fatal("cctools bug: the itable iteration has not been reset since last modification");
	}

	if (h->ientry) {
		*key = h->ientry->key;
		if (value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		while (h->ientry && h->ientry->deleted) {
			h->ientry = h->ientry->next;
		}
		if (!h->ientry) {
			h->ibucket++;
			for (; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry && !h->ientry->deleted)
					break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}

void itable_foreach_ro(struct itable *h, void (*func)(UINT64_T key, void *value, void *arg), void *arg)
{
	struct entry *e;
	int i;

	int prev_iteration = h->iteration_index;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e && !e->deleted) {
			func(e->key, e->value, arg);
			e = e->next;

			if (h->iteration_index != prev_iteration) {
				fatal("cctools bug: the itable iteration has been modified since last call to itable_foreach_ro");
			}
		}
	}
}

/* vim: set noexpandtab tabstop=8: */
