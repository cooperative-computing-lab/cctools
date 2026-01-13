/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_MAX_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125

struct entry {
	UINT64_T key;
	void *value;
	struct entry *next;
};

struct itable {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;

	/* for memory safety, itable_nextkey cannot be called in the same
	 * iteration if itable_insert or itable_remove has been called.
	 * In such case, the executable will be terminated with a fatal message.
	 * If the table should be modified during iterations, consider
	 * using the array keys from itable_keys_array. (If so, remember
	 * to free it afterwards with itable_free_keys_array.) */
	int cant_iterate_yet;
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
	h->cant_iterate_yet = 0;

	return h;
}

void itable_clear(struct itable *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			if (delete_func)
				delete_func(e->value);
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}

	/* buckets went away, thus a nextkey would be invalid */
	h->cant_iterate_yet = 1;
}

void itable_delete(struct itable *h)
{
	itable_clear(h, 0);
	free(h->buckets);
	free(h);
}

UINT64_T *itable_keys_array(struct itable *h)
{
	UINT64_T *keys = (UINT64_T *)malloc(sizeof(UINT64_T) * h->size);
	int ikey = 0;

	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			keys[ikey] = e->key;
			ikey++;
			f = e->next;
			e = f;
		}
	}

	return keys;
}

void itable_free_keys_array(UINT64_T *keys)
{
	free(keys);
}

int itable_size(struct itable *h)
{
	return h->size;
}

double itable_load(struct itable *h)
{
	return (double)h->size / h->bucket_count;
}

static int insert_to_buckets_aux(struct entry **buckets, int bucket_count, struct entry *new_entry)
{
	unsigned index;
	index = new_entry->key % bucket_count;

	// Possible memory leak! Silently replacing value if it existed.
	new_entry->next = buckets[index];
	buckets[index] = new_entry;
	return 1;
}

void *itable_lookup(struct itable *h, UINT64_T key)
{
	struct entry *e;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			return e->value;
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
			e->next = NULL;
			insert_to_buckets_aux(new_buckets, new_count, e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	/* structure of itable changed completely, thus a nextkey would be incorrect. */
	h->cant_iterate_yet = 1;

	return 1;
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD)
		itable_double_buckets(h);

	struct entry *new_entry = (struct entry *)xxmalloc(sizeof(struct entry));
	new_entry->key = key;
	new_entry->value = (void *)value;

	int inserted = insert_to_buckets_aux(h->buckets, h->bucket_count, new_entry);
	if (inserted) {
		h->size++;
		/* inserting cause different behaviours with nextkey (e.g., sometimes the new
		 * key would be included or skipped in the iteration */
		h->cant_iterate_yet = 1;

		return 1;
	}

	return 0;
}

static int itable_reduce_buckets(struct itable *h)
{
	int new_count = ((h->bucket_count + 1) / 2) - 1;

	/* DEFAULT_SIZE is the minimum size */
	if (new_count < DEFAULT_SIZE) {
		return 1;
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
			e->next = NULL;
			insert_to_buckets_aux(new_buckets, new_count, e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	/* structure of itable changed completely, thus a nextkey would be incorrect. */
	h->cant_iterate_yet = 1;

	return 1;
}

void *itable_remove(struct itable *h, UINT64_T key)
{
	struct entry *e, *f;
	void *value;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];
	f = 0;

	while (e) {
		if (key == e->key) {
			if (f) {
				f->next = e->next;
			} else {
				h->buckets[index] = e->next;
			}
			value = e->value;
			free(e);
			h->size--;
			h->cant_iterate_yet = 1;

			if (((float)h->size / h->bucket_count) < DEFAULT_MIN_LOAD) {
				itable_reduce_buckets(h);
			}

			return value;
		}
		f = e;
		e = e->next;
	}

	return 0;
}

void *itable_pop(struct itable *t)
{
	UINT64_T key;
	void *value;

	itable_firstkey(t);
	if (itable_nextkey(t, &key, (void **)&value)) {
		return itable_remove(t, key);
	} else {
		return 0;
	}
}

void itable_firstkey(struct itable *h)
{
	h->cant_iterate_yet = 0;

	h->ientry = 0;
	for (h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry)
			break;
	}
}

int itable_nextkey(struct itable *h, UINT64_T *key, void **value)
{
	if (h->cant_iterate_yet) {
		fatal("cctools bug: the itable iteration has not been reset since last modification");
	}

	if (h->ientry) {
		*key = h->ientry->key;
		if (value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if (!h->ientry) {
			h->ibucket++;
			for (; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry)
					break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}
/* vim: set noexpandtab tabstop=8: */
