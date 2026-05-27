/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75

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
	 * using the array keys from itable_keys_array. */
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

	h->size = 0;

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
	int new_count = 2 * h->bucket_count;
	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	UINT64_T index;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			e->next = NULL;
			index = e->key % new_count;
			e->next = new_buckets[index];
			new_buckets[index] = e;
			e = f;
		}
	}

	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	/* structure of itable changed completely, thus a nextkey would be incorrect. */
	h->cant_iterate_yet = 1;

	return 1;
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	if (((float)h->size / h->bucket_count) > DEFAULT_LOAD)
		itable_double_buckets(h);

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			e->value = (void *)value;
			h->cant_iterate_yet = 1;
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->key = key;
	e->value = (void *)value;
	e->next = h->buckets[index];
	h->buckets[index] = e;
	h->size++;

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

int itable_fromkey(struct itable *h, UINT64_T key)
{
	h->cant_iterate_yet = 0;

	UINT64_T index = key % h->bucket_count;
	h->ibucket = index;
	h->ientry = h->buckets[h->ibucket];

	while (h->ientry) {
		if (key == h->ientry->key) {
			return 1;
		}
		h->ientry = h->ientry->next;
	}

	itable_firstkey(h);
	return 0;
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

void itable_randomkey(struct itable *h, int *offset_bookkeep)
{
	h->cant_iterate_yet = 0;

	h->ientry = 0;
	if (h->bucket_count < 1) {
		return;
	}

	int ibucket_start = random() % h->bucket_count;

	for (h->ibucket = ibucket_start; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			*offset_bookkeep = h->ibucket;
			return;
		}
	}

	for (h->ibucket = 0; h->ibucket < ibucket_start; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			*offset_bookkeep = h->ibucket;
			return;
		}
	}
}

int itable_nextkey_with_offset(struct itable *h, int offset_bookkeep, UINT64_T *key, void **value)
{
	if (h->cant_iterate_yet) {
		fatal("cctools bug: the itable iteration has not been reset since last modification");
	}

	if (h->bucket_count < 1) {
		return 0;
	}

	offset_bookkeep = offset_bookkeep % h->bucket_count;

	if (h->ientry) {
		*key = h->ientry->key;
		if (value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if (!h->ientry) {
			h->ibucket = (h->ibucket + 1) % h->bucket_count;
			for (; h->ibucket != offset_bookkeep; h->ibucket = (h->ibucket + 1) % h->bucket_count) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry) {
					break;
				}
			}
		}
		return 1;
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
