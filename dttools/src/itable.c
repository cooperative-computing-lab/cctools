/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable.h"

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
};

struct itable *itable_create(int bucket_count)
{
	struct itable *h;

	h = (struct itable *) malloc(sizeof(struct itable));
	if(!h)
		return 0;

	if(bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	h->bucket_count = bucket_count;
	h->buckets = (struct entry **) calloc(bucket_count, sizeof(struct entry *));
	if(!h->buckets) {
		free(h);
		return 0;
	}

	h->size = 0;

	return h;
}

void itable_clear( struct itable *h, void (*delete_func)( void *) )
{
	struct entry *e, *f;
	int i;

	for(i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while(e) {
			if(delete_func) delete_func(e->value);
			f = e->next;
			free(e);
			e = f;
		}
	}

	for(i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}
}

void itable_delete(struct itable *h)
{
	itable_clear(h,0);
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

	while(e) {
		if(key == e->key) {
			return e->value;
		}
		e = e->next;
	}

	return 0;
}

static int itable_double_buckets(struct itable *h)
{
	struct itable *hn = itable_create(2 * h->bucket_count);

	if(!hn)
		return 0;

	/* Move pairs to new hash */
	uint64_t key;
	void *value;
	itable_firstkey(h);
	while(itable_nextkey(h, &key, &value))
		if(!itable_insert(hn, key, value))
		{
			itable_delete(hn);
			return 0;
		}

	/* Delete all old pairs */
	struct entry *e, *f;
	int i;
	for(i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while(e) {
			f = e->next;
			free(e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets      = hn->buckets;
	h->bucket_count = hn->bucket_count;
	h->size         = hn->size;

	/* Delete reference to new, so old is safe */
	free(hn);

	return 1;
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	if( ((float) h->size / h->bucket_count) > DEFAULT_LOAD )
		itable_double_buckets(h);

	index = key % h->bucket_count;
	e = h->buckets[index];

	while(e) {
		if(key == e->key) {
			e->value = (void *) value;
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *) malloc(sizeof(struct entry));
	if(!e)
		return 0;

	e->key = key;
	e->value = (void *) value;
	e->next = h->buckets[index];
	h->buckets[index] = e;
	h->size++;

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

	while(e) {
		if(key == e->key) {
			if(f) {
				f->next = e->next;
			} else {
				h->buckets[index] = e->next;
			}
			value = e->value;
			free(e);
			h->size--;
			return value;
		}
		f = e;
		e = e->next;
	}

	return 0;
}

void * itable_pop( struct itable *t )
{
	UINT64_T key;
	void *value;

	itable_firstkey(t);
	if(itable_nextkey(t, &key, (void**)&value)) {
		return itable_remove(t,key);
	} else {
		return 0;
	}
}

void itable_firstkey(struct itable *h)
{
	h->ientry = 0;
	for(h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if(h->ientry)
			break;
	}
}

int itable_nextkey(struct itable *h, UINT64_T * key, void **value)
{
	if(h->ientry) {
		*key = h->ientry->key;
		if(value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if(!h->ientry) {
			h->ibucket++;
			for(; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if(h->ientry)
					break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
