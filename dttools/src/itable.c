/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "itable.h"

#include <stdlib.h>
#include <string.h>

struct entry {
	int key;
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

struct itable * itable_create( int bucket_count )
{
	struct itable *h;
	int i;

	h = (struct itable*) malloc(sizeof(struct itable));
	if(!h) return 0;

	if(bucket_count==0) bucket_count=127;

	h->bucket_count = bucket_count;
	h->buckets = (struct entry**) malloc( sizeof(struct entry*)*bucket_count );
	if(!h->buckets) {
		free(h);
		return 0;
	}

	for( i=0; i<bucket_count; i++ ) {
		h->buckets[i] = 0;
	}

	h->size = 0;

	return h;
}

void itable_delete( struct itable *h )
{
	struct entry *e, *f;
	int i;

	for( i=0; i<h->bucket_count; i++ ) {
		e = h->buckets[i];
		while(e) {
			f=e->next;
			free(e);
			e=f;
		}
	}

	free(h->buckets);
	free(h);
}

int itable_size( struct itable *h )
{
	return h->size;
}

void * itable_lookup( struct itable *h, int key )
{
	struct entry *e;
	int index;
	
	index = key % h->bucket_count;
	e = h->buckets[index];

	while(e) {
		if(key==e->key) {
			return e->value;
		}
		e=e->next;
	}

	return 0;
}

int itable_insert( struct itable *h, int key, const void *value )
{
	struct entry *e;
	int index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while(e) {
		if(key==e->key) {
			e->value = (void*) value;
			return 1;
		}
		e=e->next;
	}

	e = (struct entry*) malloc(sizeof(struct entry));
	if(!e) return 0;

	e->key = key;
	e->value = (void*) value;
	e->next = h->buckets[index];
	h->buckets[index] = e;
	h->size ++;

	return 1;
}

void * itable_remove( struct itable *h, int key )
{
	struct entry *e,*f;
	void *value;
	int index;

	index = key % h->bucket_count;
	e = h->buckets[index];
	f = 0;

	while(e) {
		if(key==e->key) {
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

void itable_firstkey( struct itable *h )
{
	h->ientry = 0;
	for(h->ibucket=0;h->ibucket<h->bucket_count;h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if(h->ientry) break;
	}
}

int itable_nextkey( struct itable *h, int *key, void **value )
{
	if(h->ientry) {
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if(!h->ientry) {
			h->ibucket++;
			for(;h->ibucket<h->bucket_count;h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if(h->ientry) break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}
