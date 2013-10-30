/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "set.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75


struct entry {
	uintptr_t     element;
	struct entry *next;
};

struct set {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
};

struct set *set_create(int bucket_count)
{
	struct set *s;

	s = (struct set *) malloc(sizeof(struct set));
	if(!s)
		return 0;

	if(bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	s->bucket_count = bucket_count;
	s->buckets = (struct entry **) calloc(bucket_count, sizeof(struct entry *));
	if(!s->buckets) {
		free(s);
		return 0;
	}

	s->size = 0;

	return s;
}

void set_clear(struct set *s)
{
	struct entry *e, *f;
	int i;

	for(i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while(e) {
			f = e->next;
			free(e);
			e = f;
		}
	}
	
	for(i = 0; i < s->bucket_count; i++) {
		s->buckets[i] = 0;
	}
}

void set_delete(struct set *s)
{
	set_clear(s);
	free(s->buckets);
	free(s);
}

int set_size(struct set *s)
{
	return s->size;
}

int set_lookup(struct set *s, void *element)
{
	struct entry *e;
	uint64_t index;

    uintptr_t key = (uintptr_t) element;

	index = key % s->bucket_count;
	e = s->buckets[index];

	while(e) {
		if(key == e->element) {
			return 1;
		}
		e = e->next;
	}

	return 0;
}

static int set_double_buckets(struct set *s)
{
	struct set *sn = set_create(2 * s->bucket_count);

	if(!sn)
		return 0;

	/* Move elements to new set */
	void *element;
	set_first_element(s);
	while( (element = set_next_element(s)) )
		if(!set_insert(sn, element))
		{
			set_delete(sn);
			return 0;
		}

	/* Delete all elements */
	struct entry *e, *f;
	int i;
	for(i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while(e) {
			f = e->next;
			free(e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(s->buckets);
	s->buckets      = sn->buckets;
	s->bucket_count = sn->bucket_count;
	s->size         = sn->size;
	
	/* Delete reference to new, so old is safe */
	free(sn);

	return 1;
}

int set_insert(struct set *s, const void *element)
{
	struct entry *e;
	uint64_t index;

	uintptr_t key = (uintptr_t) element;

	if( ((float) s->size / s->bucket_count) > DEFAULT_LOAD )
		set_double_buckets(s);

	index = key % s->bucket_count;
	e = s->buckets[index];

	while(e) {
		if(key == e->element) {
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *) malloc(sizeof(struct entry));
	if(!e)
		return 0;

	e->element = key;
	e->next = s->buckets[index];
	s->buckets[index] = e;
	s->size++;

	return 1;
}

int set_push(struct set *s, const void *element)
{
  return set_insert(s, element);
}

int set_remove(struct set *s, const void *element)
{
	struct entry *e, *f;
	uint64_t index;

    uintptr_t key = (uintptr_t) element;

	index = key % s->bucket_count;
	e = s->buckets[index];
	f = 0;

	while(e) {
		if(key == e->element) {
			if(f) {
				f->next = e->next;
			} else {
				s->buckets[index] = e->next;
			}
			free(e);
			s->size--;
			return 1;
		}
		f = e;
		e = e->next;
	}

	return 0;
}

void *set_pop(struct set *s)
{
  if( set_size(s) < 1 )
    return 0;

  void *element;
  set_first_element(s);
  element = set_next_element(s);

  if(!set_remove(s, element))
    return 0;
  else
    return element;
}

void set_first_element(struct set *s)
{
	s->ientry = 0;
	for(s->ibucket = 0; s->ibucket < s->bucket_count; s->ibucket++) {
		s->ientry = s->buckets[s->ibucket];
		if(s->ientry)
			break;
	}
}

void *set_next_element(struct set *s)
{
    void *element;

	if(s->ientry) {
		element = (void *) s->ientry->element;

		s->ientry = s->ientry->next;
		if(!s->ientry) {
			s->ibucket++;
			for(; s->ibucket < s->bucket_count; s->ibucket++) {
				s->ientry = s->buckets[s->ibucket];
				if(s->ientry)
					break;
			}
		}

		return element;
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=4: */
