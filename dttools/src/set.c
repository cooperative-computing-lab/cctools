/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "set.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125

struct entry {
	uintptr_t element;
	struct entry *next;
	int deleted;
};

struct set {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
	int need_compact;
};

struct set *set_create(int bucket_count)
{
	struct set *s;

	s = (struct set *)malloc(sizeof(struct set));
	if (!s)
		return 0;

	if (bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	s->bucket_count = bucket_count;
	s->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!s->buckets) {
		free(s);
		return 0;
	}

	s->size = 0;
	s->need_compact = 0;
	s->ientry = 0;
	return s;
}

struct set *set_duplicate(struct set *s)
{
	struct set *s2;

	if (!s) {
		return NULL;
	}

	s2 = set_create(0);
	set_first_element(s);
	const void *element;
	while ((element = set_next_element(s)))
		set_insert(s2, element);

	return s2;
}

struct set *set_union(struct set *s1, struct set *s2)
{

	struct set *s = set_duplicate(s1);

	set_first_element(s2);
	const void *element;
	while ((element = set_next_element(s2)))
		set_insert(s, element);

	return s;
}

static int set_insert_to_buckets_aux(struct entry **buckets, int bucket_count, struct entry *new_entry)
{
	uint64_t index;
	struct entry *e;

	index = new_entry->element % bucket_count;
	e = buckets[index];

	while (e) {
		if (new_entry->element == e->element) {
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

static int set_reduce_buckets(struct set *s)
{
	int new_count = ((s->bucket_count + 1) / 2) - 1;

	if (new_count < DEFAULT_SIZE) {
		return 0;
	}

	if (((float)s->size / new_count) > DEFAULT_LOAD) {
		return 1;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while (e) {
			f = e->next;
			if (!e->deleted) {
				e->next = NULL;
				set_insert_to_buckets_aux(new_buckets, new_count, e);
			}
			e = f;
		}
	}

	free(s->buckets);
	s->buckets = new_buckets;
	s->bucket_count = new_count;
	s->need_compact = 0;

	return 1;
}

void set_compact(struct set *s)
{
	struct entry *e, *f;
	int i;

	if (!s->need_compact) {
		return;
	}

	for (i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];

		while (e && e->deleted) {
			f = e->next;
			free(e);
			e = f;
		}
		s->buckets[i] = e;

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

	s->need_compact = 0;
	if (((float)s->size / s->bucket_count) < DEFAULT_MIN_LOAD) {
		set_reduce_buckets(s);
	}
}

void set_clear(struct set *s)
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while (e) {
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < s->bucket_count; i++) {
		s->buckets[i] = 0;
	}

	s->size = 0;
	s->need_compact = 0;
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

	uintptr_t key = (uintptr_t)element;

	index = key % s->bucket_count;
	e = s->buckets[index];

	while (e) {
		if (key == e->element) {
			if (!e->deleted) {
				return 1;
			}
			return 0;
		}
		e = e->next;
	}

	return 0;
}

static int set_double_buckets(struct set *s)
{
	int new_count = 2 * s->bucket_count;
	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while (e) {
			f = e->next;
			if (!e->deleted) {
				e->next = NULL;
				set_insert_to_buckets_aux(new_buckets, new_count, e);
			}
			e = f;
		}
	}

	free(s->buckets);
	s->buckets = new_buckets;
	s->bucket_count = new_count;
	s->need_compact = 0;

	return 1;
}

int set_insert(struct set *s, const void *element)
{
	struct entry *e;
	uint64_t index;

	uintptr_t key = (uintptr_t)element;

	if (((float)s->size / s->bucket_count) > DEFAULT_LOAD)
		set_double_buckets(s);

	index = key % s->bucket_count;
	e = s->buckets[index];

	while (e) {
		if (key == e->element) {
			if (e->deleted) {
				e->deleted = 0;
				s->size++;
			}
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->element = key;
	e->deleted = 0;
	e->next = s->buckets[index];
	s->buckets[index] = e;
	s->size++;

	return 1;
}

int set_insert_set(struct set *s, struct set *s2)
{
	set_first_element(s2);
	int additions = 0;
	const void *element;
	while ((element = set_next_element(s2))) {
		additions += set_insert(s, element);
	}

	return additions;
}

int set_insert_list(struct set *s, struct list *l)
{
	list_first_item(l);
	int additions = 0;
	const void *element;
	while ((element = list_next_item(l))) {
		additions += set_insert(s, element);
	}

	return additions;
}

int set_push(struct set *s, const void *element)
{
	return set_insert(s, element);
}

int set_remove(struct set *s, const void *element)
{
	struct entry *e;
	uint64_t index;

	uintptr_t key = (uintptr_t)element;

	index = key % s->bucket_count;
	e = s->buckets[index];

	while (e) {
		if (key == e->element) {
			if (e->deleted) {
				return 0;
			}
			e->deleted = 1;
			s->need_compact = 1;
			s->size--;
			return 1;
		}
		e = e->next;
	}

	return 0;
}

void *set_pop(struct set *s)
{
	if (set_size(s) < 1)
		return 0;

	void *element;
	set_first_element(s);
	element = set_next_element(s);

	if (!set_remove(s, element))
		return 0;
	else
		return element;
}

void set_first_element(struct set *s)
{
	set_compact(s);
	s->ientry = 0;
	for (s->ibucket = 0; s->ibucket < s->bucket_count; s->ibucket++) {
		s->ientry = s->buckets[s->ibucket];
		if (s->ientry)
			break;
	}
}

void *set_next_element(struct set *s)
{
	void *element;

	if (s->ientry) {
		element = (void *)s->ientry->element;

		s->ientry = s->ientry->next;
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (!s->ientry) {
			s->ibucket++;
			for (; s->ibucket < s->bucket_count; s->ibucket++) {
				s->ientry = s->buckets[s->ibucket];
				if (s->ientry && !s->ientry->deleted)
					break;
			}
		}

		return element;
	} else {
		return 0;
	}
}

void set_random_element(struct set *s, int *offset_bookkeep)
{
	s->ientry = 0;
	if (s->bucket_count < 1) {
		return;
	}

	int ibucket_start = random() % s->bucket_count;

	for (s->ibucket = ibucket_start; s->ibucket < s->bucket_count; s->ibucket++) {
		s->ientry = s->buckets[s->ibucket];
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (s->ientry) {
			*offset_bookkeep = s->ibucket;
			return;
		}
	}

	for (s->ibucket = 0; s->ibucket < ibucket_start; s->ibucket++) {
		s->ientry = s->buckets[s->ibucket];
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (s->ientry) {
			*offset_bookkeep = s->ibucket;
			return;
		}
	}
}

void *set_next_element_with_offset(struct set *s, int offset_bookkeep)
{
	if (s->bucket_count < 1) {
		return 0;
	}

	void *element = NULL;

	offset_bookkeep = offset_bookkeep % s->bucket_count;

	if (s->ientry) {
		element = (void *)s->ientry->element;

		s->ientry = s->ientry->next;
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (!s->ientry) {
			s->ibucket = (s->ibucket + 1) % s->bucket_count;
			for (; s->ibucket != offset_bookkeep; s->ibucket = (s->ibucket + 1) % s->bucket_count) {
				s->ientry = s->buckets[s->ibucket];
				while (s->ientry && s->ientry->deleted) {
					s->ientry = s->ientry->next;
				}
				if (s->ientry) {
					break;
				}
			}
		}

		return element;
	}

	return 0;
}

void **set_values(struct set *s)
{
	if (s->size < 1) {
		return NULL;
	}

	void **elements = malloc(sizeof(void *) * s->size);

	int offset_bookkeep;
	void *element;
	int i = 0;
	SET_ITERATE_RANDOM_START(s, offset_bookkeep, element)
	{
		elements[i] = element;
		i++;
	}

	return elements;
}

void **set_values_array(struct set *s)
{
	if (s->size < 1) {
		return NULL;
	}

	void **elements = malloc(sizeof(void *) * (s->size + 1));
	void *element;
	int i = 0;
	SET_ITERATE(s, element)
	{
		elements[i] = element;
		i++;
	}

	elements[s->size] = NULL;

	return elements;
}

void set_free_values_array(void **values)
{
	free(values);
}

/* vim: set noexpandtab tabstop=8: */
