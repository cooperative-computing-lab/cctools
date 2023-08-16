/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "string_set.h"
#include "hash_table.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75
#define DEFAULT_FUNC hash_string

struct entry {
	char *element;
	unsigned hash;
	struct entry *next;
};

struct string_set {
	hash_func_t hash_func;
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
};

struct string_set *string_set_create(int bucket_count, hash_func_t func)
{
	struct string_set *s;

	s = (struct string_set *) malloc(sizeof(struct string_set));
	if(!s)
		return 0;

	if(bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	if(!func)
		func = DEFAULT_FUNC;

	s->hash_func = func;
	s->bucket_count = bucket_count;
	s->buckets = (struct entry **) calloc(bucket_count, sizeof(struct entry *));
	if(!s->buckets) {
		free(s);
		return 0;
	}

	s->size = 0;

	return s;
}

struct string_set *string_set_duplicate(struct string_set *s)
{
	struct string_set *s2;

	s2 = string_set_create(0, s->hash_func);
	string_set_first_element(s);
	char *element;
	while(string_set_next_element(s, &element))
		string_set_insert(s2, element);

	return s2;

}

struct string_set *string_set_union(struct string_set *s1, struct string_set *s2)
{

	struct string_set *s = string_set_duplicate(s1);

	string_set_first_element(s2);
	char *element;
	while(string_set_next_element(s2, &element))
		string_set_insert(s, element);

	return s;

}

void string_set_clear(struct string_set *s)
{
	struct entry *e, *f;
	int i;

	for(i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while(e) {
			f = e->next;
			free(e->element);
			free(e);
			e = f;
		}
	}

	for(i = 0; i < s->bucket_count; i++) {
		s->buckets[i] = 0;
	}
}

void string_set_delete(struct string_set *s)
{
	string_set_clear(s);
	free(s->buckets);
	free(s);
}

int string_set_size(struct string_set *s)
{
	return s->size;
}

int string_set_lookup(struct string_set *s, const char *element)
{
	struct entry *e;
	uint64_t hash, index;

	hash = s->hash_func(element);
	index = hash % s->bucket_count;
	e = s->buckets[index];

	while(e) {
		if(hash == e->hash && !strcmp(element, e->element)) {
			return 1;
		}
		e = e->next;
	}

	return 0;
}

static int string_set_double_buckets(struct string_set *s)
{
	struct string_set *sn = string_set_create(2 * s->bucket_count, s->hash_func);

	if(!sn)
		return 0;

	/* Move elements to new string_set */
	char *element;
	string_set_first_element(s);
	while(string_set_next_element(s, &element) )
		if(!string_set_insert(sn, element))
		{
			string_set_delete(sn);
			return 0;
		}

	/* Delete all elements */
	struct entry *e, *f;
	int i;
	for(i = 0; i < s->bucket_count; i++) {
		e = s->buckets[i];
		while(e) {
			f = e->next;
			free(e->element);
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

int string_set_insert(struct string_set *s, const char *element)
{
	struct entry *e;
	uint64_t hash, index;

	if( ((float) s->size / s->bucket_count) > DEFAULT_LOAD )
		string_set_double_buckets(s);

	hash = s->hash_func(element);
	index = hash % s->bucket_count;
	e = s->buckets[index];

	while(e) {
		if(hash == e->hash && !strcmp(element, e->element)) {
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *) malloc(sizeof(struct entry));
	if(!e)
		return 0;

	e->element = strdup(element);
	if(!e->element) {
		free(e);
		return 0;
	}

	e->hash = hash;
	e->next = s->buckets[index];
	s->buckets[index] = e;
	s->size++;

	return 1;
}

int string_set_insert_string_set(struct string_set *s, struct string_set *s2)
{
	string_set_first_element(s2);
	int additions = 0;
	char *element;
	while(string_set_next_element(s2, &element)){
		additions += string_set_insert(s, element);
	}

	return additions;
}

int string_set_push(struct string_set *s, const char *element)
{
  return string_set_insert(s, element);
}

int string_set_remove(struct string_set *s, const char *element)
{
	struct entry *e, *f;
	uint64_t hash, index;

	hash = s->hash_func(element);
	index = hash % s->bucket_count;
	e = s->buckets[index];
	f = 0;

	while(e) {
		if(hash == e->hash && !strcmp(element, e->element)) {
			if(f) {
				f->next = e->next;
			} else {
				s->buckets[index] = e->next;
			}
			free(e->element);
			free(e);
			s->size--;
			return 1;
		}
		f = e;
		e = e->next;
	}

	return 0;
}

void string_set_first_element(struct string_set *s)
{
	s->ientry = 0;
	for(s->ibucket = 0; s->ibucket < s->bucket_count; s->ibucket++) {
		s->ientry = s->buckets[s->ibucket];
		if(s->ientry)
			break;
	}
}

int string_set_next_element(struct string_set *s, char **element)
{
	if(s->ientry) {
		*element = (char *) s->ientry->element;

		s->ientry = s->ientry->next;
		if(!s->ientry) {
			s->ibucket++;
			for(; s->ibucket < s->bucket_count; s->ibucket++) {
				s->ientry = s->buckets[s->ibucket];
				if(s->ientry)
					break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
