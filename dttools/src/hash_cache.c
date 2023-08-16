/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_cache.h"
#include "hash_table.h"

#include <time.h>
#include <stdlib.h>

struct hash_cache {
	struct hash_table *table;
	hash_cache_cleanup_t cleanup;
};

struct entry {
	void *value;
	time_t expires;
};

struct hash_cache *hash_cache_create(int size, hash_func_t func, hash_cache_cleanup_t cleanup)
{
	struct hash_cache *cache;

	cache = malloc(sizeof(*cache));
	if(!cache)
		return 0;

	cache->table = hash_table_create(size, func);
	if(!cache->table) {
		free(cache);
		return 0;
	}

	cache->cleanup = cleanup;

	return cache;
}

void hash_cache_delete(struct hash_cache *cache)
{
	if(cache) {
		if(cache->table)
			hash_table_delete(cache->table);
		free(cache);
	}
}

int hash_cache_insert(struct hash_cache *cache, const char *key, void *value, int lifetime)
{
	struct entry *e, *old;

	e = malloc(sizeof(*e));
	if(!e)
		return 0;

	e->value = value;
	e->expires = time(0) + lifetime;

	old = hash_table_remove(cache->table, key);
	if(old) {
		cache->cleanup(old->value);
		free(old);
	}

	hash_table_insert(cache->table, key, e);

	return 1;
}

void *hash_cache_remove(struct hash_cache *cache, const char *key)
{
	struct entry *e;
	void *result;

	e = hash_table_remove(cache->table, key);
	if(e) {
		result = e->value;
		if(e->expires < time(0)) {
			cache->cleanup(result);
			result = 0;
		}
		free(e);
	} else {
		result = 0;
	}

	return result;
}

void *hash_cache_lookup(struct hash_cache *cache, const char *key)
{
	struct entry *e;
	void *result;

	e = hash_table_lookup(cache->table, key);
	if(e) {
		result = e->value;
		if(e->expires < time(0)) {
			result = hash_cache_remove(cache, key);
			if(result) cache->cleanup(result);
			result = 0;
		}
	} else {
		result = 0;
	}

	return result;
}

void hash_cache_firstkey(struct hash_cache *cache)
{
	hash_table_firstkey(cache->table);
}

int hash_cache_nextkey(struct hash_cache *cache, char **key, void **item)
{
	struct entry *e;
	time_t current = time(0);

	while(hash_table_nextkey(cache->table, key, (void **) &e)) {
		if(e->expires < current) {
			hash_cache_remove(cache, *key);
			continue;
		} else {
			*item = e->value;
			return 1;
		}
	}

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
