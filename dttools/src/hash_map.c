
#include "hash_map.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>

struct hash_map_entry {
	char *key;
	void *value;
};

struct hash_map {
	struct hash_map_entry **buckets;
	int buckets_count;
	int live_count;
	int dead_count;
	int active_cursors;
};

struct hash_map_cursor {
	unsigned position;
	struct hash_map *owner;
};

#define HASH_MAP_LOAD_FACTOR_MIN 10
#define HASH_MAP_LOAD_FACTOR_MAX 40
#define HASH_MAP_SIZE_MIN 127

static struct hash_map_entry * hash_map_entry_create( const char *key, void *value )
{
	struct hash_map_entry *e = malloc(sizeof(*e));
	e->key = xxstrdup(key);
	e->value = value;
	return e;
}

static void hash_map_entry_delete( struct hash_map_entry *e )
{
	free(e->key);
	/* e->value belongs to the user. */
	free(e);
}

struct hash_map * hash_map_create( unsigned buckets, hash_func_t hashfunc )
{
	struct hash_map *map = malloc(sizeof(*map));

	if(buckets==0) buckets = HASH_MAP_SIZE_MIN;
	if(hashfunc==0) hashfunc = string_hash;
	
	map->buckets = calloc(sizeof(struct hash_map_entry *e),bucket_count);
	map->hashfunc = hashfunc;
	map->bucket_count = bucket_count;
	map->live_count = 0;
	map->dead_count = 0;
	map->active_cursors = 0;

	return map;
}

void hash_map_delete( struct hash_map *map )
{
	hash_map_clear(map,0);
	free(map->buckets);
	free(map);
}

void hash_map_clear( struct hash_map *map, void (*deletefunc)(void*) )
{
	for(unsigned i=0; i<map->bucket_count; i++) {
		struct hash_map_entry *e = map->buckets[i];
		if(e) {
			if(e!=HASH_MAP_TOMBSTONE) {
				if(deletefunc) deletefunc(e->value);
				hash_map_entry_delete(e);
			}
			map->buckets[i] = 0;
		}
	}
	map->load_count = 0;
	map->dead_count = 0;
}

static void hash_map_rehash( struct hash_map *map, int new_bucket_count )
{
	struct hash_map_entry **new_buckets = calloc(new_bucket_count,sizeof(struct hash_map_entry *));

	for(int i=0;i<map->bucket_count;i++) {
		struct hash_map_entry *e = map->buckets[i];
		if(e && e!=HASH_MAP_TOMBSTONE) {
			hash_map_position_t newpos = map->hashfunc(e->key) % new_bucket_count;
			while(1) {
				if(new_buckets[newpos]) {
					newpos++;
					if(newpos>new_bucket_count) newpos = 0;
				} else {
					break;
				}
			}
			new_buckets[newpos] = e;
		}
	}

	free(map->buckets);

	map->buckets = new_buckets;
	map->bucket_count = new_bucket_count;
}

static int hash_map_consider_rehash( struct hash_map *map )
{
	if(map->cursors_active>=0) return 0;

	int live_load = live_count*100/bucket_count;
	int total_load = (live_count+dead_count)*100/bucket_count;

	int new_bucket_count = map->bucket_count;
	  
	if(live_load<HASH_MAP_LOAD_FACTOR_MIN) {
		new_bucket_count = ((map->bucket_count + 1)/2)-1;	
	} else if(total_load>HASH_MAP_LOAD_FACTOR_MAX) {
		new_bucket_count = ((map->bucket_count+1)*2)-1;
	} else {
		return 0;
	}

	if(new_bucket_count<=HASH_MAP_SIZE_MIN) return 0;

	hash_map_rehash(map,new_bucket_count);

	return 1;
}

int hash_map_insert( struct hash_map *map, const char *key, void *value )
{
	if(!map || !key || !value) return 0;

	/*
	Extraordinary case: if there were a large number of prior inserts done
	while rehashing was inhibited due to active cursors, we might run out
	of space.  Complain but then rehash anyway, resulting in confused
	(but still valid) cursors.
	*/
	
	if((map->live_count + map->dead_count)>=map->count_count) {
		notice("WARNING: forced hash_map_rehash even with cursors active!");
		unsigned new_bucket_count = ((map->bucket_count+1)*2)-1;
		hash_map_rehash(map,new_bucket_count);
	}
	
	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % map->bucket_count;

	while(1) {
		struct hash_map_entry *e = map->bucket[pos];
		if(!e) {
			map->live_count++;
			map->dead_count--;
			break;
		} else if(e==HASH_MAP_TOMBSTONE) {
			map->dead_count--;
			break;
		} else {
			pos++;
			if(pos>=map->bucket_count) {
				pos = 0;
			}
		}
	}

	map->bucket[pos] = hash_map_entry_create(key,value,hashkey);
	
	hash_map_consider_rehash(map);
	
	return 1;
}

void * hash_map_lookup( struct hash_map *map, const char *key )
{
	if(!map || !key) return 0;

	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % bucket_count;

	for( ; ; pos = (pos+1) % bucket_count ) {
		struct hash_map_entry *e = map->bucket[pos];
		if(!e) {
			return 0;
		} else {
			if(e==HASH_MAP_TOMBSTONE) {
				continue;
			} else if(!strcmp(key,e->key)) {
				return e->value;
			} else {
				continue;
			}
		}
	}

	/* NOTREACHED */
	return 0;
}

void * hash_map_remove( struct hash_map *map, const char *key )
{
	if(!map || !key) return 0;

	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % bucket_count;

	while(1) {
		struct hash_map_entry *e = map->bucket[pos];
		if(e==HASH_MAP_TOMBSTONE) {
			pos++;
			if(pos>=map->bucket_count) pos = 0;
			continue;
		} else if(e && !strcmp(key,e->key)) {
			void *value = e->value;
			hash_map_entry_delete(e);
			map->bucket[pos] = HASH_MAP_TOMBSTONE;
			return value;
		} else {
			return 0;
		}
	}

	hash_map_consider_rehash(map);
	
	/* NOTREACHED */
	return 0;
}

struct hash_map_cursor * hash_map_cursor_create( struct hash_map *map )
{
	struct hash_map_cursor *cursor = malloc(sizeof(*cursor));
	cursor->owner = map;
	cursor->position = 0;
	cursor->map->active_cursors++;
}

void hash_map_cursor_seek( struct hash_map_cursor *cursor, int pos )
{
	cursor->position = position % cursor->map->bucket_count;

	struct hash_map_entry *e = map->bucket[pos];
	if(!e || e==HASH_MAP_TOMBSTONE) hash_map_cursor_next(cursor);
}

int  hash_map_cursor_get( struct hash_map_cursor *cursor, void **value )
{
	struct hash_map_entry *e = map->bucket[cursor->pos];

	if(!e || e==HASH_MAP_TOMBSTONE) {
	  return 0;
	} else {
	  *value = e->value;
	  return 1;
	}
}

int  hash_map_cursor_next( struct hash_map_cursor *cursor )
{
	while(1) {
		cursor->position++;
		if(pos>=cursor->map->bucket_count) {
			cursor->position = 0;
			return 0;
		}

		struct hash_map_entry *e = map->bucket[pos];

		if(e==HASH_MAP_TOMBSTONE) {
			continue;
		} else if(e) {
			return 1;
		} else {
			continue;
		}
	}
}

void hash_map_cursor_delete( struct hash_map_cursor *cursor )
{
	cursor->map->active_cursors--;
	hash_map_consider_rehash(map);
	free(cursor);
}
