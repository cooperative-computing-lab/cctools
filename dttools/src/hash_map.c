
#include "hash_map.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct hash_map_entry {
	char *key;
	void *value;
	unsigned hashkey;
};

struct hash_map {
	struct hash_map_entry **buckets;
	hash_func_t hashfunc;
	unsigned bucket_count;	 /* Length of the buckets array. */
	unsigned live_count;	 /* Number of valid entries in array. */
	unsigned dead_count;	 /* Number of tombstone entries in array. */
	unsigned active_cursors; /* Number of cursors created on this map. */
};

struct hash_map_cursor {
	unsigned position;
	struct hash_map *map;
};

#define HASH_MAP_LOAD_FACTOR_MIN 10
#define HASH_MAP_LOAD_FACTOR_MAX 30
#define HASH_MAP_SIZE_MIN 127

/* A value indicating a deleted object occupied this bucket. */
#define HASH_MAP_TOMBSTONE ((struct hash_map_entry *)(-1))

/* A value indicating that a cursor does not point to a valid entry. */
#define HASH_MAP_CURSOR_INVALID ((unsigned)-1)

static struct hash_map_entry *hash_map_entry_create(const char *key, void *value, unsigned hashkey)
{
	struct hash_map_entry *e = malloc(sizeof(*e));
	e->key = xxstrdup(key);
	/* Note that e->value is neither allocated nor deleted by this module. */
	e->value = value;
	e->hashkey = hashkey;
	return e;
}

static void hash_map_entry_delete(struct hash_map_entry *e)
{
	free(e->key);
	/* Note that e->value is neither allocated nor deleted by this module. */
	free(e);
}

struct hash_map *hash_map_create(unsigned bucket_count, hash_func_t hashfunc)
{
	struct hash_map *map = malloc(sizeof(*map));

	if (bucket_count < 1)
		bucket_count = HASH_MAP_SIZE_MIN;
	if (!hashfunc)
		hashfunc = hash_string;

	map->buckets = calloc(bucket_count, sizeof(struct hash_map_entry *));
	map->hashfunc = hashfunc;
	map->bucket_count = bucket_count;
	map->live_count = 0;
	map->dead_count = 0;
	map->active_cursors = 0;

	return map;
}

void hash_map_delete(struct hash_map *map)
{
	hash_map_clear(map, 0);
	free(map->buckets);
	free(map);
}

int hash_map_size(struct hash_map *map)
{
	return map->live_count;
}

double hash_map_load(struct hash_map *map)
{
	return ((double)map->live_count) / map->bucket_count;
}

/*
Remove and dispose of all items in the hash table, applying
the providing deletefunc to each one.  Note that this routine
shortcuts the usual delete function, so as to avoid
(a) a table full of tombstones and (b) a forced rehash,
since the likely next step is to delete the table.
*/

void hash_map_clear(struct hash_map *map, void (*deletefunc)(void *))
{
	for (unsigned i = 0; i < map->bucket_count; i++) {
		struct hash_map_entry *e = map->buckets[i];
		if (e) {
			if (e != HASH_MAP_TOMBSTONE) {
				if (deletefunc)
					deletefunc(e->value);
				hash_map_entry_delete(e);
			}
			map->buckets[i] = 0;
		}
	}
	map->live_count = 0;
	map->dead_count = 0;
}

/*
Force a rehash of the table by allocating a table with "new_bucket_count",
transferring all of the live objects over, and disposing of the old bucket array.
Note that a rehash may invalidate current cursors into the table.
*/

static void hash_map_rehash(struct hash_map *map, unsigned new_bucket_count)
{
	struct hash_map_entry **new_buckets = calloc(new_bucket_count, sizeof(struct hash_map_entry *));

	for (unsigned i = 0; i < map->bucket_count; i++) {
		struct hash_map_entry *e = map->buckets[i];
		if (e && e != HASH_MAP_TOMBSTONE) {
			unsigned newpos = e->hashkey % new_bucket_count;
			while (new_buckets[newpos]) {
				newpos++;
				if (newpos > new_bucket_count)
					newpos = 0;
			}
			new_buckets[newpos] = e;
		}
	}

	free(map->buckets);

	map->buckets = new_buckets;
	map->bucket_count = new_bucket_count;
	map->dead_count = 0;
}

static void hash_map_shrink(struct hash_map *map)
{
	unsigned new_bucket_count = ((map->bucket_count + 1) / 2) - 1;
	hash_map_rehash(map, new_bucket_count);
}

static void hash_map_grow(struct hash_map *map)
{
	unsigned new_bucket_count = ((map->bucket_count + 1) * 2) - 1;
	hash_map_rehash(map, new_bucket_count);
}

/* Consider the current size of the table and grow or shrink as necessary. */

int hash_map_consider_rehash(struct hash_map *map)
{
	/* Never rehash the table while cursors are actively iterating. */
	if (map->active_cursors > 0)
		return 0;

	unsigned live_load = map->live_count * 100 / map->bucket_count;
	unsigned total_load = (map->live_count + map->dead_count) * 100 / map->bucket_count;

	if (live_load < HASH_MAP_LOAD_FACTOR_MIN && map->bucket_count > HASH_MAP_SIZE_MIN) {
		hash_map_shrink(map);
		return map->bucket_count;
	} else if (total_load > HASH_MAP_LOAD_FACTOR_MAX) {
		hash_map_grow(map);
		return map->bucket_count;
	} else {
		return 0;
	}
}

/*
Insert an item into the hash_map and then rehash if necessary.
Note that *any* insertion is disallowed while a cursor is active,
because insertion might require a rehash which would confuse cursors.

Why not just optimistically permit insertions as long as there is room?
Because this would result in a *rare* failure when repeated inserts
left no room in the table, requiring a (prohibited) rehash.

We prefer to establish a strict, easily testable rule rather than
leave a rare, difficult to test bug.
*/

int hash_map_insert(struct hash_map *map, const char *key, void *value)
{
	if (!map || !key)
		return 0;

	if (map->active_cursors) {
		fatal("hash_map_insert attempted on a table with active cursors!");
		/* NOTREACHED */
		return 0;
	}

	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % map->bucket_count;

	for (;; pos = (pos + 1) % map->bucket_count) {
		struct hash_map_entry *e = map->buckets[pos];
		if (e) {
			if (e == HASH_MAP_TOMBSTONE) {
				/* tombstone: insert here */
				map->dead_count--;
				break;
			}
			if (hashkey == e->hashkey && !strcmp(key, e->key)) {
				if (value == e->value) {
					/* identical key:value exists */
					return 1;
				} else {
					notice(D_DEBUG, "key %s already exists in hash table with different value, ignoring new value.", key);
					return 0;
				}
			} else {
				/* different key: keep looking */
				continue;
			}
		} else {
			/* empty bucket: insert here */
			break;
		}
	}

	map->live_count++;
	map->buckets[pos] = hash_map_entry_create(key, value, hashkey);

	hash_map_consider_rehash(map);

	return 1;
}

void *hash_map_lookup(struct hash_map *map, const char *key)
{
	if (!map || !key)
		return 0;

	struct hash_map_entry *e = 0;

	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % map->bucket_count;

	for (;; pos = (pos + 1) % map->bucket_count) {
		e = map->buckets[pos];
		if (e) {
			if (e == HASH_MAP_TOMBSTONE) {
				continue;
			} else if (hashkey == e->hashkey && !strcmp(key, e->key)) {
				break;
			} else {
				continue;
			}
		} else {
			return 0;
		}
	}

	return e->value;
}

/*
Remove an item from the hash_map.
This operation is safe because removals happen in place,
and rehashes are suppressed while cursors are active.
*/

void *hash_map_remove(struct hash_map *map, const char *key)
{
	if (!map || !key)
		return 0;

	struct hash_map_entry *e = 0;

	unsigned hashkey = map->hashfunc(key);
	unsigned pos = hashkey % map->bucket_count;

	for (;; pos = (pos + 1) % map->bucket_count) {
		e = map->buckets[pos];
		if (e) {
			if (e == HASH_MAP_TOMBSTONE) {
				continue;
			} else if (hashkey == e->hashkey && !strcmp(key, e->key)) {
				break;
			} else {
				continue;
			}
		} else {
			return 0;
		}
	}

	/* e now points to the entry to remove */
	void *value = e->value;
	hash_map_entry_delete(e);
	map->buckets[pos] = HASH_MAP_TOMBSTONE;
	map->live_count--;
	map->dead_count++;

	hash_map_consider_rehash(map);

	return value;
}

char **hash_map_keys(struct hash_map *map)
{
	char **keys = malloc(sizeof(char *) * (map->live_count + 1));
	char *key = 0;
	void *value = 0;
	int i = 0;
	HASH_MAP_ITERATE_READONLY(map, key, value)
	{
		keys[i++] = strdup(key);
	}
	keys[i] = 0;
	return keys;
}

void hash_map_keys_delete(char **keys)
{
	for (int i = 0; keys[i]; i++) {
		free(keys[i]);
	}
	free(keys);
}

void hash_map_keys_delete_ppp(char ***keys)
{
	hash_map_keys_delete(*keys);
}

/*
Create a new cursor object, which will be initially positioned
at the first valid entry in the hash_map, or at the INVALID position,
if the hash_map is empty.
*/

struct hash_map_cursor *hash_map_cursor_create(struct hash_map *map)
{
	struct hash_map_cursor *cursor = malloc(sizeof(*cursor));

	cursor->position = 0;
	cursor->map = map;
	cursor->map->active_cursors++;

	hash_map_cursor_seek(cursor, 0);

	return cursor;
}

/*
Move the cursor to the first valid entry in the hash_map
at a location equal or greater than the position argument.
If there is no such entry, go to the INVALID position.
A position of zero indicates the first valid entry.
(It's not clear if this function is useful for any position other than zero.)
*/

void hash_map_cursor_seek(struct hash_map_cursor *cursor, unsigned position)
{
	if (position >= cursor->map->bucket_count) {
		cursor->position = HASH_MAP_CURSOR_INVALID;
	} else {
		cursor->position = position;
		struct hash_map_entry *e = cursor->map->buckets[cursor->position];
		if (!e || e == HASH_MAP_TOMBSTONE) {
			hash_map_cursor_next(cursor);
		}
	}
}

/*
If the cursor points to a valid position,
give back that key:value and return true.
Otherwise return false.
*/

int hash_map_cursor_get(struct hash_map_cursor *cursor, char **key, void **value)
{
	/* If the cursor is already invalid, return false. */
	if (cursor->position == HASH_MAP_CURSOR_INVALID) {
		return 0;
	}

	/* If the position is off the end, convert it to invalid and return false. */
	/* This could happen if a hash_map was shrunk while an iterator was active. */
	if (cursor->position >= cursor->map->bucket_count) {
		cursor->position = HASH_MAP_CURSOR_INVALID;
		return 0;
	}

	/* Grab the bucket at that position, which may be null. */
	struct hash_map_entry *e = cursor->map->buckets[cursor->position];

	/* If there is no object at this position, advance the cursor. */
	/* This could happen if the item was removed during iteration. */
	/* Should we advance the cursor instead? */
	if (!e || e == HASH_MAP_TOMBSTONE) {
		hash_map_cursor_next(cursor);
		if (cursor->position == HASH_MAP_CURSOR_INVALID) {
			return 0;
		}
	}

	/* Otherwise, we have a good value and return true. */
	*key = e->key;
	*value = e->value;
	return 1;
}

/*
Advance the cursor to the next valid position in the hash_map
and return true. If there is no valid next position, then
return false.
*/

int hash_map_cursor_next(struct hash_map_cursor *cursor)
{
	if (cursor->position == HASH_MAP_CURSOR_INVALID) {
		return 0;
	}

	while (1) {
		cursor->position++;

		if (cursor->position >= cursor->map->bucket_count) {
			cursor->position = HASH_MAP_CURSOR_INVALID;
			return 0;
		}

		struct hash_map_entry *e = cursor->map->buckets[cursor->position];

		if (e == HASH_MAP_TOMBSTONE) {
			continue;
		} else if (e) {
			return 1;
		} else {
			continue;
		}
	}
}

void hash_map_cursor_delete(struct hash_map_cursor *cursor)
{
	cursor->map->active_cursors--;
	free(cursor);
}

void hash_map_cursor_delete_pp(struct hash_map_cursor **cursor)
{
	hash_map_cursor_delete(*cursor);
}
