
#include "hash_table.h"
#include "hash_string.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct hash_table_entry {
	char *key;
	void *value;
	unsigned hashkey;
};

struct hash_table {
	struct hash_table_entry **buckets;
	hash_func_t hashfunc;
	unsigned bucket_count;	 /* Length of the buckets array. */
	unsigned live_count;	 /* Number of valid entries in array. */
	unsigned dead_count;	 /* Number of tombstone entries in array. */
	unsigned active_cursors; /* Number of cursors created on this table. */
};

struct hash_table_cursor {
	unsigned position;
	struct hash_table *table;
};

#define HASH_TABLE_LOAD_FACTOR_MIN 10
#define HASH_TABLE_LOAD_FACTOR_MAX 30
#define HASH_TABLE_SIZE_MIN 127

/* A value indicating a deleted object occupied this bucket. */
#define HASH_TABLE_TOMBSTONE ((struct hash_table_entry *)(-1))

/* A value indicating that a cursor does not point to a valid entry. */
#define HASH_TABLE_CURSOR_INVALID ((unsigned)-1)

static struct hash_table_entry *hash_table_entry_create(const char *key, const void *value, unsigned hashkey)
{
	struct hash_table_entry *e = malloc(sizeof(*e));
	e->key = xxstrdup(key);
	/* Note that e->value is neither allocated nor deleted by this module. */
	e->value = (void*) value;
	e->hashkey = hashkey;
	return e;
}

static void hash_table_entry_delete(struct hash_table_entry *e)
{
	free(e->key);
	/* Note that e->value is neither allocated nor deleted by this module. */
	free(e);
}

struct hash_table *hash_table_create(unsigned bucket_count, hash_func_t hashfunc)
{
	struct hash_table *table = malloc(sizeof(*table));

	if (bucket_count < 1)
		bucket_count = HASH_TABLE_SIZE_MIN;
	if (!hashfunc)
		hashfunc = hash_string;

	table->buckets = calloc(bucket_count, sizeof(struct hash_table_entry *));
	table->hashfunc = hashfunc;
	table->bucket_count = bucket_count;
	table->live_count = 0;
	table->dead_count = 0;
	table->active_cursors = 0;

	return table;
}

void hash_table_delete(struct hash_table *table)
{
	hash_table_clear(table, 0);
	free(table->buckets);
	free(table);
}

int hash_table_size(struct hash_table *table)
{
	return table->live_count;
}

double hash_table_load(struct hash_table *table)
{
	return ((double)table->live_count) / table->bucket_count;
}

/*
Remove and dispose of all items in the hash table, applying
the providing deletefunc to each one.  Note that this routine
shortcuts the usual delete function, so as to avoid
(a) a table full of tombstones and (b) a forced rehash,
since the likely next step is to delete the table.
*/

void hash_table_clear(struct hash_table *table, void (*deletefunc)(void *))
{
	for (unsigned i = 0; i < table->bucket_count; i++) {
		struct hash_table_entry *e = table->buckets[i];
		if (e) {
			if (e != HASH_TABLE_TOMBSTONE) {
				if (deletefunc)
					deletefunc(e->value);
				hash_table_entry_delete(e);
			}
			table->buckets[i] = 0;
		}
	}
	table->live_count = 0;
	table->dead_count = 0;
}

/*
Force a rehash of the table by allocating a table with "new_bucket_count",
transferring all of the live objects over, and disposing of the old bucket array.
Note that a rehash may invalidate current cursors into the table.
*/

static void hash_table_rehash(struct hash_table *table, unsigned new_bucket_count)
{
	struct hash_table_entry **new_buckets = calloc(new_bucket_count, sizeof(struct hash_table_entry *));

	for (unsigned i = 0; i < table->bucket_count; i++) {
		struct hash_table_entry *e = table->buckets[i];
		if (e && e != HASH_TABLE_TOMBSTONE) {
			unsigned newpos = e->hashkey % new_bucket_count;
			while (new_buckets[newpos]) {
				newpos++;
				if (newpos > new_bucket_count)
					newpos = 0;
			}
			new_buckets[newpos] = e;
		}
	}

	free(table->buckets);

	table->buckets = new_buckets;
	table->bucket_count = new_bucket_count;
	table->dead_count = 0;
}

static void hash_table_shrink(struct hash_table *table)
{
	unsigned new_bucket_count = ((table->bucket_count + 1) / 2) - 1;
	hash_table_rehash(table, new_bucket_count);
}

static void hash_table_grow(struct hash_table *table)
{
	unsigned new_bucket_count = ((table->bucket_count + 1) * 2) - 1;
	hash_table_rehash(table, new_bucket_count);
}

/* Consider the current size of the table and grow or shrink as necessary. */

int hash_table_consider_rehash(struct hash_table *table)
{
	/* Never rehash the table while cursors are actively iterating. */
	if (table->active_cursors > 0)
		return 0;

	unsigned live_load = table->live_count * 100 / table->bucket_count;
	unsigned total_load = (table->live_count + table->dead_count) * 100 / table->bucket_count;

	if (live_load < HASH_TABLE_LOAD_FACTOR_MIN && table->bucket_count > HASH_TABLE_SIZE_MIN) {
		hash_table_shrink(table);
		return table->bucket_count;
	} else if (total_load > HASH_TABLE_LOAD_FACTOR_MAX) {
		hash_table_grow(table);
		return table->bucket_count;
	} else {
		return 0;
	}
}

/*
Insert an item into the hash_table and then rehash if necessary.
Note that *any* insertion is disallowed while a cursor is active,
because insertion might require a rehash which would confuse cursors.

Why not just optimistically permit insertions as long as there is room?
Because this would result in a *rare* failure when repeated inserts
left no room in the table, requiring a (prohibited) rehash.

We prefer to establish a strict, easily testable rule rather than
leave a rare, difficult to test bug.
*/

int hash_table_insert(struct hash_table *table, const char *key, const void *value)
{
	if (!table || !key)
		return 0;

	if (table->active_cursors) {
		fatal("hash_table_insert attempted on a table with active cursors!");
		/* NOTREACHED */
		return 0;
	}

	unsigned hashkey = table->hashfunc(key);
	unsigned pos = hashkey % table->bucket_count;

	for (;; pos = (pos + 1) % table->bucket_count) {
		struct hash_table_entry *e = table->buckets[pos];
		if (e) {
			if (e == HASH_TABLE_TOMBSTONE) {
				/* tombstone: insert here */
				table->dead_count--;
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

	table->live_count++;
	table->buckets[pos] = hash_table_entry_create(key, value, hashkey);

	hash_table_consider_rehash(table);

	return 1;
}

void *hash_table_lookup(struct hash_table *table, const char *key)
{
	if (!table || !key)
		return 0;

	struct hash_table_entry *e = 0;

	unsigned hashkey = table->hashfunc(key);
	unsigned pos = hashkey % table->bucket_count;

	for (;; pos = (pos + 1) % table->bucket_count) {
		e = table->buckets[pos];
		if (e) {
			if (e == HASH_TABLE_TOMBSTONE) {
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
Internal function to remove item at specific position already
determined to be valid by the caller.  Adjust counts,
convert the entry to a tombstone, consider rehashing,
and return the value.
*/

static void * hash_table_remove_at_position(struct hash_table *table, unsigned position)
{
	struct hash_table_entry *e = table->buckets[position];
	if(!e || e==HASH_TABLE_TOMBSTONE) return 0;

	void *value = e->value;
	hash_table_entry_delete(e);
	table->buckets[position] = HASH_TABLE_TOMBSTONE;
	table->live_count--;
	table->dead_count++;

	hash_table_consider_rehash(table);

	return value;
}

/*
Remove an item from the hash_table.
This operation is safe because removals happen in place,
and rehashes are suppressed while cursors are active.
*/

void *hash_table_remove(struct hash_table *table, const char *key)
{
	if (!table || !key)
		return 0;

	struct hash_table_entry *e = 0;

	unsigned hashkey = table->hashfunc(key);
	unsigned pos = hashkey % table->bucket_count;

	for (;; pos = (pos + 1) % table->bucket_count) {
		e = table->buckets[pos];
		if (e) {
			if (e == HASH_TABLE_TOMBSTONE) {
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

	return hash_table_remove_at_position(table,pos);
}

void * hash_table_pop(struct hash_table *table)
{
	char *key=0;
	void *value=0;
	struct hash_table_cursor *cursor = hash_table_cursor_create(table);
	hash_table_cursor_get(cursor,&key,&value);
	hash_table_cursor_delete(cursor);
	return value;
}

char **hash_table_keys(struct hash_table *table)
{
	char **keys = malloc(sizeof(char *) * (table->live_count + 1));
	char *key = 0;
	void *value = 0;
	int i = 0;
	int iteration;
	HASH_TABLE_ITERATE(table, iteration, key, value)
	{
		keys[i++] = strdup(key);
	}
	keys[i] = 0;
	return keys;
}

void hash_table_keys_delete(char **keys)
{
	for (int i = 0; keys[i]; i++) {
		free(keys[i]);
	}
	free(keys);
}

void hash_table_keys_delete_ppp(char ***keys)
{
	hash_table_keys_delete(*keys);
}

/*
Create a new cursor object, which will be initially positioned
at the first valid entry in the hash_table, or at the INVALID position,
if the hash_table is empty.
*/

struct hash_table_cursor *hash_table_cursor_create(struct hash_table *table)
{
	struct hash_table_cursor *cursor = malloc(sizeof(*cursor));

	cursor->position = 0;
	cursor->table = table;
	cursor->table->active_cursors++;

	hash_table_cursor_seek(cursor, 0);

	return cursor;
}

/*
Move the cursor to the first valid entry in the hash_table
at a location equal or greater than the position argument.
If there is no such entry, go to the INVALID position.
A position of zero indicates the first valid entry.
(It's not clear if this function is useful for any position other than zero.)
*/

void hash_table_cursor_seek(struct hash_table_cursor *cursor, unsigned position)
{
	if (position >= cursor->table->bucket_count) {
		cursor->position = HASH_TABLE_CURSOR_INVALID;
	} else {
		cursor->position = position;
		struct hash_table_entry *e = cursor->table->buckets[cursor->position];
		if (!e || e == HASH_TABLE_TOMBSTONE) {
			hash_table_cursor_next(cursor);
		}
	}
}

/*
If the cursor points to a valid position,
give back that key:value and return true.
Otherwise return false.
*/

int hash_table_cursor_get(struct hash_table_cursor *cursor, char **key, void **value)
{
	/* If the cursor is already invalid, return false. */
	if (cursor->position == HASH_TABLE_CURSOR_INVALID) {
		return 0;
	}

	/* If the position is off the end, convert it to invalid and return false. */
	/* This shouldn't happen b/c rehashing is inhibited by cursors. */
	if (cursor->position >= cursor->table->bucket_count) {
		cursor->position = HASH_TABLE_CURSOR_INVALID;
		return 0;
	}

	/* Grab the bucket at that position, which may be null. */
	struct hash_table_entry *e = cursor->table->buckets[cursor->position];

	/* If there is no object at this position, return false.. */
	/* This would only happen if the object was removed */
	/* after the cursor advanced to this position. */
	if (!e || e == HASH_TABLE_TOMBSTONE) {
		return 0;
	}

	/* Otherwise, we have a good value and return true. */
	*key = e->key;
	*value = e->value;
	return 1;
}

/*
Remove the item at the current cursor position.
Further gets at this position will return null,
until the cursor is advanced to the next position.
*/

void * hash_table_cursor_remove(struct hash_table_cursor *cursor )
{
	if(cursor->position==HASH_TABLE_CURSOR_INVALID) {
		return 0;
	}

	if(cursor->position>=cursor->table->bucket_count) {
		return 0;
	}

	return hash_table_remove_at_position(cursor->table,cursor->position);
}

/*
Advance the cursor to the next valid position in the hash_table
and return true. If there is no valid next position, then
return false.
*/

int hash_table_cursor_next(struct hash_table_cursor *cursor)
{
	if (cursor->position == HASH_TABLE_CURSOR_INVALID) {
		return 0;
	}

	while (1) {
		cursor->position++;

		if (cursor->position >= cursor->table->bucket_count) {
			cursor->position = HASH_TABLE_CURSOR_INVALID;
			return 0;
		}

		struct hash_table_entry *e = cursor->table->buckets[cursor->position];

		if (e == HASH_TABLE_TOMBSTONE) {
			continue;
		} else if (e) {
			return 1;
		} else {
			continue;
		}
	}
}

void hash_table_cursor_delete(struct hash_table_cursor *cursor)
{
	cursor->table->active_cursors--;
	free(cursor);
}

/* Helper function to assist with the auto-cleanup attribute. */

void hash_table_cursor_delete_pp(struct hash_table_cursor **cursor)
{
	hash_table_cursor_delete(*cursor);
}
