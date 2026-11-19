/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

/** @file hash_table.h A general purpose hash table.
This hash table module maps C strings to arbitrary objects (void pointers).
For example, to store a file object using the pathname as a key:
<pre>
struct hash_table *h;
h = hash_table_create(0,0);

FILE * file = fopen(pathname,"r");

hash_table_insert(h,pathname,file);
file = hash_table_lookup(h,pathname);
file = hash_table_remove(h,pathname);

</pre>

Use the following macro to safely iterate over all items in a table:

<pre>
char *key;
void *value;
int iteration;

HASH_TABLE_ITERATE(table, iteration, &key, &value) {
	printf("table contains: %s\n",key);
}
</pre>

Note that iteration has several important properties:
- @ref HASH_TABLE_ITERATE can be safely nested, because each call uses an independent cursor.
- @ref hash_table_insert is not permitted during iteration, to avoid a rehash upsetting the iteration order.
- @ref hash_table_remove is permitted during iteration, and the "key" value remains valid until the end of the loop.
*/

#include "hash_string.h"

/** The type signature for a hash function given to @ref hash_table_create */

typedef unsigned (*hash_func_t) (const char *key);

/** Create a new hash table.
@param buckets The number of buckets in the table.  If zero, a default value will be used.
@param func The default hash function to be used.  If zero, @ref hash_string will be used.
@return A pointer to a new hash table.
*/

struct hash_table *hash_table_create(unsigned buckets, hash_func_t func);

/** Remove all entries from an hash table.
@param h The hash table to delete.
@param delete_func If non-null, will be invoked on each object to delete it.
*/

void hash_table_clear(struct hash_table *h, void (*delete_func)( void *) );


/** Delete a hash table.
Note that this function will not delete all of the objects contained within the hash table.
@param h The hash table to delete.
*/

void hash_table_delete(struct hash_table *h);

/** Count the entries in a hash table.
@return The number of entries in the table.
@param h A pointer to a hash table.
*/

int hash_table_size(struct hash_table *h);

/** Return the proportion of elements
vs buckets in the table.
@return The load of the table.
@param h A pointer to a hash table.
*/

double hash_table_load(struct hash_table *h);

/** Insert a key and value.
This call will fail if the table already contains the same key.
You must call @ref hash_table_remove to remove it.
Also note that you cannot insert a null value into the table.
@param h A pointer to a hash table.
@param key A pointer to a string key which will be hashed and duplicated.
@param value A pointer to store with the key.
@return One if the insert succeeded, failure otherwise
*/

int hash_table_insert(struct hash_table *h, const char *key, const void *value);

/** Look up a value by key.
@param h A pointer to a hash table.
@param key A string key to search for.
@return If found, the pointer associated with the key, otherwise null.
*/

void *hash_table_lookup(struct hash_table *h, const char *key);

/** Remove a value by key.
@param h A pointer to a hash table.
@param key A string key to remove.
@return If found, the pointer associated with the key, otherwise null.
*/

void *hash_table_remove(struct hash_table *h, const char *key);

/** Remove any one value that is most convenient.
@param h A pointer to a hash table.
@return One object removed from the table.
*/

void *hash_table_remove_any( struct hash_table *h );

/** Remove a value from a random position in the table.
@param h A pointer to a hash table.
@return One object removed from the table.
*/

void *hash_table_remove_random( struct hash_table *h );

/** Iterate over all entries in a hash table.
This function invokes a callback on every key and value in the table.
The table must not be modified during the call; if an insert occurs, the program will abort.
@param h A pointer to a hash table.
@param func A callback invoked once for each entry.
@param arg An arbitrary pointer passed to each invocation of @a func.
*/

void hash_table_foreach_ro(struct hash_table *h, void (*func)(const char *key, void *value, void *arg), void *arg);

/** Return a NULL-terminated array with a copy of all the current keys.
It is the responsibility of the caller to free this array with
@ref hash_table_keys_delete.
@param h A pointer to a hash table.
@return NULL-termianted array of size with a copy of all the current keys.
*/

char **hash_table_keys(struct hash_table *h);


/** Free an array generated from hash_table_free_keys_array.
@param keys NULL-terminated array of size with a copy of keys.
*/
void hash_table_keys_delete(char **keys);
void hash_table_keys_delete_ppp( char ***keys );

/** Create a cursor for iterating over the values of a table.
@param table A pointer to a hash table.
@return A pointer to a cursor object.
*/

struct hash_table_cursor * hash_table_cursor_create( struct hash_table *table );

/** Move a cursor to a specific position in the table.
The position zero indicates the first available object
in the table, allowing for iterating over the entire
set of objects in the table.  Positions other than zero
are currently undefined.
@param cursor A pointer to a cursor object.
@param pos The desired position of the cursor.
*/

void hash_table_cursor_seek( struct hash_table_cursor *cursor, unsigned pos );

/** Get the item under a cursor.
If the cursor is positioned over a valid object,
fill in the key and value and return true.
If the cursor is invalid, simply return false.
@param cursor A pointer to a cursor object.
@param key A pointer to be filled in with the current key.
@param value A pointer to be filled in with the current value.
@return True if the cursor was positioned over a valid object.
*/

int  hash_table_cursor_get( struct hash_table_cursor *cursor, char **key, void **value );

/** Move the cursor to the next object.
Continue iteration by moving the cursor to the next available object and return true.
If there are no more objects, move it to the invalid position and return false.
@param A pointer to a cursor object.
@return True if the cursor is now positioned over a valid object.
*/
int  hash_table_cursor_next( struct hash_table_cursor *cursor );

/** Delete a cursor.
@param A pointer to a cursor object.
*/

void hash_table_cursor_delete( struct hash_table_cursor *cursor );
void hash_table_cursor_delete_pp( struct hash_table_cursor **cursor );

/** Utility macro to simplify common case of iterating over a hash table.
Use as follows:

<pre>
char *key;
void *value;
int iteration;

HASH_TABLE_ITERATE(table, iteration, key, value) {
	printf("table contains: %s\n",key);
}

</pre>
*/

#define HASH_TABLE_ITERATE( table, iteration, key, value ) \
  (void) iteration; \
  for( struct hash_table_cursor *__cursor  __attribute__((cleanup(hash_table_cursor_delete_pp))) = hash_table_cursor_create(table), *__keep_going=__cursor; __keep_going; __keep_going=0) \
    for( hash_table_cursor_seek(__cursor,0); hash_table_cursor_get(__cursor,&key,(void**)&value); hash_table_cursor_next(__cursor) )

#define HASH_TABLE_ITERATE_SNAPSHOT( table, key, value ) \
  for( char **__keys __attribute__((cleanup(hash_table_keys_delete_ppp))) = hash_table_keys(table), **__keep_going=__keys; __keep_going; __keep_going=0) \
    for( int __i=0; key=__keys[__i], value=hash_table_lookup(table,key); __i++ )

#endif
