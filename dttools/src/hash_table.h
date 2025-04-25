/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
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

To list all of the items in a hash table, use @ref hash_table_firstkey and @ref hash_table_nextkey like this:

<pre>
char *key;
void *value;

hash_table_firstkey(table);
while(hash_table_nextkey(&key,&value)) {
	printf("table contains: %s\n",key);
}
</pre>
*/

/** The type signature for a hash function given to @ref hash_table_create */

typedef unsigned (*hash_func_t) (const char *key);

/** Create a new hash table.
@param buckets The number of buckets in the table.  If zero, a default value will be used.
@param func The default hash function to be used.  If zero, @ref hash_string will be used.
@return A pointer to a new hash table.
*/

struct hash_table *hash_table_create(int buckets, hash_func_t func);

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

/** Begin iteration over all keys.
This function begins a new iteration over a hash table,
allowing you to visit every key and value in the table.
Next, invoke @ref hash_table_nextkey to retrieve each value in order.
@param h A pointer to a hash table.
*/

void hash_table_firstkey(struct hash_table *h);

/** Continue iteration over all keys.
This function returns the next key and value in the iteration.
Warning: It cannot be called after either hash_table_insert or hash_table_remove
during the same iteration. If this is needed, consider
iterating using manual hash_table_lookup with keys from hash_table_keys_array.
@param h A pointer to a hash table.
@param key A pointer to a key pointer.
@param value A pointer to a value pointer.
@return Zero if there are no more elements to visit, one otherwise.
*/

int hash_table_nextkey(struct hash_table *h, char **key, void **value);

/** Begin iteration over all keys from a random offset.
This function begins a new iteration over a hash table,
allowing you to visit every key and value in the table.
Next, invoke @ref hash_table_nextkey_with_offset to retrieve each value in order.
@param h A pointer to a hash table.
@param offset_bookkeep An integer to pointer where the origin to the iteration is recorded.
*/

void hash_table_randomkey(struct hash_table *h, int *offset_bookkeep);

/** Continue iteration over all keys from an arbitray offset.
This function returns the next key and value in the iteration.
@param h A pointer to a hash table.
@param offset_bookkeep The origin for this iteration. See @ref hash_table_randomkey
@param key A pointer to a key pointer.
@param value A pointer to a value pointer.
@return Zero if there are no more elements to visit, one otherwise.
*/

int hash_table_nextkey_with_offset(struct hash_table *h, int offset_bookkeep, char **key, void **value);

/** Begin iteration at the given key.
Invoke @ref hash_table_nextkey to retrieve each value in order.
Note that subsequent calls to this functions may result in different iteration orders as the hash_table may have been
resized.
@param h A pointer to a hash table.
@param key A string key to search for.
@return Zero if key not in hash table, one otherwise.
*/

int hash_table_fromkey(struct hash_table *h, const char *key);

/** A default hash function.
@param s A string to hash.
@return An integer hash of the string.
*/

unsigned hash_string(const char *s);

/** Return a NULL-termianted array with a copy of all the current keys.
It is the responsibility of the caller to free this array with
hash_table_free_keys_array.
@param h A pointer to a hash table.
@return NULL-termianted array of size with a copy of all the current keys.
*/

char **hash_table_keys_array(struct hash_table *h);


/** Free an array generated from hash_table_free_keys_array.
@param keys NULL-termianted array of size with a copy of keys.
*/
void hash_table_free_keys_array(char **keys);

/** Utility macro to simplify common case of iterating over a hash table.
Use as follows:

<pre>
char *key;
void *value;

HASH_TABLE_ITERATE(table,key,value) {
	printf("table contains: %s\n",key);
}

</pre>
*/

#define HASH_TABLE_ITERATE( table, key, value ) hash_table_firstkey(table); while(hash_table_nextkey(table,&key,(void**)&value))

#define HASH_TABLE_ITERATE_RANDOM_START( table, offset_bookkeep, key, value ) hash_table_randomkey(table, &offset_bookkeep); while(hash_table_nextkey_with_offset(table, offset_bookkeep, &key, (void **)&value))

#define HASH_TABLE_ITERATE_FROM_KEY( table, iter_control, iter_count_var, key_start, key, value ) \
	iter_control = 0; \
	iter_count_var = 0; \
  hash_table_fromkey(table, key_start); \
	while(iter_count_var < hash_table_size(table) && (iter_count_var+=1 && (hash_table_nextkey(table, &key, (void **)&value) || (!iter_control && (iter_control+=1) && hash_table_fromkey(table, NULL) && hash_table_nextkey(table, &key, (void **)&value)))))

#endif
