/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ITABLE_H
#define ITABLE_H

#include "int_sizes.h"

/** @file itable.h An integer-indexed hash table.
This hash table module maps integers to arbitrary objects (void pointers).
For example, to store a filename using the file descriptor as a key:
<pre>
struct itable *t;
t = itable_create(0);

fd = open(pathname,O_RDONLY,0);

itable_insert(t,fd,pathname);
pathname = itable_remove(h,id);

</pre>

To list all of the items in an itable, use @ref itable_firstkey and @ref itable_nextkey like this:

<pre>
UINT64_T  key;
void *value;

itable_firstkey(h);
while(itable_nextkey(h,&key,&value)) {
	printf("table contains: %d\n",key);
}
</pre>

Alternatively:

<pre>
UINT64_T  key;
void *value;

ITABLE_ITERATE(h,key,value) {
	printf("table contains: %d\n",key);
}
</pre>
*/

/** Create a new integer table.
@param buckets The number of buckets in the table.  If zero, a default value will be used.
@return A pointer to a new integer table.
*/

struct itable *itable_create(int buckets);

/** Remove all entries from an integer table.
@param h The integer table to delete.
@param delete_func If non-null, will be invoked on each object to delete it.
*/

void itable_clear( struct itable *h, void (*delete_func)(void*) );

/** Delete an integer table.
Note that this function will not delete all of the objects contained within the integer table.
@param h The integer table to delete.
*/

void itable_delete(struct itable *h);


/** Return an array with all the current keys.
It is the responsibility of the caller to free this array with
itable_free_keys_array.
@param h A pointer to a hash table.
@return An array of all the current keys.
*/

UINT64_T *itable_keys_array(struct itable *h);


/** Free an array generated from itable_free_keys_array.
@param keys An array of all the keys.
*/

void itable_free_keys_array(UINT64_T *keys);


/** Count the entries in an integer table.
@return The number of entries in the table.
@param h A pointer to an integer table.
*/

int itable_size(struct itable *h);

/** Get the proportion of elements vs buckets in the table.
@return The load of the table.
@param h A pointer to an integer table.
*/

double itable_load(struct itable *h);


/** Insert a key and value.
This call will replace the value if it already contains the same key.
Also note that you cannot insert a null value into the table.
@param h A pointer to an integer table.
@param key An integer key
@param value A pointer to store with the key.
@return One if the insert succeeded, failure otherwise
*/

int itable_insert(struct itable *h, UINT64_T key, const void *value);

/** Look up a value by key.
@param h A pointer to an integer table.
@param key An integer key to search for.
@return If found, the pointer associated with the key, otherwise null.
*/

void *itable_lookup(struct itable *h, UINT64_T key);

/** Remove a value by key.
@param h A pointer to an integer table.
@param key An integer key to remove.
@return If found, the pointer associated with the key, otherwise null.
*/

void *itable_remove(struct itable *h, UINT64_T key);

/** Remove any one value.
@param h A pointer to an integer table.
@return One object removed from the table.
*/

void * itable_pop( struct itable *h );

/** Begin iteration over all keys.
This function begins a new iteration over an integer table,
allowing you to visit every key and value in the table.
Next, invoke @ref itable_nextkey to retrieve each value in order.
@param h A pointer to an integer table.
*/

void itable_firstkey(struct itable *h);

/** Continue iteration over all keys.
This function returns the next key and value in the iteration.
@param h A pointer to an integer table.
@param key A pointer to a key integer.
@param value A pointer to a value pointer. (can be NULL)
@return Zero if there are no more elements to visit, one otherwise.
*/

int itable_nextkey(struct itable *h, UINT64_T * key, void **value);

/** Utility macro to simplify common case of iterating over an itable.
Use as follows:

<pre>
UINT64_T key;
void *value;

ITABLE_ITERATE(table,key,value) {
	printf("table contains: %lld\n",key);
}

</pre>
*/

#define ITABLE_ITERATE(table,key,value) itable_firstkey(table); while(itable_nextkey(table,&key,(void**)&value))

#endif
