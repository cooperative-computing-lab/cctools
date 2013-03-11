/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ITABLE_H
#define ITABLE_H

#include "int_sizes.h"

/** @file itable.h An integer-indexed hash table.
This hash table module map integers to arbitrary objects (void pointers).
For example, to store a filename using the file descriptor as a key:
<pre>
struct itable *t;
t = itable_create(0);

fd = open(pathname,O_RDONLY,0);

itable_insert(t,fd,pathname);
pathname = itable_remove(h,id);

</pre>

To list all of the items in a itable, use @ref itable_firstkey and @ref itable_nextkey like this:

<pre>
UINT64_T  key;
void *value;

itable_firstkey(h);

while(itable_nextkey(h,&key,&value)) {
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
Note that this function will not delete all of the objects contained within the integer table.
@param h The integer table to delete.
*/

void itable_clear(struct itable *h);

/** Delete an integer table.
Note that this function will not delete all of the objects contained within the integer table.
@param h The integer table to delete.
*/

void itable_delete(struct itable *h);

/** Count the entries in an integer table.
@return The number of entries in the table.
@param h A pointer to an integer table.
*/

int itable_size(struct itable *h);

/** Insert a key and value.
This call will fail if the table already contains the same key.
You must call @ref itable_remove to remove it.
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
@param value A pointer to a value pointer.
@return Zero if there are no more elements to visit, one otherwise.
*/

int itable_nextkey(struct itable *h, UINT64_T * key, void **value);

#endif
