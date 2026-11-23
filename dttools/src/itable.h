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
int iteration;

ITABLE_ITERATE(h, iteration, &key, &value)) {
	printf("table contains: %d\n",key);
}
</pre>

Alternatively:

<pre>
UINT64_T  key;
void *value;
int iteration;

ITABLE_ITERATE(h, iteration, key, value) {
	printf("table contains: %d\n",key);
}
</pre>
*/

/** Create a new integer table.
@param buckets The number of buckets in the table.  If zero, a default value will be used.
@return A pointer to a new integer table.
*/

struct itable *itable_create(unsigned buckets);

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

/** Count the entries in an integer table.
@return The number of entries in the table.
@param h A pointer to an integer table.
*/

int itable_size(struct itable *h);

/** Insert a key and value.
If the table already contains the same key with the same value, nothing changes.
If the table already contains the same key with a different value,
the new value replaces the old one and a warning is emitted.
This is a bug in the caller, and leaks the old entry: call
@ref itable_remove first to replace a value cleanly.
An insert while a cursor is active on the table is not permitted,
and the program will abort.
@param h A pointer to an integer table.
@param key An integer key
@param value A pointer to store with the key.
@return One if the insert succeeded, zero if the table is null.
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

void * itable_remove_any( struct itable *h );

/** Remove a value at random.
@param h A pointer to an integer table.
@return One object removed from the table.
*/

void * itable_remove_random( struct itable *h );

/** Iterate over all entries in an integer table.
This function invokes a callback on every key and value in the table.
Unlike @ref ITABLE_ITERATE, this function does not use the table's
iteration state, so it may be called while another iteration is in
progress.  The table must not be modified during the call; if an
insert, compact, clear, or resize occurs, the program will abort.
@param h A pointer to an integer table.
@param func A callback invoked once for each entry.
@param arg An arbitrary pointer passed to each invocation of @a func.
*/

void itable_foreach_ro(struct itable *h, void (*func)(UINT64_T key, void *value, void *arg), void *arg);

/** Create a cursor for iterating over the values of a table.
@param table A pointer to a itable.
@return A pointer to a cursor object.
*/

struct itable_cursor * itable_cursor_create( struct itable *table );

/** Move a cursor to a specific position in the table.
The position zero indicates the first available object
in the table, allowing for iterating over the entire
set of objects in the table.  Positions other than zero
are currently undefined.
@param cursor A pointer to a cursor object.
@param pos The desired position of the cursor.
*/

void itable_cursor_seek( struct itable_cursor *cursor, unsigned pos );

/** Get the item under a cursor.
If the cursor is positioned over a valid object,
fill in the key and value and return true.
If the cursor is invalid, simply return false.
@param cursor A pointer to a cursor object.
@param key A pointer to be filled in with the current key.
@param value A pointer to be filled in with the current value.
@return True if the cursor was positioned over a valid object.
*/

int  itable_cursor_get( struct itable_cursor *cursor, UINT64_T *key, void **value );

/** Move the cursor to the next object.
Continue iteration by moving the cursor to the next available object and return true.
If there are no more objects, move it to the invalid position and return false.
@param A pointer to a cursor object.
@return True if the cursor is now positioned over a valid object.
*/
int  itable_cursor_next( struct itable_cursor *cursor );

/** Delete a cursor.
@param A pointer to a cursor object.
*/

void itable_cursor_delete( struct itable_cursor *cursor );
void itable_cursor_delete_pp( struct itable_cursor **cursor );

/** Utility macro to simplify common case of iterating over an itable
Use as follows:

<pre>
UINT64_T key;
void *value;
int iteration;

ITABLE_ITERATE(table, iteration, key, value) {
	printf("table contains: %lld\n",key);
}

</pre>
*/

#define ITABLE_ITERATE( table, iteration, key, value ) \
  (void) iteration; \
  for( struct itable_cursor *__cursor  __attribute__((cleanup(itable_cursor_delete_pp))) = itable_cursor_create(table), *__keep_going=__cursor; __keep_going; __keep_going=0) \
    for( itable_cursor_seek(__cursor,0); itable_cursor_get(__cursor,&key,(void**)&value); itable_cursor_next(__cursor) )

#endif
