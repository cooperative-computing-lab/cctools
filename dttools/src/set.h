/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SET_H
#define SET_H

#include "int_sizes.h"

/** @file set.h A set data structure.
Arbitrary objects that are equal (the same location in memory) appear only once
in the set.  For example, as a set of filenames:
<pre>
struct set *s;
s = set_create(0);

set_push(s,pathname);
set_push(s,pathname_b);
set_push(s,pathname);

assert(set_size(s) == 2);

path = set_pop(s);

assert(set_size(s) == 1);
</pre>

To list all of the elements in a set, use @ref set_first_element and @ref set_next_element like this:

<pre>
void *element;

set_first_element(s);
while(element = set_next_element(s)) {
	printf("set contains: %x\n", element);
}
</pre>

*/

/** Create a new set.
@param buckets The number of elements in the set.  If zero, a default element will be used. Increases dynamically as needed.
@return A pointer to a new set.
*/

struct set *set_create(int buckets);

/** Remove all entries from a set.
Note that this function will not free all of the objects contained within the set.
@param s The set to delete.
*/

void set_clear(struct set *s);

/** Delete a set.
Note that this function will not free all of the objects contained within the set.
@param s The set to delete.
*/

void set_delete(struct set *s);

/** Count the entries in a set.
@return The number of entries in the set.
@param s A pointer to a set.
*/

int set_size(struct set *s);

/** Insert a element to the set.
This call will return 0 if element was already in the set. 
You must call @ref set_remove to remove it.
Also note that you cannot insert a null element into the set.
@param s A pointer to a set.
@param element A pointer to store.
@return One if the insert succeeded, 0 otherwise.
*/

int set_insert(struct set *s, const void *element);

/** Insert a element to the set.
This is equivalent to set_insert
*/

int set_push(struct set *h, const void *element);

/** Look up a element in the set.
@param s A pointer to a set.
@param element A pointer to search for.
@return If found, 1, otherwise 0.
*/

int set_lookup(struct set *s, void *element);

/** Remove a element.
@param s A pointer to a set.
@param key A pointer to remove.
@return If found 1, otherwise 0.
*/

int set_remove(struct set *s, const void *element);

/** Remove an arbitrary element from the set.
@param s A pointer to a set.
@return The pointer removed.
*/
void *set_pop(struct set *s);

/** Begin iteration over all the elements.
This function begins a new iteration over a set,
allowing you to visit every element in the set.
Next, invoke @ref set_next_element to retrieve each element in order.
@param s A pointer to a set.
*/

void set_first_element(struct set *s);

/** Continue iteration over all elements.
This function returns the next element in the iteration.
@param s A pointer to a set.
@return zero if there are no more elements to visit, the next element otherwise.
*/

void *set_next_element(struct set *s);

#endif
