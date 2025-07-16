/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SET_H
#define SET_H

#include "int_sizes.h"
#include "list.h"

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

#define SET_ITERATE( set, element ) set_first_element(set); while((element = set_next_element(set)))

#define SET_ITERATE_RANDOM_START( set, offset_bookkeep, element ) set_random_element(set, &offset_bookkeep); while((element = set_next_element_with_offset(set, offset_bookkeep)))

/** Create a new set.
@param buckets The number of elements in the set.  If zero, a default element will be used. Increases dynamically as needed.
@return A pointer to a new set.
*/

struct set *set_create(int buckets);

/** Duplicate a set from an existing set.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s The set to be duplicated.
@return A pointer to a new set.
*/

struct set *set_duplicate(struct set *s);

/** Unions two sets into one set. Could also be called Merge.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s1 A pointer to the first set to be unioned.
@param s2 A pointer to the second set to be unioned.
@return A pointer to a new set.
*/

struct set *set_union(struct set *s1, struct set *s2);

/** Remove all entries from a set.
Note that this function will not free all of the objects contained within the set.
@param s A pointer to a set.
*/

void set_clear(struct set *s);

/** Delete a set.
Note that this function will not free all of the objects contained within the set.
@param s A pointer to a set.
*/

void set_delete(struct set *s);

/** Count the entries in a set.
@return The number of entries in the set.
@param s A pointer to a set.
*/

int set_size(struct set *s);

/** Insert an element to the set.
This call will return 0 if element was already in the set.
You must call @ref set_remove to remove it.
Also note that you cannot insert a null element into the set.
@param s A pointer to a set.
@param element A pointer to store.
@return One if the insert succeeded, 0 otherwise.
*/

int set_insert(struct set *s, const void *element);

/** Insert an existing set into the set.
This call will return 1 if all elements of s2 exist or are added to the set.
Also note that you cannot insert a null set into the set.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s A pointer to a set.
@param s2 A pointer to a set to be inserted.
@return Number of items added to set.
*/

int set_insert_set(struct set *s, struct set *s2);

/** Insert an existing list into the set.
This call will return 1 if all elements of list exist or are added to the set.
Also note that you cannot insert a null list into the set.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s A pointer to a set.
@param l A pointer to a list to be inserted.
@return Number of items added to set.
*/

int set_insert_list(struct set *s, struct list *l);

/** Insert an element to the set.
This is equivalent to set_insert
*/

int set_push(struct set *h, const void *element);

/** Look up a element in the set.
@param s A pointer to a set.
@param element A pointer to search for.
@return If found, 1, otherwise 0.
*/

int set_lookup(struct set *s, void *element);

/** Remove an element.
@param s A pointer to a set.
@param element A pointer to remove.
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

/** Begin iteration over all elements from a random offset.
This function begins a new iteration over a set,
allowing you to visit every element in the set.
Next, invoke @ref set_next_element_with_offset to retrieve each value in order.
@param s A pointer to a set.
@param offset_bookkeep An integer to pointer where the origin to the iteration is recorded.
*/

void set_random_element(struct set *s, int *offset_bookkeep);

/** Continue iteration over all elements from an arbitray offset.
This function returns the next element in the iteration.
@param s A pointer to a set.
@param offset_bookkeep The origin for this iteration. See @ref set_random_element
*/

void *set_next_element_with_offset(struct set *s, int offset_bookkeep);

/** A set_size(s) array of the current elements in the set in a random order.
Caller should free the array.
@param s A pointer to a set.
@return An array of pointers. NULL if there are no elements in the set.
*/
void **set_values(struct set *s);

/** A set_size(s) + 1 array of the current elements in the set in a random order.
The array is NULL-terminated. Caller should free the array with set_free_values_array.
@param s A pointer to a set.
@return An array of pointers terminated with NULL. NULL if there are no elements in the set.
*/
void **set_values_array(struct set *s);

/** Free an array returned by set_values_array.
@param values An array returned by set_values_array.
*/
void set_free_values_array(void **values);

#endif
