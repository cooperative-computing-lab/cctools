/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef STRING_SET_H
#define STRING_SET_H

#include "int_sizes.h"
#include "hash_table.h"

/** @file string_set.h A string_set data structure.
Strings that are equal (based on hash and strcmp) appear only once
in the set.  For example, as a set of filenames:
<pre>
struct string_set *s;
s = string_set_create(0);

string_set_push(s,"FOO");
string_set_push(s,"BAR");
string_set_push(s,"FOO");

assert(string_set_size(s) == 2);

path = string_set_pop(s);

assert(string_set_size(s) == 1);
</pre>

To list all of the elements in a string_set, use @ref string_set_first_element and @ref string_set_next_element like this:

<pre>
char *element;

string_set_first_element(s);
while(string_set_next_element(s, &element)) {
	printf("string_set contains: %s\n", element);
}
</pre>

*/

/** Create a new string_set.
@param buckets The number of elements in the string_set.  If zero, a default element will be used. Increases dynamically as needed.
@param func The default hash function to be used.  If zero, @ref hash_string will be used.
@return A pointer to a new string_set.
*/

struct string_set *string_set_create(int buckets, hash_func_t func);

/** Duplicate a string_set from an existing string_set.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s The string_set to be duplicated.
@return A pointer to a new string_set.
*/

struct string_set *string_set_duplicate(struct string_set *s);

/** Unions two string_sets into one string_set. Could also be called Merge.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s1 A pointer to the first string_set to be unioned.
@param s2 A pointer to the second string_set to be unioned.
@return A pointer to a new string_set.
*/

struct string_set *string_set_union(struct string_set *s1, struct string_set *s2);

/** Remove all entries from a string_set.
Note that this function will not free all of the objects contained within the string_set.
@param s A pointer to a string_set.
*/

void string_set_clear(struct string_set *s);

/** Delete a string_set.
Note that this function will not free all of the objects contained within the string_set.
@param s A pointer to a string_set.
*/

void string_set_delete(struct string_set *s);

/** Count the entries in a string_set.
@return The number of entries in the string_set.
@param s A pointer to a string_set.
*/

int string_set_size(struct string_set *s);

/** Insert an element to the string_set.
This call will return 0 if element was already in the string_set.
You must call @ref string_set_remove to remove it.
Also note that you cannot insert a null element into the string_set.
@param s A pointer to a string_set.
@param element A pointer to store.
@return One if the insert succeeded, 0 otherwise.
*/

int string_set_insert(struct string_set *s, const char *element);

/** Insert an existing string_set into the string_set.
This call will return 1 if all elements of s2 exist or are added to the string_set.
Also note that you cannot insert a null string_set into the string_set.
NOTE: This does not duplicated the element pointers, beware of double frees.
@param s A pointer to a string_set.
@param s2 A pointer to a string_set to be inserted.
@return Number of items added to string_set.
*/

int string_set_insert_string_set(struct string_set *s, struct string_set *s2);

/** Insert an element to the string_set.
This is equivalent to string_set_insert
*/

int string_set_push(struct string_set *h, const char *element);

/** Look up a element in the string_set.
@param s A pointer to a string_set.
@param element A pointer to search for.
@return If found, 1, otherwise 0.
*/

int string_set_lookup(struct string_set *s, const char *element);

/** Remove an element.
@param s A pointer to a string_set.
@param element A pointer to remove.
@return If found 1, otherwise 0.
*/

int string_set_remove(struct string_set *s, const char *element);

/** Remove an arbitrary element from the string_set.
@param s A pointer to a string_set.
@return The pointer removed.
*/
void *string_set_pop(struct string_set *s);

/** Begin iteration over all the elements.
This function begins a new iteration over a string_set,
allowing you to visit every element in the string_set.
Next, invoke @ref string_set_next_element to retrieve each element in order.
@param s A pointer to a string_set.
*/

void string_set_first_element(struct string_set *s);

/** Continue iteration over all elements.
This function returns the next element in the iteration.
@param s A pointer to a string_set.
@param element A char pointer where the result will be set.
@return zero if there are no more elements to visit, and one otherwise.
*/

int string_set_next_element(struct string_set *s, char **element);

#endif
