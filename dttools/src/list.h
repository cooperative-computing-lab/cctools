/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file list.h Robust, reentrant linked list structure.
 *
 * Aside from list create and delete operations, most functionality
 * is based on cursors on a list. A cursor is a
 * logical position within a list. Due to insertions and deletions,
 * a simple numeric index is not sufficient to define a constant
 * position. Cursors are unaffected by changes in other parts of
 * the list. Lookups, insertions, deletions, etc. all happen at
 * the current location of a cursor. Cursors also support iteration,
 * by moving forward and backward in the list.
 *
 * After creation, a cursor's position is undefined. It could be thought
 * of as sitting at index ∞. Insertions in this state always place
 * the item at the tail of the list, and the cursor's position is
 * unaffected. Calls that examine the value under a cursor fail if the
 * position is undefined.
 *
 * To interact with the contents of a list, a cursor must be placed on
 * a list item by moving forward/backward or by seeking to a specific
 * index. Negative indices are interpreted relative to the tail of the
 * list, so index 0 is the head, and index -1 is the tail.
 *
 * After an item is dropped, it will not be reachable by seeking or moving.
 * If a cursor is on an item that is deleted, it will no longer be able
 * to interact with that item. The cursor can only move off the item.
 * Once all cursors have moved off the item, it is finally free()d.
 */

#ifndef LIST_H
#define LIST_H

#include <limits.h>
#include <stdbool.h>

/*
It turns out that many libraries and tools make use of
symbols like "debug" and "fatal".  This causes strange
failures when we link against such codes.  Rather than change
all of our code, we simply insert these defines to
transparently modify the linker namespace we are using.
*/

#ifndef DOXYGEN

#define list_create			cctools_list_create
#define list_destroy			cctools_list_destroy
#define list_length			cctools_list_length
#define list_cursor_create		cctools_list_cursor_create
#define list_cursor_destroy		cctools_list_cursor_destroy
#define list_cursor_clone		cctools_list_cursor_clone
#define list_reset			cctools_list_reset
#define list_seek			cctools_list_seek
#define list_tell			cctools_list_tell
#define list_next			cctools_list_next
#define list_prev			cctools_list_prev
#define list_get			cctools_list_get
#define list_set			cctools_list_set
#define list_drop			cctools_list_drop
#define list_insert			cctools_list_insert

#define list_size			cctools_list_size
#define list_delete			cctools_list_delete
#define list_free			cctools_list_free
#define list_pop_head			cctools_list_pop_head
#define list_peek_head			cctools_list_peek_head
#define list_pop_tail			cctools_list_pop_tail
#define list_peek_tail			cctools_list_peek_tail
#define list_peek_current		cctools_list_peek_current
#define list_remove			cctools_list_remove
#define list_find			cctools_list_find
#define list_splice			cctools_list_splice
#define list_split			cctools_list_split
#define list_push_head			cctools_list_push_head
#define list_push_tail			cctools_list_push_tail
#define list_push_priority		cctools_list_push_priority
#define list_iterate			cctools_list_iterate
#define list_iterate_reverse		cctools_list_iterate_reverse
#define list_first_item			cctools_list_first_item
#define list_next_item			cctools_list_next_item

#endif

/** Create an empty linked list.
 * @returns A pointer to the newly created list.
 */
struct list *list_create(void);

/** Delete an empty list.
 * The caller is responsible for removing all the
 * items in the list before deleting. If the list is
 * non-empty or there are any living cursors on the list,
 * this call fails and the list is not modified.
 * @param list The list to delete.
 * @returns true if the list was deleted.
 * @returns false if the list is non-empty or there are live cursors.
 */
bool list_destroy(struct list *list);

/** Get the number of items in a list.
 * @param list The list to look at.
 * @returns The number of items in the list.
 */
unsigned list_length(struct list *list);

/** Create a new cursor on a list.
 * The cursor's position is initially undefined.
 * The cursor must be deleted with @ref list_cursor_destroy.
 * @param list The list to use.
 * @returns A pointer to a new cursor.
 */
struct list_cursor *list_cursor_create(struct list *list);

/** Delete a previously created cursor.
 * All cursors must be deleted to avoid
 * leaking memory and references.
 * This does not modify the underlying list.
 * @param cur The cursor to free.
 */
void list_cursor_destroy(struct list_cursor *cur);

/** Get a copy of an existing cursor.
 * The returned cursor is independent from the original, but initially
 * sits at the same position.
 * @param cur The cursor to clone.
 * @returns A new cursor, which must also be deleted.
 */
struct list_cursor *list_cursor_clone(struct list_cursor *cur);

/** Reset the position of a cursor.
 * After calling, the cursor's position will be undefined, just like
 * a newly-created cursor. This function always succeeds.
 * @param cur The cursor to reset.
 */
void list_reset(struct list_cursor *cur);

/** Move a cursor to an item by index.
 * The index specifies the item the cursor will move to.
 * The first item (i.e. the head of the list) is at index zero.
 * Negative indices are taken from the back of the list,
 * so index -1 is the list tail.
 * @param cur The cursor to move.
 * @param index The position of the cursor.
 * @returns true if the cursor moved.
 * @returns false if the index is out of bounds, leaving the cursor unchanged.
 */
bool list_seek(struct list_cursor *cur, int index);

/** Get the position of a cursor within a list.
 * Due to insertions and deletions, an item's index within
 * a list is subject to change. This function walks from the
 * beginning of the list to determine the cursor's current index.
 * If the cursor's position is undefined, or sitting on a dropped item,
 * this function returns false and does not modify the passed in index.
 * @param cur The cursor to check.
 * @param index The location to store the index.
 * @returns true if the cursor's index was written.
 * @returns false if the cursor position is undefined or the item was dropped.
 */
bool list_tell(struct list_cursor *cur, unsigned *index);

/** Move a cursor to the next item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the next item.
 * @returns false if the cursor moved off the end of the list, making the cursor's position undefined.
 */
bool list_next(struct list_cursor *cur);

/** Move a cursor to the previous item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the previous item.
 * @returns false if the cursor moved off the end of the list, making the cursor's position undefined.
 */
bool list_prev(struct list_cursor *cur);

/** Get the item under a cursor.
 * If cursor position is undefined, the passed pointer will not be modified.
 * Note that there are no restrictions on the stored pointer,
 * so it is perfectly possible to receive a null pointer from a list.
 * @param cur The cursor to look at.
 * @param item The location at which to store the value under the cursor.
 * @returns true if the value of the list item was stored.
 * @returns false if the cursor position is undefined.
 */
bool list_get(struct list_cursor *cur, void **item);

/** Set the value under the cursor.
 * If the cursor position is undefined, this function simply returns false.
 * Any pointer value (including NULL) is allowed.
 * @param cur The cursor position to set.
 * @param item The value to set to.
 * @returns true on success.
 * @returns false if the cursor position is undefined.
 */
bool list_set(struct list_cursor *cur, void *item);

/** Remove the item under the cursor.
 * This function is safe to use while iterating over a list,
 * and in the presence of other cursors. Any cursors on the same item
 * will be advanced to the next item.
 * @param cur The cursor to use.
 * @returns true if an item was successfully removed.
 * @returns false if the cursor position is undefined.
 */
bool list_drop(struct list_cursor *cur);

/** Insert an item to the left of the cursor.
 * If the cursor position is undefined, insert at the list tail.
 * There are no restrictions on the pointer value, so inserting
 * a null pointer is perfectly valid. The cursor position is unchanged.
 * @param cur The cursor to use.
 * @param item The pointer to insert.
 */
void list_insert(struct list_cursor *cur, void *item);


// Utility functions

typedef int (*list_op_t) (void *item, const void *arg);
typedef double (*list_priority_t) (void *item);

/** Count the elements in a list.
 * @param list The list to count.
 * @return The number of items stored in the list.
 * */

int list_size(struct list *list);

/** Duplicate a linked list
Returns a copy of the linked list.  Note that the
pointers in both lists point to the same places.
@param list The list to be duplicated
@return A pointer to the duplicate list
*/

struct list *list_duplicate(struct list *list);

/** Delete a linked list.
Note that this function only deletes the list itself,
it does not delete the items referred to by the list.
@param list The list to delete.
*/

void list_delete(struct list *list);

/** Free every item referred to by the list.
Note that this function does not delete the list itself.
@param list The list to free.
*/

void list_free(struct list *list);

/** Delete every item contained within this list,
using the provided function.
Note that this function does not delete the list itself.
@param list The list to free.
@param delete_func The function to delete items with.
*/

void list_clear(struct list *list, void (*delete_func)(void*item) );

/** Splice two lists together.
@param top A linked list that will be destroyed in the process.
@param bottom A linked list that will be destroyed in the process.
@return A new linked list with elements from top at the head and bottom at the tail.
*/

struct list *list_splice(struct list *top, struct list *bottom);

/** Split a list into two at the given item
If arg is NULL or not found, list_split returns NULL and the list is unaffected.
Otherwise src will contain all elements [src->head, arg) and a new list will be created with all elements [arg, src->tail].
@param src The linked list to be split
@param cmp The comparison function.  Should return non-zero on a match.
@param arg The data element to split on.
@return A new linked list with arg as the head and all elements after arg as elements of the new list.
*/

struct list *list_split(struct list *src, list_op_t cmp, const void *arg);

/** Push an item onto the list head.
@param list The list to push onto.
@param item The item to push onto the list.
@return True on success, false on failure (due to out of memory.)
*/
int list_push_head(struct list *list, void *item);

/** Pop an item off of the list head.
@param list The list to pop.
@return The item popped, or null if list is empty.
*/
void *list_pop_head(struct list *list);

/** Peek at the list head.
@param list The list to peek.
@return The item at the list head, or null if list is empty.
*/
void *list_peek_head(struct list *list);

/** Push an item onto the list tail.
@param list The list to push onto.
@param item The item to push onto the list.
@return True on success, false on failure (due to out of memory.)
*/
int list_push_tail(struct list *list, void *item);

/** Pop an item off of the list tail.
@param list The list to pop.
@return The item popped, or null if list is empty.
*/
void *list_pop_tail(struct list *list);

/** Move the list head to the tail
@param list The list to rotate
@return The old list head, new tail
*/
void *list_rotate(struct list *list);

/** Peek at the list tail.
@param list The list to peek.
@return The item at the list tail, or null if list is empty.
*/
void *list_peek_tail(struct list *list);

/** Peek at the current element in the iteration.
@param list The list to peek.
@return The item at the current internal iterator, or null if list is empty.
*/
void *list_peek_current(struct list *list);

/** Push an item onto of a list in priority order.
 * The passed-in function is used to determine the priority of each item.
 * The new item is inserted at the leftmost position such that
 *     p(n) >= p(item) > p(n + 1)
 * in the general position. If a list is built using list_push_priority()
 * with the same priority function, it will always be sorted. Note that each
 * insertion takes O(n) time, where n is the number of list items.
 * See list_sort() for a more efficient way to sort an entire list.
 * @param list The list to modify.
 * @param p The priority function to apply to list items.
 * @param item The new item to insert in priority order.
 */
void list_push_priority(struct list *list, list_priority_t p, void *item);

/** Find an element within a list
This function searches the list, comparing each element in the list to arg, and returns a pointer to the first matching element.
@param list The list to search
@param cmp The comparison function.  Should return non-zero on a match.
@param arg The element to compare against
@return A pointer to the first matched element, or NULL if no elements match.
*/

void *list_find(struct list *list, list_op_t cmp, const void *arg);

/** Remove an item from the list
This function searches the list for the item pointed to by value and removes it.
@param list The list to search
@param value The item to remove
@return The removed item.
*/

void *list_remove(struct list *list, const void *value);

/** Begin traversing a list.
This function sets the internal list iterator to the first item.
Call @ref list_next_item to begin returning the items.
@param list The list to traverse.
*/

void list_first_item(struct list *list);

/** Continue traversing a list.
This function returns the current list item,
and advances the internal iterator to the next item.
@param list The list to traverse.
@return The current item in the list, NULL if end of list.
*/

void *list_next_item(struct list *list);

/** Apply a function to a list.
Invokes op on every member of the list.
@param list The list to operate on.
@param op The operator to apply.
@param arg An optional parameter to send to op.
*/

int list_iterate(struct list *list, list_op_t op, const void *arg);

/** Apply a function to a list in reverse.
Invokes op on every member of the list in reverse.
@param list The list to operate on.
@param op The operator to apply.
@param arg An optional parameter to send to op.
*/
int list_iterate_reverse(struct list *list, list_op_t op, const void *arg);

/** Sort a list using a comparator function
@param list The list to sort.
@param comparator The comparison function used in the sort.  The function should take in pointers to two objects casted as void* and return an integer indicating whether the first is less than (negative), equal to (0), or greater than (positive) the second.
@return A pointer to the list passed in.  Identical to the list parameter.
*/
struct list *list_sort(struct list *list, int (*comparator) (const void *, const void *));

/** Macro to iterate over a list in the most common case.
Note that a statement or code block must follow the macro, like this:

<pre>
char *s;

LIST_ITERATE( list, s ) {
	printf("%s\n",s);
}
</pre>
*/

#define LIST_ITERATE( list, item ) list_first_item(list); while((item=list_next_item(list)))

#endif
