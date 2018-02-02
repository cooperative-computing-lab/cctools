/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file linkedlist.h Robust, reentrant linked list structure.
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
 * of as sitting at index -∞. Insertions in this state always place
 * the item at the head of the list, and the cursor's position is
 * unaffected. Calls that examine the value under a cursor fail if the
 * position is undefined.
 *
 * To interact with the contents of a list, a cursor must be placed on
 * a list item by moving forward/backward or by seeking to a specific
 * index. Negative indices are interpreted relative to the tail of the
 * list, so index 0 is the head, and index -1 is the tail.
 *
 * If a cursor is on an item that is deleted, it is automatically shifted
 * on to the next item. If there are no items to the right, or if they
 * are all subsequently deleted, the cursor's position becomes undefined.
 */

#ifndef LINKEDLIST_H
#define LINKEDLIST_H

#include <stdbool.h>


/** Create an empty linked list.
 * @returns A pointer to the newly created list.
 */
struct linkedlist *linkedlist_create(void);

/** Delete an empty list.
 * The caller is responsible for removing all the
 * items in the list before deleting. If the list is
 * non-empty or there are any living cursors on the list,
 * this call fails and the list is not modified.
 * @param list The list to delete.
 * @returns true if the list was deleted.
 * @returns false if the list is non-empty or there are live cursors.
 */
bool linkedlist_delete(struct linkedlist *list);

/** Get the number of items in a list.
 * @param list The list to look at.
 * @returns The number of items in the list.
 */
unsigned linkedlist_size(struct linkedlist *list);

/** Create a new cursor on a list.
 * The cursor's position is initially undefined.
 * The cursor must be deleted with @ref linkedlist_cursor_delete.
 * @param list The list to use.
 * @returns A pointer to a new cursor.
 */
struct linkedlist_cursor *linkedlist_cursor_create(struct linkedlist *list);

/** Delete a previously created cursor.
 * All cursors must be deleted to avoid
 * leaking memory and references.
 * This does not modify the underlying list.
 * @param cur The cursor to free.
 */
void linkedlist_cursor_delete(struct linkedlist_cursor *cur);

/** Get a copy of an existing cursor.
 * The returned cursor is independent from the original, but initially
 * sits at the same position.
 * @param cur The cursor to clone.
 * @returns A new cursor, which must also be deleted.
 */
struct linkedlist_cursor *linkedlist_cursor_clone(struct linkedlist_cursor *cur);

/** Reset the position of a cursor.
 * After calling, the cursor's position will be undefined, just like
 * a newly-created cursor. This function always succeeds.
 * @param cur The cursor to reset.
 */
void linkedlist_reset(struct linkedlist_cursor *cur);

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
bool linkedlist_seek(struct linkedlist_cursor *cur, int index);

/** Get the position of a cursor within a list.
 * Due to insertions and deletions, an item's index within
 * a list is subject to change. This function walks from the
 * beginning of the list to determine the cursor's current index.
 * @param cur The cursor to check.
 * @returns a non-negative number if the cursor is on a list item.
 * @returns -1 if the cursor's position is undefined.
 */
int linkedlist_tell(struct linkedlist_cursor *cur);

/** Move a cursor to the next item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the next item.
 * @returns false if the cursor moved off the end of the list, making the cursor's position undefined.
 */
bool linkedlist_next(struct linkedlist_cursor *cur);

/** Move a cursor to the previous item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the previous item.
 * @returns false if the cursor moved off the end of the list, making the cursor's position undefined.
 */
bool linkedlist_prev(struct linkedlist_cursor *cur);

/** Get the item under a cursor.
 * If cursor position is undefined, the passed pointer will not be modified.
 * Note that there are no restrictions on the stored pointer,
 * so it is perfectly possible to receive a null pointer from a list.
 * @param cur The cursor to look at.
 * @param item The location at which to store the value under the cursor.
 * @returns true if the value of the list item was stored.
 * @returns false if the cursor position is undefined.
 */
bool linkedlist_get(struct linkedlist_cursor *cur, void **item);

/** Set the value under the cursor.
 * If the cursor position is undefined, this function simply returns false.
 * Any pointer value (including NULL) is allowed.
 * @param cur The cursor position to set.
 * @param item The value to set to.
 * @returns true on success.
 * @returns false if the cursor position is undefined.
 */
bool linkedlist_set(struct linkedlist_cursor *cur, void *item);

/** Remove the item under the cursor.
 * This function is safe to use while iterating over a list,
 * and in the presence of other cursors. Any cursors on the same item
 * will be advanced to the next item.
 * @param cur The cursor to use.
 * @returns true if an item was successfully removed.
 * @returns false if the cursor position is undefined.
 */
bool linkedlist_drop(struct linkedlist_cursor *cur);

/** Insert an item to the right of the cursor.
 * If the cursor position is undefined, insert at the list head.
 * There are no restrictions on the pointer value, so inserting
 * a null pointer is perfectly valid. The cursor position is unchanged.
 * @param cur The cursor to use.
 * @param item The pointer to insert.
 */
void linkedlist_insert(struct linkedlist_cursor *cur, void *item);

#endif
