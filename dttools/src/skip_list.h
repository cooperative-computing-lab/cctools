/* Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SKIP_LIST_H
#define SKIP_LIST_H

#include <stdint.h>
#include <stdbool.h>

/** @file skip_list.h Skip list structure built on top of list.h
 *
 * A skip list is a probabilistic data structure that allows O(log n)
 * search, insertion, and deletion. This implementation uses multiple
 * levels of linked lists, where each level contains ceil(log(n)) elements
 * from the level below.
 *
 * Items are stored sorted by priority tuples in descending order (highest
 * priority first). Priority tuples are compared lexicographically.
 *
 * <b>Example: Creating and populating a skip list with 2-element priority tuples</b>
 *
 * <pre>
 * struct skip_list *sl = skip_list_create(2, 0.5);  // 2-element priority tuples, 0.5 probability
 * struct skip_list_cursor *cur = skip_list_cursor_create(sl);
 * char *apple = "apple", *banana = "banana", *cherry = "cherry";

 * skip_list_insert(cur, apple, 10.0, 5.0);   // Priority: (10.0, 5.0)
 * skip_list_insert(cur, banana, 20.0, 3.0);  // Priority: (20.0, 3.0) - highest
 * skip_list_insert(cur, cherry, 10.0, 8.0);  // Priority: (10.0, 8.0)

 * skip_list_cursor_destroy(cur);
 * skip_list_destroy(sl);
 * </pre>
 *
 * <b>Example: Iterating through all items (in priority order)</b>
 *
 * <pre>
 * struct skip_list_cursor *cur = skip_list_cursor_create(sl);
 * skip_list_seek(cur, 0);
 *
 * void *item;
 * do {
 *     if (skip_list_get(cur, &item)) {
 *         printf("%s\n", (char *)item);  // Prints: banana, cherry, apple
 *     }
 * } while (skip_list_next(cur));
 *
 * skip_list_cursor_destroy(cur);
 * </pre>
 */

struct skip_list;
struct skip_list_cursor;

/** Create an empty skip list with priority-based sorting.
 * Items are sorted by priority tuples in descending (high to low) order.
 * Priority tuples are compared lexicographically.
 * @param priority_size The number of doubles in each priority tuple.
 * @param probability The probability (0 < p <= 0.5) of promoting an item to the next level (default: 0.5).
 * @returns A pointer to the newly created skip list.
 */
struct skip_list *skip_list_create(unsigned priority_size, double probability);

/** Delete a skip list.
 * The caller is responsible for removing all items before deleting.
 * If the skip list is non-empty or there are living cursors, this call fails.
 * @param sl The skip list to delete.
 * @returns true if the skip list was deleted.
 * @returns false if the skip list is non-empty or there are live cursors.
 */
bool skip_list_destroy(struct skip_list *sl);

/** Get the number of items in a skip list.
 * @param sl The skip list to examine.
 * @returns The number of items in the skip list.
 */
int skip_list_length(struct skip_list *sl);

/** Get the priority tuple of the first item in the skip list.
 * @param sl The skip list to examine.
 * @returns Pointer to the priority array (owned by the struct skip_list), or NULL if list is empty.
 */
const double *skip_list_peek_head_priority(struct skip_list *sl);

/** Get the first item in the skip list without removing it.
 * @param sl The skip list to examine.
 * @returns Pointer to the first item, or NULL if list is empty.
 */
const void *skip_list_peek_head(struct skip_list *sl);

/** Remove and return the first item in the skip list.
 * @param sl The skip list to examine.
 * @returns Pointer to the removed item, or NULL if list is empty.
 */
void *skip_list_pop_head(struct skip_list *sl);

/** Create a cursor for traversing a skip list.
 * @param sl The skip list to create a cursor for.
 * @returns A new cursor positioned at no item (use skip_list_seek to position it).
 */
struct skip_list_cursor *skip_list_cursor_create(struct skip_list *sl);

/** Delete a previously created cursor.
 * @param cur The cursor to free.
 */
void skip_list_cursor_destroy(struct skip_list_cursor *cur);

/** Get a copy of an existing cursor.
 * @param cur The cursor to clone.
 * @returns A new cursor at the same position.
 */
struct skip_list_cursor *skip_list_cursor_clone(struct skip_list_cursor *cur);

/** Move a cursor's position to match another cursor.
 * After this call, to_move will point to the same item as destination.
 * @param to_move The cursor whose position will be updated.
 * @param destination The source cursor whose position will be copied.
 */
void skip_list_cursor_move(struct skip_list_cursor *to_move, struct skip_list_cursor *destination);

/** Reset the position of a cursor.
 * @param cur The cursor to reset.
 */
void skip_list_reset(struct skip_list_cursor *cur);

/** Move a cursor to an item by index.
 * @param cur The cursor to move.
 * @param index The position (negative indices from the tail).
 * @returns true if the cursor moved.
 * @returns false if the index is out of bounds.
 */
bool skip_list_seek(struct skip_list_cursor *cur, int index);

/** Get the position of a cursor within a skip list.
 * @param cur The cursor to check.
 * @param index The location to store the index.
 * @returns true if the cursor's index was written.
 * @returns false if the cursor position is undefined.
 */
bool skip_list_tell(struct skip_list_cursor *cur, unsigned *index);

/** Move a cursor to the next item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the next item.
 * @returns false if the cursor moved off the end.
 */
bool skip_list_next(struct skip_list_cursor *cur);

/** Move a cursor to the previous item.
 * @param cur The cursor to move.
 * @returns true if the cursor moved to the previous item.
 * @returns false if the cursor moved off the beginning.
 */
bool skip_list_prev(struct skip_list_cursor *cur);

/** Move a cursor to the first item with the given priority tuple (internal function).
 * This uses the skip list structure for faster O(log n) lookup.
 * Use the skip_list_cursor_move_to_priority macro instead of calling this directly.
 * @param cur The cursor to move.
 * @param priority Array of doubles representing the priority tuple to find.
 * @returns true if the cursor moved to an item with matching priority.
 * @returns false if no item with that priority was found.
 */
bool skip_list_cursor_move_to_priority_arr(struct skip_list_cursor *cur, double *priority);

/** Move a cursor to the first item with the given priority tuple.
 * This uses the skip list structure for faster O(log n) lookup.
 * Priority values should be passed as doubles matching the priority_size from skip_list_create.
 * @param cur The cursor to move.
 * @param ... Priority values (doubles) matching the priority_size.
 * @returns true if the cursor moved to an item with matching priority.
 * @returns false if no item with that priority was found.
 */
#define skip_list_cursor_move_to_priority(cur, ...) \
	skip_list_cursor_move_to_priority_arr(cur, (double[]){__VA_ARGS__})

/** Get the item under a cursor.
 * @param cur The cursor to look at.
 * @param item The location to store the value.
 * @returns true if the value was stored.
 * @returns false if the cursor position is undefined.
 */
bool skip_list_get(struct skip_list_cursor *cur, void **item);

/** Get the priority tuple of the item under a cursor.
 * @param cur The cursor to look at.
 * @returns Pointer to the priority array (owned by the struct skip_list), or NULL if cursor position is undefined.
 */
const double *skip_list_get_priority(struct skip_list_cursor *cur);

/** Set the value under the cursor.
 * @param cur The cursor position to set.
 * @param item The value to set.
 * @returns true on success.
 * @returns false if the cursor position is undefined.
 */
bool skip_list_set(struct skip_list_cursor *cur, void *item);

/** Remove the item under the cursor.
 * @param cur The cursor to use.
 * @returns true if an item was successfully removed.
 * @returns false if the cursor position is undefined.
 */
bool skip_list_remove_here(struct skip_list_cursor *cur);

/** Remove the first node found with the given data pointer.
 * @param sl The skip list to search.
 * @param data The data pointer to find and remove.
 * @returns true if an item was successfully removed.
 * @returns false if the item was not found.
 */
bool skip_list_remove(struct skip_list *sl, void *data);

/** Remove the first node found with the given priority tuple.
 * This uses the skip list structure for faster O(log n) lookup.
 * @param sl The skip list to search.
 * @param priority Array of doubles representing the priority tuple to find and remove.
 * @returns true if an item was successfully removed.
 * @returns false if no item with that priority was found.
 */
bool skip_list_remove_by_priority_arr(struct skip_list *sl, double *priority);

/** Remove the first node found with the given priority tuple.
 * This uses the skip list structure for faster O(log n) lookup.
 * @param sl The skip list to search.
 * @param ... Priority values (doubles) matching the priority_size.
 * @returns true if an item was successfully removed.
 * @returns false if no item with that priority was found.
 */
#define skip_list_remove_by_priority(sl, ...) \
	skip_list_remove_by_priority_arr(sl, (double[]){__VA_ARGS__})

/** Insert an item with priority into the skip list (internal function).
 * Items are inserted in sorted order by priority (descending).
 * Use the skip_list_insert macro instead of calling this directly.
 * @param lst The skip list to insert into.
 * @param item The pointer to insert.
 * @param priority Array of doubles representing the priority tuple.
 */
void skip_list_insert_arr(struct skip_list *lst, void *item, double *priority);

/** Insert an item with priority into the skip list.
 * Items are automatically inserted in sorted order by priority (high to low).
 * Priority values should be passed as doubles matching the priority_size from skip_list_create.
 *
 * Example for priority_size=2:
 *   skip_list_insert(cur, data, 10.0, 5.0);
 *
 * Example for priority_size=3:
 *   skip_list_insert(cur, data, 10.0, 5.0, 2.5);
 *
 * @param lst The skip list to insert into.
 * @param item The pointer to insert.
 * @param ... Priority values (doubles) matching the priority_size.
 */
#define skip_list_insert(lst, item, ...) \
	skip_list_insert_arr(lst, item, (double[]){__VA_ARGS__})


/** Iterate over all items in a skip list (highest to lowest priority).
 *
 * These macros provide a convenient way to iterate over all items while
 * maintaining a cursor that points to the current item during each iteration.
 *
 * @param cursor A pre-existing cursor (struct skip_list_cursor *) that will be
 *               positioned at each item during iteration. Can be used to modify
 *               or remove the current item. Iteration starts at the position
 *               of that the cursor is currently at.
 * @param item A pointer variable that will receive each item's data.
 *
 * <b>Example usage:</b>
 * <pre>
 * struct skip_list *sl = skip_list_create(1, 0.5);
 * struct skip_list_cursor *cur = skip_list_cursor_create(sl);
 * struct my_data *data;
 *
 * // Insert some items...
 *
 * skip_list_seek(cur, 0);
 * SKIP_LIST_ITERATE_START(cur, data)
 *     printf("Processing: %s\n", data->name);
 *     if (should_remove(data)) {
 *         skip_list_remove_here(cur);
 *     }
 * SKIP_LIST_ITERATE_END(cur)
 *
 * skip_list_cursor_destroy(cur);
 * </pre>
 *
 * @note The cursor is valid during each iteration and can be used with
 *       skip_list_remove_here(), skip_list_get_priority(), etc.
 * @note Safe to use break/continue within the loop body.
 */
#define SKIP_LIST_ITERATE(cursor, item) \
	for(; skip_list_get(cursor, (void **)&item); skip_list_next(cursor))

#endif