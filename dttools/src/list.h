/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LIST_H
#define LIST_H

/** @file list.h Double-linked non-intrusive list. */

/*
It turns out that many libraries and tools make use of
symbols like "debug" and "fatal".  This causes strange
failures when we link against such codes.  Rather than change
all of our code, we simply insert these defines to
transparently modify the linker namespace we are using.
*/

#define list_delete			cctools_list_delete
#define list_pop_head			cctools_list_pop_head 
#define list_peek_head			cctools_list_peek_head
#define list_pop_tail			cctools_list_pop_tail
#define list_peek_tail			cctools_list_peek_tail
#define list_remove			cctools_list_remove
#define list_find			cctools_list_find
#define list_create			cctools_list_create
#define list_splice			cctools_list_splice
#define list_size			cctools_list_size
#define list_push_priority		cctools_list_push_priority
#define list_push_head			cctools_list_push_head
#define list_push_tail			cctools_list_push_tail
#define list_iterate			cctools_list_iterate
#define list_iterate_reverse		cctools_list_iterate_reverse
#define list_first_item			cctools_list_first_item
#define list_next_item			cctools_list_next_item

struct list_node {
	void *data;
	struct list_node *next;
	struct list_node *prev;
	int priority;
};

struct list {
	struct list_node *head;
	struct list_node *tail;
	struct list_node *iter;
	int size;
};

typedef int (*list_op_t) ( void *item, const void *arg );

/** Create a new linked list.
@return A pointer to an empty linked list.
*/

struct list * list_create();

/** Delete a linked list.
Note that this function only deletes the list itself,
it does not delete the items referred to by the list.
@param list The list to delete.
*/

void list_delete( struct list *list );

/** Splice two lists together.
@param top A linked list that will be destroyed in the process.
@param bottom A linked list that will be destroyed in the process.
@return A new linked list with elements from top at the head and bottom at the tail.
*/

struct list * list_splice( struct list *top, struct list *bottom );

/** Count the elements in a list.
@param list The list to count.
@return The number of items stored in the list.
*/

int list_size( struct list *list );

/** Push an item in priority order.
@param list The list to push onto.
@param item The item to push onto the list.
@param prio The integer priority of the item.
@return True on success, false on failure (due to out of memory.)
*/
int list_push_priority( struct list *list, void *item, int prio );

/** Push an item onto the list head.
@param list The list to push onto.
@param item The item to push onto the list.
@return True on success, false on failure (due to out of memory.)
*/
int list_push_head( struct list *list, void *item );

/** Pop an item off of the list head.
@param list The list to pop.
@return The item popped, or null if list is empty.
*/
void * list_pop_head( struct list *list );

/** Peek at the list head.
@param list The list to peek.
@return The item at the list head, or null if list is empty.
*/
void * list_peek_head( struct list *list );

/** Push an item onto the list tail.
@param list The list to push onto.
@param item The item to push onto the list.
@return True on success, false on failure (due to out of memory.)
*/
int list_push_tail( struct list *list, void *item );

/** Pop an item off of the list tail.
@param list The list to pop.
@return The item popped, or null if list is empty.
*/
void * list_pop_tail( struct list *list );

/** Peek at the list tail.
@param list The list to peek.
@return The item at the list tail, or null if list is empty.
*/
void * list_peek_tail( struct list *list );

/** Begin traversing a list.
This function sets the internal list iterator to the first item.
Call @ref list_next_item to begin returning the items.
@param list The list to traverse.
*/

void list_first_item( struct list *list );

/** Continue traversing a list.
This function returns the current list item,
and advances the internal iterator to the next item.
@param list The list to traverse.
@return The current item in the list.
*/

void * list_next_item( struct list *list );

/** Apply a function to a list.
Invokes op on every member of the list.
@param list The list to operate on.
@param op The operator to apply.
@param arg An optional parameter to send to op.
*/

int list_iterate( struct list *list, list_op_t op, const void *arg );

/** Apply a function to a list in reverse.
Invokes op on every member of the list in reverse.
@param list The list to operate on.
@param op The operator to apply.
@param arg An optional parameter to send to op.
*/
int list_iterate_reverse( struct list *list, list_op_t op, const void *arg );

#endif
