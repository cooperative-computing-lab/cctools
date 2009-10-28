/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef LIST_H
#define LIST_H

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

struct list * list_create();
struct list * list_splice( struct list *top, struct list *bottom );
void          list_delete( struct list *l );
int	      list_size( struct list *l );

int           list_push_priority( struct list *l, void *item, int prio );

int	      list_push_head( struct list *l, void *item );
void *	      list_pop_head( struct list *l );
void *	      list_peek_head( struct list *l );

int	      list_push_tail( struct list *l, void *item );
void *	      list_pop_tail( struct list *l );
void *	      list_peek_tail( struct list *l );

void *        list_remove( struct list *l, const void *value );
void *        list_find( struct list *l, list_op_t cmp, const void *arg );
int           list_iterate( struct list *l, list_op_t op, const void *arg );
int           list_iterate_reverse( struct list *l, list_op_t op, const void *arg );

void          list_first_item( struct list *list );
void *        list_next_item( struct list *list );

#endif
