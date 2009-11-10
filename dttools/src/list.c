/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "list.h"

#include <stdlib.h>

struct list * list_create()
{
	struct list *l;

	l = malloc(sizeof(struct list));
	if(!l) return 0;

	l->head=0;
	l->tail=0;
	l->size=0;
	l->iter=0;

	return l;
}

struct list * list_splice( struct list *top, struct list *bottom )
{
	if( !top->head ) {
		list_delete( top );
		return bottom;
	}

	if( !bottom->head ) {
		list_delete( bottom );
		return top;
	}

	top->tail->next = bottom->head;
	top->tail = bottom->tail;
	
	bottom->head->prev = top->tail;
	bottom->head = 0;
	bottom->tail = 0;

	top->size += bottom->size;
	top->iter = 0;

	list_delete( bottom );
	return top;	
}

void list_delete( struct list *l )
{
	struct list_node *n,*m;

	if(!l) return;

	for( n=l->head; n; n=m ) {
		m=n->next;
		free(n);
	}

	free(l);
}

int list_size( struct list *l )
{
	return l->size;
}

static struct list_node * new_node( void *data, struct list_node *prev, struct list_node *next )
{
	struct list_node *node;

	node = malloc(sizeof(struct list_node));
	node->data = data;
	node->next = next;
	node->prev = prev;
	node->priority = 0;

	if(next) { next->prev=node; }
	if(prev) { prev->next=node; }

	return node;
}

int list_push_priority( struct list *l, void *item, int priority )
{
	struct list_node *n;
	struct list_node *node;

	if(!l->head) return list_push_head(l,item);
	if(l->head->priority<priority) return list_push_head(l,item);

	for(n=l->head;n;n=n->next) {
		if(n->priority<priority) {
			node = new_node(item,n->prev,n);
			node->priority = priority;
			return 1;
		}
	}
	
	return list_push_tail(l,item);
}

int list_push_head( struct list *l, void *item )
{
	struct list_node *node;

	node = new_node(item,0,l->head);
	if(!node) return 0;
	l->head = node;
	if(!l->tail) l->tail = node;
	l->size++;

	return 1;
}

int list_push_tail( struct list *l, void *item )
{
	struct list_node *node;

	node = new_node(item,l->tail,0);
	if(!node) return 0;
	l->tail = node;
	if(!l->head) l->head = node;
	l->size++;


	return 1;
}

void * list_pop_head( struct list *l )
{
	struct list_node *node;
	void *item;

	if(!l->head) return 0;

	node = l->head;
	l->head = l->head->next;
	if(l->head) {
		l->head->prev=0;
	} else {
		l->tail=0;
	}
	item = node->data;
	free(node);
	l->size--;

	return item;
}

void * list_pop_tail( struct list *l )
{
	struct list_node *node;
	void *item;

	if(!l->tail) return 0;

	node = l->tail;
	l->tail = l->tail->prev;
	if(l->tail) {
		l->tail->next=0;
	} else {
		l->head=0;
	}
	item = node->data;
	free(node);
	l->size--;

	return item;
}

void * list_peek_head( struct list *l )
{
	if(l->head) {
		return l->head->data;
	} else {
		return 0;
	}
}

void * list_peek_tail( struct list *l )
{
	if(l->tail) {
		return l->tail->data;
	} else {
		return 0;
	}
}

void * list_remove( struct list *l, const void *value )
{
	struct list_node *n;
	void *data;

	for(n=l->head;n;n=n->next) {
		if( value==n->data ) {
			data = n->data;
			if( n->next ) n->next->prev = n->prev;
			if( n->prev ) n->prev->next = n->next;
			if( n==l->head ) l->head = n->next;
			if( n==l->tail ) l->tail = n->prev;
			free(n);
			l->size--;
			return data;
		}
	}

	return 0;
}

void * list_find( struct list *l, list_op_t comparator, const void *arg )
{
	struct list_node *n;

	for(n=l->head;n;n=n->next) {
		if(comparator(n->data,arg)) return n->data;
	}

	return 0;
}

int list_iterate( struct list *l, list_op_t operator, const void *arg )
{
	struct list_node *n;
	int alltheway=1;

	for(n=l->head;n;n=n->next) {
		if(!operator(n->data,arg)) {
			alltheway = 0;
			break;
		}
	}

	return alltheway;
}

int list_iterate_reverse( struct list *l, list_op_t operator, const void *arg )
{
	struct list_node *n;
	int alltheway=1;

	for(n=l->tail;n;n=n->prev) {
		if(!operator(n->data,arg)) {
			alltheway = 0;
			break;
		}
	}

	return alltheway;
}

void list_first_item( struct list *list )
{
	list->iter = list->head;
}

void *list_next_item( struct list *list )
{
	if(list->iter) {
		void *v = list->iter->data;
		list->iter = list->iter->next;
		return v;
	} else {
		return 0;
	}
}
