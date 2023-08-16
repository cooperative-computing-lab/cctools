/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "list.h"

struct list {
	// We keep a refcount on the list itself so we know how many cursors
	// are out there. The list cannot be deleted while it has living cursors.
	unsigned refcount;
	unsigned length;
	struct list_item *head;
	struct list_item *tail;
	struct list_cursor *iter; // global iterator for backwards compatibility
};

struct list_item {
	// Each item also has a refcount. We don't want to free() an item
	// that has cursors on it, so we wait until the item is marked
	// dead AND the refcount hits zero to free() it.
	unsigned refcount;
	struct list *list;
	struct list_item *next;
	struct list_item *prev;
	void *data;
	// drop() just marks an item removed, hiding from all operations
	bool dead;
};

struct list_cursor {
	struct list *list;
	struct list_item *target;
};

static void oom(void) {
	const char *message = "out of memory\n";
	write(STDERR_FILENO, message, strlen(message));
	abort();
}

static void list_ref(struct list *list) {
	if (!list) return;
	assert(list->refcount < UINT_MAX);
	++list->refcount;
}

static void list_unref(struct list *list) {
	if (!list) return;
	assert(list->refcount > 0);
	--list->refcount;
}

static void list_item_ref(struct list_item *item) {
	if (!item) return;
	assert(item->refcount < UINT_MAX);
	++item->refcount;
}

static void list_item_unref(struct list_item *item) {
	if (!item) return;
	assert(item->refcount > 0);
	--item->refcount;
	if (item->dead && item->refcount == 0) {
		struct list_item *left = item->prev;
		struct list_item *right = item->next;
		if (left) {
			left->next = right;
		} else {
			item->list->head = right;
		}
		if (right) {
			right->prev = left;
		} else {
			item->list->tail = left;
		}
		free(item);
	}
}

struct list *list_create(void) {
	struct list *out = calloc(1, sizeof(*out));
	if (!out) oom();
	out->iter = list_cursor_create(out);
	return out;
}

unsigned list_length(struct list *list) {
	assert(list);
	return list->length;
}

bool list_destroy(struct list *list) {
	if (!list) return true;
	if (list->length > 0) return false;
	if (list->refcount > 1) return false;
	list_cursor_destroy(list->iter);
	assert(list->refcount == 0);
	free(list);
	return true;
}

struct list_cursor *list_cursor_create(struct list *list) {
	assert(list);
	struct list_cursor *cur = calloc(1, sizeof(*cur));
	if (!cur) oom();
	cur->list = list;
	list_ref(list);
	return cur;
}

void list_reset(struct list_cursor *cur) {
	assert(cur);
	list_item_unref(cur->target);
	cur->target = NULL;
}

void list_cursor_destroy(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	list_reset(cur);
	list_unref(cur->list);
	free(cur);
}

bool list_get(struct list_cursor *cur, void **item) {
	assert(cur);
	if (!cur->target) return false;
	if (cur->target->dead) return false;
	*item = cur->target->data;
	return true;
}

bool list_set(struct list_cursor *cur, void *item) {
	assert(cur);
	if (!cur->target) return false;
	if (cur->target->dead) return false;
	cur->target->data = item;
	return true;
}

struct list_cursor *list_cursor_clone(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	struct list_cursor *out = list_cursor_create(cur->list);
	out->target = cur->target;
	list_item_ref(out->target);
	return out;
}

bool list_next(struct list_cursor *cur) {
	assert(cur);
	struct list_item *old = cur->target;
	if (!cur->target) return false;
	do {
		cur->target = cur->target->next;
	} while (cur->target && cur->target->dead);
	list_item_ref(cur->target);
	list_item_unref(old);
	return cur->target ? true : false;
}

bool list_prev(struct list_cursor *cur) {
	assert(cur);
	struct list_item *old = cur->target;
	if (!cur->target) return false;
	do {
		cur->target = cur->target->prev;
	} while (cur->target && cur->target->dead);
	list_item_ref(cur->target);
	list_item_unref(old);
	return cur->target ? true : false;
}

bool list_tell(struct list_cursor *cur, unsigned *index) {
	assert(cur);
	assert(cur->list);
	assert(index);
	if (!cur->target) return false;

	unsigned pos = 0;
	for (struct list_item *i = cur->list->head; i != cur->target; i = i->next) {
		assert(i);
		if (i->dead) continue;
		assert(pos < INT_MAX);
		++pos;
	}
	if (cur->target->dead) return false;
	*index = pos;
	return true;
}

bool list_seek(struct list_cursor *cur, int index) {
	assert(cur);
	assert(cur->list);

	if (index < 0) {
		if ((unsigned) abs(index) > cur->list->length) return false;
		list_reset(cur);
		cur->target = cur->list->tail;
		while (cur->target && cur->target->dead) {
			cur->target = cur->target->prev;
		}
		list_item_ref(cur->target);
		while (++index) {
			bool ok = list_prev(cur);
			assert(ok);
		}
	} else {
		if ((unsigned) index >= cur->list->length) return false;
		list_reset(cur);
		cur->target = cur->list->head;
		while (cur->target && cur->target->dead) {
			cur->target = cur->target->next;
		}
		list_item_ref(cur->target);
		while (index--) {
			bool ok = list_next(cur);
			assert(ok);
		}
	}
	return true;
}

bool list_drop(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	if (!cur->target) return false;
	if (cur->target->dead) return true;
	cur->target->dead = true;
	assert(cur->list->length > 0);
	--cur->list->length;
	return true;
}

void list_insert(struct list_cursor *cur, void *item) {
	assert(cur);
	assert(cur->list);

	struct list_item *node = calloc(1, sizeof(*node));
	if (!node) oom();
	node->list = cur->list;
	node->data = item;
	assert(cur->list->length < UINT_MAX);
	++cur->list->length;

	if (cur->target) {
		struct list_item *right = cur->target;
		struct list_item *left = right->prev;
		node->next = right;
		node->prev = left;
		right->prev = node;
		if (left) {
			left->next = node;
		} else {
			cur->list->head = node;
		}
	} else {
		struct list_item *tail = cur->list->tail;
		node->prev = tail;
		cur->list->tail = node;
		if (tail) {
			assert(tail->next == NULL);
			tail->next = node;
		} else {
			assert(!cur->list->head);
			cur->list->head = node;
		}
	}
}

/*
 * The rest of the functions make up the existing list API. They are widely used
 * throughout the codebase. These high-level functions are implemented
 * based on the minimal core above.
 */

int list_size(struct list *list) {
	return (int) list_length(list);
}

struct list *list_splice(struct list *top, struct list *bottom) {
	assert(top);
	assert(bottom);

	void *item;
	bool ok;

	if (top->length == 0) {
		ok = list_destroy(top);
		assert(ok);
		return bottom;
	}

	if (bottom->length == 0) {
		ok = list_destroy(bottom);
		assert(ok);
		return top;
	}

	struct list_cursor *cur_top = list_cursor_create(top);
	struct list_cursor *cur_bot = list_cursor_create(bottom);

	for (list_seek(cur_bot, 0); list_get(cur_bot, &item); list_next(cur_bot)) {
		list_insert(cur_top, item);
		list_drop(cur_bot);
	}

	list_cursor_destroy(cur_bot);
	list_cursor_destroy(cur_top);

	ok = list_destroy(bottom);
	assert(ok);

	list_reset(top->iter);
	return top;
}

struct list *list_split(struct list *l, list_op_t comparator, const void *arg) {
	assert(l);

	void *item;
	struct list *out = NULL;

	if (!arg) return NULL;
	if (l->length < 2) return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (comparator(item, arg)) break;
	}

	while (list_get(cur, &item)) {
		if (!out) out = list_create();
		struct list_cursor *end = list_cursor_create(out);
		list_insert(end, item);
		list_cursor_destroy(end);
		list_drop(cur);
		list_next(cur);
	}

	list_cursor_destroy(cur);
	return out;
}

void list_delete(struct list *l) {
	if (!l)
		return;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, 0);
	do {
		list_drop(cur);
	} while (list_next(cur));
	list_cursor_destroy(cur);

	bool ok = list_destroy(l);
	assert(ok);
}

void list_free(struct list *l)
{
	list_clear(l,free);
}

void list_clear(struct list *l, void (*delete_func)(void *item) )
{
	if(!l) return;

	void *item;

	while((item=list_pop_head(l))) {
		delete_func(item);
	}

}

int list_push_head(struct list *l, void *item) {
	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, 0);
	list_insert(cur, item);
	list_cursor_destroy(cur);
	return 1;
}

int list_push_tail(struct list *l, void *item) {
	struct list_cursor *cur = list_cursor_create(l);
	list_insert(cur, item);
	list_cursor_destroy(cur);
	return 1;
}

void *list_pop_head(struct list *l) {
	void *item = NULL;

	if (!l)
		return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, 0);
	list_get(cur, &item);
	list_drop(cur);
	list_cursor_destroy(cur);

	return item;
}

void *list_pop_tail(struct list *l) {
	void *item = NULL;

	if (!l)
		return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, -1);
	list_get(cur, &item);
	list_drop(cur);
	list_cursor_destroy(cur);

	return item;
}

void *list_rotate(struct list *l)
{
	// If list is empty, return nothing
	if(!l->head) return 0;

	// If list has a single node, return that value.
	if(l->head==l->tail) return l->head->data;

	struct list_item *node = l->head;

	// Change head to next item.
	l->head = node->next;
	l->head->prev = 0;

	// Change node pointers
	node->prev = l->tail;
	node->next = 0;

	// Change tail to point to node
	l->tail->next = node;
	l->tail = node;

	return node->data;
}

void *list_peek_head(struct list *l) {
	void *item = NULL;

	if (!l)
		return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, 0);
	list_get(cur, &item);
	list_cursor_destroy(cur);

	return item;
}

void *list_peek_tail(struct list *l) {
	void *item = NULL;

	if (!l)
		return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, -1);
	list_get(cur, &item);
	list_cursor_destroy(cur);

	return item;
}

void *list_peek_current(struct list *l) {
	void *item = NULL;

	if (!l)
		return NULL;

	list_get(l->iter, &item);
	return item;
}

void *list_remove(struct list *l, const void *value) {
	void *item;
	void *out = NULL;
	bool ok;

	if (!value)
		return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (value == item) {
			out = item;
			ok = list_drop(cur);
			assert(ok);
			break;
		}
	}
	list_cursor_destroy(cur);

	return out;
}

void *list_find(struct list *l, list_op_t comparator, const void *arg) {
	void *item;
	void *out = NULL;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if(comparator(item, arg)) {
			out = item;
			break;
		}
	}
	list_cursor_destroy(cur);

	return out;
}

int list_iterate(struct list *l, list_op_t operator, const void *arg) {
	void *item;
	int alltheway = 1;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (!operator(item, arg)) {
			alltheway = 0;
			break;
		}
	}
	list_cursor_destroy(cur);

	return alltheway;
}

int list_iterate_reverse(struct list *l, list_op_t operator, const void *arg) {
	void *item;
	int alltheway = 1;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, -1); list_get(cur, &item); list_prev(cur)) {
		if (!operator(item, arg)) {
			alltheway = 0;
			break;
		}
	}
	list_cursor_destroy(cur);

	return alltheway;
}

void list_first_item(struct list *list) {
	list_seek(list->iter, 0);
}

void *list_next_item(struct list *list) {
	void *item = NULL;
	list_get(list->iter, &item);
	list_next(list->iter);
	return item;
}

struct list *list_duplicate(struct list *src) {
	void *item;
	struct list *dst = list_create();
	struct list_cursor *src_cur = list_cursor_create(src);
	struct list_cursor *dst_cur = list_cursor_create(dst);

	for (list_seek(src_cur, 0); list_get(src_cur, &item); list_next(src_cur)) {
		list_insert(dst_cur, item);
	}

	list_cursor_destroy(src_cur);
	list_cursor_destroy(dst_cur);
	return dst;
}

struct list *list_sort(struct list *list, int (*comparator) (const void *, const void *)) {
	void **array = NULL;
	int size, i = 0;

	struct list_cursor *cur = list_cursor_create(list);
	if (!list_seek(cur, 0)) goto DONE;

	size = list_size(list);
	array = malloc(size * sizeof(*array));

	while (list_get(cur, &array[i])) {
		list_drop(cur);
		list_next(cur);
		i++;
	}
	qsort(array, size, sizeof(*array), comparator);
	for(i = 0; i < size; i++) {
		list_insert(cur, array[i]);
	}
DONE:
	free(array);
	list_cursor_destroy(cur);
	return list;
}


void list_push_priority(struct list *list, list_priority_t p, void *item) {
	assert(list);
	assert(p);

	void *i = NULL;
	struct list_cursor *cur = list_cursor_create(list);
	for (list_seek(cur, 0); list_get(cur, &i); list_next(cur)) {
			if (p(i) <= p(item)) {
				list_insert(cur, item);
				break;
			}
			i = NULL;
		}
		// if the list is empty or we ran off the end,
		// i is NULL here
		if (!i) list_insert(cur, item);
		list_cursor_destroy(cur);
}

/* vim: set noexpandtab tabstop=8: */
