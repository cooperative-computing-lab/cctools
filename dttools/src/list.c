/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "list.h"

struct list {
	unsigned refcount;
	unsigned length;
	struct list_item *head;
	struct list_item *tail;
	struct list_cursor *iter;
};

struct list_item {
	unsigned refcount;
	double priority;
	struct list *list;
	struct list_item *next;
	struct list_item *prev;
	void *data;
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
	if (--item->refcount == 0) {
		if (item->prev != item)
			list_item_unref(item->prev);
		list_item_unref(item->next);
		free(item);
	}
}

static void list_cursor_relax(struct list_cursor *cur) {
	assert(cur);
	if (!list_cursor_moved(cur)) return;

	struct list_item *old = cur->target;
	struct list_item *tmp = old;
	while (tmp && tmp->prev == tmp) {
		tmp = tmp->next;
	}

	list_item_ref(tmp);
	list_item_unref(old);
	cur->target = tmp;
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
	if (list_size(list) > 0) return false;
	assert(list->head == NULL);
	assert(list->tail == NULL);
	if (list->refcount > 1) return false;
	list_cursor_destroy(list->iter);
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

void list_cursor_reset(struct list_cursor *cur) {
	assert(cur);
	list_cursor_relax(cur);
	list_item_unref(cur->target);
	cur->target = NULL;
}

void list_cursor_destroy(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	list_cursor_reset(cur); // relaxes
	list_unref(cur->list);
	free(cur);
}

bool list_cursor_moved(struct list_cursor *cur) {
	assert(cur);
	if (!cur->target) return false;
	if (cur->target->prev == cur->target) return true;
	return false;
}

bool list_get(struct list_cursor *cur, void **item) {
	assert(cur);
	list_cursor_relax(cur);
	if (!cur->target) return false;
	*item = cur->target->data;
	return true;
}

bool list_set(struct list_cursor *cur, void *item) {
	assert(cur);
	list_cursor_relax(cur);
	if (!cur->target) return false;
	cur->target->data = item;
	return true;
}

double list_get_priority(struct list_cursor *cur) {
	assert(cur);
	list_cursor_relax(cur);
	if (!cur->target) return NAN;
	return cur->target->priority;
}

bool list_set_priority(struct list_cursor *cur, double priority) {
	assert(cur);
	list_cursor_relax(cur);
	if (!cur->target) return false;
	cur->target->priority = priority;
	return true;
}

struct list_cursor *list_cursor_clone(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	list_cursor_relax(cur);
	struct list_cursor *out = list_cursor_create(cur->list);
	out->target = cur->target;
	list_item_ref(out->target);
	return out;
}

bool list_next(struct list_cursor *cur) {
	assert(cur);
	list_cursor_relax(cur);
	struct list_item *old = cur->target;
	if (!old) return false;
	list_item_ref(old->next);
	cur->target = old->next;
	list_item_unref(old);
	return cur->target ? true : false;
}

bool list_prev(struct list_cursor *cur) {
	assert(cur);
	list_cursor_relax(cur);
	struct list_item *old = cur->target;
	if (!old) return false;
	list_item_ref(old->prev);
	cur->target = old->prev;
	list_item_unref(old);
	return cur->target ? true : false;
}

int list_tell(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	list_cursor_relax(cur);
	if (!cur->target) return -1;

	int pos = 0;
	for (struct list_item *i = cur->list->head; i != cur->target; i = i->next) {
		assert(i);
		assert(pos < INT_MAX);
		++pos;
	}
	return pos;
}

bool list_seek(struct list_cursor *cur, int index) {
	assert(cur);
	assert(cur->list);

	if (index < 0) {
		if (abs(index) > list_size(cur->list)) return false;
		list_item_unref(cur->target);
		struct list_item *target = cur->list->tail;
		assert(target);
		while (++index) {
			target = target->prev;
			assert(target);
		}
		list_item_ref(target);
		cur->target = target;
	} else {
		if (index >= list_size(cur->list)) return false;
		list_item_unref(cur->target);
		struct list_item *target = cur->list->head;
		assert(target);
		while (index--) {
			target = target->next;
			assert(target);
		}
		list_item_ref(target);
		cur->target = target;
	}
	return true;
}

bool list_drop(struct list_cursor *cur) {
	assert(cur);
	assert(cur->list);
	list_cursor_relax(cur);
	if (!cur->target) return false;
	assert(cur->list->length > 0);
	--cur->list->length;

	struct list_item *next = cur->target->next;
	struct list_item *prev = cur->target->prev;

	if (next) {
		next->prev = prev;
	} else {
		cur->list->tail = prev;
	}
	if (prev) {
		prev->next = next;
	} else {
		cur->list->head = next;
	}

	cur->target->prev = cur->target;
	list_item_ref(cur->target->next);
	list_item_unref(cur->target);
	list_item_unref(cur->target);
	return true;
}

void list_insert(struct list_cursor *cur, void *item) {
	assert(cur);
	assert(cur->list);
	list_cursor_relax(cur);

	struct list_item *node = calloc(1, sizeof(*node));
	if (!node) oom();
	node->refcount = 2;
	node->list = cur->list;
	node->data = item;
	assert(cur->list->length < UINT_MAX);
	++cur->list->length;

	if (cur->target) {
		struct list_item *left = cur->target;
		struct list_item *right = left->next;
		node->next = right;
		node->prev = left;
		left->next = node;
		if (right) {
			right->prev = node;
		} else {
			cur->list->tail = node;
		}
	} else {
		node->next = cur->list->head;
		if (node->next) {
			assert(node->next->prev == NULL);
			node->next->prev = node;
		} else {
			cur->list->tail = node;
		}
		cur->list->head = node;
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

	struct list_cursor *cur_top = list_cursor_create(top);
	struct list_cursor *cur_bot = list_cursor_create(bottom);

	for (list_seek(cur_bot, 0); list_get(cur_bot, &item); list_next(cur_bot)) {
		list_seek(cur_top, -1);
		list_insert(cur_top, item);
	}

	list_cursor_destroy(cur_top);
	list_cursor_destroy(cur_bot);

	list_delete(bottom);

	return top;
}

struct list *list_split(struct list *l, list_op_t comparator, const void *arg) {
	assert(l);

	void *item;
	struct list *out = NULL;

	if (!arg) return NULL;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		if (comparator(item, arg)) break;
	}

	while (list_get(cur, &item)) {
		if (!out) out = list_create();
		struct list_cursor *end = list_cursor_create(out);
		list_seek(end, -1);
		list_insert(end, item);
		list_drop(cur);
		list_cursor_destroy(end);
	}

	return out;
}

void list_delete(struct list *l) {
	if (!l)
		return;

	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, 0);
	while (list_drop(cur)) continue;
	list_cursor_destroy(cur);

	bool ok = list_destroy(l);
	assert(ok);
}

void list_free(struct list *l) {
	void *item;

	if (!l)
		return;

	struct list_cursor *cur = list_cursor_create(l);
	for (list_seek(cur, 0); list_get(cur, &item); list_next(cur)) {
		free(item);
	}
	list_cursor_destroy(cur);
}

int list_push_priority(struct list *l, void *item, double priority) {
	bool ok;
	struct list_cursor *cur = list_cursor_create(l);

	if (list_size(l) == 0) {
		list_insert(cur, item);
		ok = list_seek(cur, 0);
		assert(ok);
	} else {
		ok = list_seek(cur, -1);
		assert(ok);
		do {
			double p = list_get_priority(cur);
			if (priority > p) break;
		} while (list_prev(cur));
		list_insert(cur, item);
		ok = list_next(cur);
		if (!ok) {
			ok = list_seek(cur, 0);
			assert(ok);
		}
	}

	ok = list_set_priority(cur, priority);
	assert(ok);

	list_cursor_destroy(cur);
	return 1;
}

int list_push_head(struct list *l, void *item) {
	struct list_cursor *cur = list_cursor_create(l);
	list_insert(cur, item);
	list_cursor_destroy(cur);

	return 1;
}

int list_push_tail(struct list *l, void *item) {
	struct list_cursor *cur = list_cursor_create(l);
	list_seek(cur, -1);
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

	for (list_seek(src_cur, -1); list_get(src_cur, &item); list_prev(src_cur)) {
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
		i++;
	}
	qsort(array, size, sizeof(*array), comparator);
	for(i = size - 1; i >= 0; i--) {
		list_insert(cur, array[i]);
	}
DONE:
	free(array);
	list_cursor_destroy(cur);
	return list;
}

/* vim: set noexpandtab tabstop=4: */
