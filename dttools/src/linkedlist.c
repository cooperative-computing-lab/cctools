/*
Copyright (C) 2018- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "linkedlist.h"

struct linkedlist {
	unsigned refcount;
	unsigned length;
	struct linkedlist_item *head;
	struct linkedlist_item *tail;
};

struct linkedlist_item {
	unsigned refcount;
	struct linkedlist *list;
	struct linkedlist_item *next;
	struct linkedlist_item *prev;
	void *data;
};

struct linkedlist_cursor {
	struct linkedlist *list;
	struct linkedlist_item *target;
};

static void oom(void) {
	const char *message = "out of memory\n";
	write(STDERR_FILENO, message, strlen(message));
	abort();
}

static void linkedlist_ref(struct linkedlist *list) {
	if (!list) return;
	assert(list->refcount < UINT_MAX);
	++list->refcount;
}

static void linkedlist_unref(struct linkedlist *list) {
	if (!list) return;
	assert(list->refcount > 0);
	--list->refcount;
}

static void linkedlist_item_ref(struct linkedlist_item *item) {
	if (!item) return;
	assert(item->refcount < UINT_MAX);
	++item->refcount;
}

static void linkedlist_item_unref(struct linkedlist_item *item) {
	if (!item) return;
	assert(item->refcount > 0);
	if (--item->refcount == 0) {
		if (item->prev != item)
			linkedlist_item_unref(item->prev);
		linkedlist_item_unref(item->next);
		free(item);
	}
}

static void linkedlist_cursor_relax(struct linkedlist_cursor *cur) {
	assert(cur);
	if (!cur->target) return;
	if (cur->target->prev != cur->target) return;

	struct linkedlist_item *old = cur->target;
	struct linkedlist_item *tmp = old;
	while (tmp && tmp->prev == tmp) {
		tmp = tmp->next;
	}

	linkedlist_item_ref(tmp);
	linkedlist_item_unref(old);
	cur->target = tmp;
}

struct linkedlist *linkedlist_create(void) {
	struct linkedlist *out = calloc(1, sizeof(*out));
	if (!out) oom();
	return out;
}

unsigned linkedlist_length(struct linkedlist *list) {
	assert(list);
	return list->length;
}

bool linkedlist_delete(struct linkedlist *list) {
	if (!list) return true;
	if (linkedlist_length(list) > 0) return false;
	assert(list->head == NULL);
	assert(list->tail == NULL);
	if (list->refcount > 0) return false;
	free(list);
	return true;
}

struct linkedlist_cursor *linkedlist_cursor_create(struct linkedlist *list) {
	assert(list);
	struct linkedlist_cursor *cur = calloc(1, sizeof(*cur));
	if (!cur) oom();
	cur->list = list;
	linkedlist_ref(list);
	return cur;
}

void linkedlist_cursor_reset(struct linkedlist_cursor *cur) {
	assert(cur);
	linkedlist_cursor_relax(cur);
	linkedlist_item_unref(cur->target);
	cur->target = NULL;
}

void linkedlist_cursor_delete(struct linkedlist_cursor *cur) {
	assert(cur);
	assert(cur->list);
	linkedlist_cursor_reset(cur); // relaxes
	linkedlist_unref(cur->list);
	free(cur);
}

bool linkedlist_get(struct linkedlist_cursor *cur, void **item) {
	assert(cur);
	linkedlist_cursor_relax(cur);
	if (!cur->target) return false;
	*item = cur->target->data;
	return true;
}

bool linkedlist_set(struct linkedlist_cursor *cur, void *item) {
	assert(cur);
	linkedlist_cursor_relax(cur);
	if (!cur->target) return false;
	cur->target->data = item;
	return true;
}

struct linkedlist_cursor *linkedlist_cursor_clone(struct linkedlist_cursor *cur) {
	assert(cur);
	assert(cur->list);
	linkedlist_cursor_relax(cur);
	struct linkedlist_cursor *out = linkedlist_cursor_create(cur->list);
	out->target = cur->target;
	linkedlist_item_ref(out->target);
	return out;
}

bool linkedlist_next(struct linkedlist_cursor *cur) {
	assert(cur);
	linkedlist_cursor_relax(cur);
	struct linkedlist_item *old = cur->target;
	if (!old) return false;
	linkedlist_item_ref(old->next);
	cur->target = old->next;
	linkedlist_item_unref(old);
	return cur->target ? true : false;
}

bool linkedlist_prev(struct linkedlist_cursor *cur) {
	assert(cur);
	linkedlist_cursor_relax(cur);
	struct linkedlist_item *old = cur->target;
	if (!old) return false;
	linkedlist_item_ref(old->prev);
	cur->target = old->prev;
	linkedlist_item_unref(old);
	return cur->target ? true : false;
}

int linkedlist_tell(struct linkedlist_cursor *cur) {
	assert(cur);
	assert(cur->list);
	linkedlist_cursor_relax(cur);
	if (!cur->target) return -1;

	int pos = 0;
	for (struct linkedlist_item *i = cur->list->head; i != cur->target; i = i->next) {
		assert(i);
		assert(pos < INT_MAX);
		++pos;
	}
	return pos;
}

bool linkedlist_seek(struct linkedlist_cursor *cur, int index) {
	assert(cur);
	assert(cur->list);

	if (index < 0) {
		if ((unsigned) abs(index) > linkedlist_length(cur->list)) return false;
		linkedlist_item_unref(cur->target);
		struct linkedlist_item *target = cur->list->tail;
		assert(target);
		while (++index) {
			target = target->prev;
			assert(target);
		}
		linkedlist_item_ref(target);
		cur->target = target;
	} else {
		if ((unsigned) index >= linkedlist_length(cur->list)) return false;
		linkedlist_item_unref(cur->target);
		struct linkedlist_item *target = cur->list->head;
		assert(target);
		while (index--) {
			target = target->next;
			assert(target);
		}
		linkedlist_item_ref(target);
		cur->target = target;
	}
	return true;
}

bool linkedlist_drop(struct linkedlist_cursor *cur) {
	assert(cur);
	assert(cur->list);
	linkedlist_cursor_relax(cur);
	if (!cur->target) return false;
	assert(cur->list->length > 0);
	--cur->list->length;

	struct linkedlist_item *next = cur->target->next;
	struct linkedlist_item *prev = cur->target->prev;

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

	linkedlist_item_unref(cur->target);
	linkedlist_item_unref(cur->target);
	return true;
}

void linkedlist_insert(struct linkedlist_cursor *cur, void *item) {
	assert(cur);
	assert(cur->list);
	linkedlist_cursor_relax(cur);

	struct linkedlist_item *node = calloc(1, sizeof(*node));
	if (!node) oom();
	node->refcount = 2;
	node->list = cur->list;
	node->data = item;
	assert(cur->list->length < UINT_MAX);
	++cur->list->length;

	if (cur->target) {
		struct linkedlist_item *left = cur->target;
		struct linkedlist_item *right = left->next;
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
