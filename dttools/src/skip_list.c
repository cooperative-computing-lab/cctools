/* Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "skip_list.h"
#include "debug.h"
#include "xxmalloc.h"

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_LEVEL 24

/*
 * This is a priority-based skip list providing O(log n) average time complexity
 * for search, insertion, and deletion, O(n) for seek, and O(1) for next.
 *
 * Priorities are compared lexicographically (left-to-right):
 *   [1.0, 2.0, 3.0] < [1.0, 3.0, 1.0]  (differs at index 1)
 *   [2.0, 1.0, 1.0] > [1.0, 9.0, 9.0]  (differs at index 0)
 *
 * List refcount is incremented by each cursor. Node refcount is incremented
 * when a cursor points to it. Dead nodes are freed when their refcount reaches
 * 0. Always destroy cursors before destroying the list.
 *
 * The implementation uses multiple levels of linked lists. Level 0 contains
 * all elements in priority order. Higher levels contain randomly promoted nodes
 * that act as express lanes for faster traversal. Each node's maximum level is
 * determined probabilistically during insertion - with probability p, a node at
 * level i is promoted to level i+1.
 *
 * Operations traverse from the highest level downward, dropping to lower levels
 * when overshooting the target, achieving O(log n) expected time.
 *
 * See skip_list.h for detailed API documentation.
 */

/* Skip list node - represents an element at one level */
struct skip_list_node {
	void *data;
	double *priority; /* Priority tuple */
	unsigned refcount;
	bool dead;
	int level;			  /* Maximum level this node participates in */
	struct skip_list_node **forward;  /* Array of forward pointers, one per level */
	struct skip_list_node **backward; /* Array of backward pointers, one per level */
};

/* Skip list structure */
struct skip_list {
	unsigned refcount;
	unsigned length;
	unsigned priority_size;
	double probability;
	int level;		     /* Current max level (0-based) */
	struct skip_list_node *head; /* Sentinel node */
	struct skip_list_node *tail; /* Sentinel node */
};

/* Cursor for traversing skip list */
struct skip_list_cursor {
	struct skip_list *list;
	struct skip_list_node *target;
};

static void skip_list_ref(struct skip_list *sl)
{
	if (!sl)
		return;

	++sl->refcount;
}

static void skip_list_unref(struct skip_list *sl)
{
	if (!sl)
		return;

	assert(sl->refcount > 0);
	--sl->refcount;
}

static void skip_list_node_ref(struct skip_list_node *node)
{
	if (!node)
		return;

	++node->refcount;
}

static int delete_node(struct skip_list_node *node, struct skip_list *sl)
{
	assert(node);
	assert(sl);

	if (node->dead && node->refcount == 0) {
		/* Unlink the node from the skip list structure at all levels using backward pointers */
		/* This is O(1) per level thanks to the doubly-linked structure */
		for (int level = node->level; level >= 0; level--) {
			node->backward[level]->forward[level] = node->forward[level];
			node->forward[level]->backward[level] = node->backward[level];
		}

		/* "Delete" the top level if it is empty */
		while (sl->level > 0 && sl->head->forward[sl->level] == sl->tail) {
			sl->level--;
		}

		/* Now free the node */
		free(node->priority);
		free(node->forward);
		free(node->backward);
		free(node);

		return 1;
	}

	return 0;
}

static void skip_list_node_unref(struct skip_list_node *node, struct skip_list *sl)
{
	if (!node || node == sl->head || node == sl->tail) {
		return;
	}

	assert(node->refcount > 0);
	--node->refcount;

	delete_node(node, sl);
}

/* Compare two priority tuples (returns >0 if p1 > p2, 0 if equal, <0 if p1 < p2) */
static int compare_priority(double *p1, double *p2, unsigned size)
{
	for (unsigned i = 0; i < size; i++) {
		if (p1[i] > p2[i])
			return 1;
		if (p1[i] < p2[i])
			return -1;
	}
	return 0;
}

/* Generate random level for a new node based on probability */
static int random_level(struct skip_list *sl)
{
	int level = 0;
	while (level < MAX_LEVEL - 1 && ((double)rand() / RAND_MAX) < sl->probability) {
		level++;
	}
	return level;
}

/* Create a new node with the given level */
static struct skip_list_node *create_node(int level, void *data, double *priority, unsigned priority_size)
{
	struct skip_list_node *node = xxmalloc(sizeof(*node));
	memset(node, 0, sizeof(*node));

	node->forward = xxmalloc((level + 1) * sizeof(struct skip_list_node *));
	node->backward = xxmalloc((level + 1) * sizeof(struct skip_list_node *));
	node->priority = xxmalloc(priority_size * sizeof(double));

	memset(node->forward, 0, (level + 1) * sizeof(struct skip_list_node *));
	memset(node->backward, 0, (level + 1) * sizeof(struct skip_list_node *));
	if (priority) {
		memcpy(node->priority, priority, priority_size * sizeof(double));
	}

	node->data = data;
	node->dead = false;
	node->refcount = 0;
	node->level = level;

	return node;
}

struct skip_list *skip_list_create(unsigned priority_size, double probability)
{
	assert(priority_size > 0);
	assert(probability > 0.0 && probability <= 0.5);

	struct skip_list *sl = xxmalloc(sizeof(*sl));
	memset(sl, 0, sizeof(*sl));

	sl->priority_size = priority_size;
	sl->probability = probability;
	sl->level = 0;
	sl->length = 0;
	sl->refcount = 0;

	sl->head = create_node(MAX_LEVEL, NULL, NULL, priority_size);
	sl->tail = create_node(MAX_LEVEL, NULL, NULL, priority_size);

	/* Link head and tail together at all levels */
	for (int i = 0; i < MAX_LEVEL; i++) {
		sl->head->forward[i] = sl->tail;
		sl->tail->backward[i] = sl->head;
	}

	return sl;
}

int skip_list_length(struct skip_list *sl)
{
	assert(sl);
	return sl->length;
}

bool skip_list_delete(struct skip_list *sl)
{
	if (!sl)
		return true;
	if (sl->length > 0)
		return false;
	if (sl->refcount > 0)
		return false;

	/* Clean up any dead nodes that are still around (with refcount 0) */
	struct skip_list_node *node = sl->head->forward[0];
	while (node != sl->tail) {
		struct skip_list_node *next = node->forward[0];
		assert(node->refcount == 0);
		free(node->priority);
		free(node->forward);
		free(node->backward);
		free(node);
		node = next;
	}

	/* Free the head node */
	if (sl->head) {
		free(sl->head->forward);
		free(sl->head->backward);
		free(sl->head);
	}

	/* Free the tail node */
	if (sl->tail) {
		free(sl->tail->forward);
		free(sl->tail->backward);
		free(sl->tail);
	}

	free(sl);
	return true;
}

struct skip_list_cursor *skip_list_cursor_create(struct skip_list *sl)
{
	assert(sl);
	struct skip_list_cursor *cur = xxmalloc(sizeof(*cur));
	cur->list = sl;
	cur->target = NULL;
	skip_list_ref(sl);

	return cur;
}

void skip_list_cursor_delete(struct skip_list_cursor *cur)
{
	if (!cur)
		return;
	assert(cur->list);
	skip_list_node_unref(cur->target, cur->list);
	skip_list_unref(cur->list);
	free(cur);
}

struct skip_list_cursor *skip_list_cursor_clone(struct skip_list_cursor *cur)
{
	assert(cur);
	assert(cur->list);
	struct skip_list_cursor *out = skip_list_cursor_create(cur->list);
	out->target = cur->target;
	skip_list_node_ref(out->target);
	return out;
}

void skip_list_cursor_move(struct skip_list_cursor *to_move, struct skip_list_cursor *destination)
{
	assert(to_move);
	assert(destination);
	skip_list_node_unref(to_move->target, to_move->list);
	to_move->target = destination->target;
	skip_list_node_ref(to_move->target);
}

bool skip_list_cursor_move_to_priority_arr(struct skip_list_cursor *cur, double *priority)
{
	assert(cur);
	assert(cur->list);
	assert(priority);

	struct skip_list *sl = cur->list;
	struct skip_list_node *x = sl->head;

	/* Traverse from highest level down to find the node with matching priority.
	 * Skip over dead nodes during search for efficiency. */
	for (int i = sl->level; i >= 0; i--) {
		/* Move forward while next node has higher priority (or is dead) */
		while (x->forward[i] != sl->tail &&
				(x->forward[i]->dead ||
						compare_priority(x->forward[i]->priority, priority, sl->priority_size) > 0)) {
			x = x->forward[i];
		}
	}

	/* Move to the next node at level 0 (it should be here anyway). This is our candidate */
	x = x->forward[0];

	/* Skip any dead nodes to find a live one with matching priority */
	while (x != sl->tail && x->dead) {
		x = x->forward[0];
	}

	/* Check if we found a matching live node */
	if (x != sl->tail && compare_priority(x->priority, priority, sl->priority_size) == 0) {
		/* Move cursor to this node with proper reference counting */
		skip_list_node_unref(cur->target, sl);
		cur->target = x;
		skip_list_node_ref(cur->target);
		return true;
	}

	return false;
}

void skip_list_reset(struct skip_list_cursor *cur)
{
	assert(cur);
	skip_list_node_unref(cur->target, cur->list);
	cur->target = NULL;
}

bool seek_forward(struct skip_list_cursor *cur, int index)
{
	assert(index >= 0);
	skip_list_reset(cur);
	struct skip_list *sl = cur->list;

	if (index >= skip_list_length(sl)) {
		return false;
	}

	cur->target = sl->head;
	while (index >= 0) {
		cur->target = cur->target->forward[0];

		if (cur->target == sl->tail) {
			return false;
		}

		// Skip dead nodes without counting them
		// and try to delete them if their refcount is 0
		if (cur->target->dead) {
			delete_node(cur->target, sl);
			continue;
		}

		index--;
	}

	skip_list_node_ref(cur->target);

	return true;
}

bool seek_backward(struct skip_list_cursor *cur, int index)
{
	assert(index < 0);
	skip_list_reset(cur);
	struct skip_list *sl = cur->list;

	if (-index > skip_list_length(sl)) {
		return false;
	}

	cur->target = sl->tail;
	while (index < 0) {
		cur->target = cur->target->backward[0];

		if (cur->target == sl->head) {
			return false;
		}

		// Skip dead nodes without counting them
		// and try to delete them if their refcount is 0
		if (cur->target->dead) {
			delete_node(cur->target, sl);
			continue;
		}

		index++;
	}

	skip_list_node_ref(cur->target);

	return true;
}

bool skip_list_seek(struct skip_list_cursor *cur, int index)
{
	assert(cur);
	assert(cur->list);

	if (index >= 0) {
		return seek_forward(cur, index);
	} else {
		return seek_backward(cur, index);
	}
}

bool skip_list_tell(struct skip_list_cursor *cur, unsigned *index)
{
	assert(cur);
	assert(cur->list);
	assert(index);

	if (!cur->target || cur->target->dead)
		return false;

	unsigned pos = 0;
	struct skip_list_node *node = cur->list->head->forward[0];

	while (node != cur->list->tail && node != cur->target) {
		if (!node->dead)
			pos++;
		node = node->forward[0];
	}

	if (node == cur->list->tail)
		return false;

	*index = pos;
	return true;
}

bool skip_list_next(struct skip_list_cursor *cur)
{
	assert(cur);
	if (!cur->target) {
		return false;
	}

	struct skip_list_node *old = cur->target;

	/* Move to next non-dead node */
	do {
		cur->target = cur->target->forward[0];
	} while (cur->target != cur->list->tail && cur->target->dead);

	if (cur->target == cur->list->tail) {
		cur->target = NULL;
	}

	skip_list_node_ref(cur->target);
	skip_list_node_unref(old, cur->list);

	return cur->target ? true : false;
}

bool skip_list_prev(struct skip_list_cursor *cur)
{
	assert(cur);
	if (!cur->target) {
		return false;
	}

	struct skip_list_node *old = cur->target;

	/* Move to previous non-dead node */
	do {
		cur->target = cur->target->backward[0];
	} while (cur->target != cur->list->head && cur->target->dead);

	if (cur->target == cur->list->head) {
		cur->target = NULL;
	}

	skip_list_node_ref(cur->target);
	skip_list_node_unref(old, cur->list);

	return cur->target ? true : false;
}

const double *skip_list_peek_head_priority(struct skip_list *sl)
{
	assert(sl);

	struct skip_list_cursor *cur = skip_list_cursor_create(sl);
	const double *priority = NULL;

	if (skip_list_seek(cur, 0)) {
		priority = cur->target->priority;
	}

	skip_list_cursor_delete(cur);
	return priority;
}

const void *skip_list_peek_head(struct skip_list *sl)
{
	assert(sl);

	struct skip_list_cursor *cur = skip_list_cursor_create(sl);
	const void *item = NULL;

	if (skip_list_seek(cur, 0)) {
		item = cur->target->data;
	}

	skip_list_cursor_delete(cur);
	return item;
}

void *skip_list_pop_head(struct skip_list *sl)
{
	assert(sl);

	struct skip_list_cursor *cur = skip_list_cursor_create(sl);
	void *item = NULL;

	if (skip_list_seek(cur, 0)) {
		item = cur->target->data;
		skip_list_remove_here(cur);
	}

	skip_list_cursor_delete(cur);
	return item;
}

bool skip_list_get(struct skip_list_cursor *cur, void **item)
{
	assert(cur);
	if (!cur->target)
		return false;
	if (cur->target->dead)
		return false;
	if (item) {
		// if item is not given, then we are simply checking if the cursor is
		// at a valid position
		*item = cur->target->data;
	}
	return true;
}

const double *skip_list_get_priority(struct skip_list_cursor *cur)
{
	assert(cur);
	if (!cur->target) {
		return NULL;
	}

	if (cur->target->dead) {
		return NULL;
	}

	return cur->target->priority;
}

bool skip_list_set(struct skip_list_cursor *cur, void *item)
{
	assert(cur);
	if (!cur->target)
		return false;
	if (cur->target->dead)
		return false;
	cur->target->data = item;
	return true;
}

/* Remove the node under the cursor. */
bool skip_list_remove_here(struct skip_list_cursor *cur)
{
	assert(cur);
	assert(cur->list);

	if (!cur->target)
		return false;
	if (cur->target->dead)
		return true;

	struct skip_list *sl = cur->list;
	struct skip_list_node *target = cur->target;

	/* Mark node as dead */
	target->dead = true;
	assert(sl->length > 0);
	--sl->length;

	/* Note: We don't unlink the node here. We just mark it as dead.
	 * It will be skipped by iteration and search operations.
	 * The node will be freed when its refcount reaches 0. */

	return true;
}

/* Remove the first node found with the given data. */
bool skip_list_remove(struct skip_list *sl, void *data)
{
	assert(sl);

	struct skip_list_cursor *cur = skip_list_cursor_create(sl);

	// Start at the beginning
	if (!skip_list_seek(cur, 0)) {
		skip_list_cursor_delete(cur);
		return false;
	}

	void *item;
	bool ok = false;
	while (skip_list_get(cur, &item)) {
		if (item == data) {
			ok = skip_list_remove_here(cur);

			// move the cursor to the next node so that
			// the reference count is decremented for the current node
			// and possibly deleted by delete_node()
			skip_list_next(cur);
			break;
		}

		if (!skip_list_next(cur)) {
			break;
		}
	}

	skip_list_cursor_delete(cur);
	return ok;
}

bool skip_list_remove_by_priority_arr(struct skip_list *sl, double *priority)
{
	assert(sl);
	assert(priority);

	struct skip_list_node *x = sl->head;

	/* Traverse from highest level down to find the node with matching priority.
	 * Skip over dead nodes during search for efficiency. */
	for (int i = sl->level; i >= 0; i--) {
		/* Move forward while next node has higher priority (or is dead) */
		while (x->forward[i] != sl->tail &&
				(x->forward[i]->dead ||
						compare_priority(x->forward[i]->priority, priority, sl->priority_size) > 0)) {
			x = x->forward[i];
		}
	}

	/* Move to the next node at level 0 (it should be here anyway). This is our candidate */
	x = x->forward[0];

	/* Skip any dead nodes to find a live one with matching priority */
	while (x != sl->tail && x->dead) {
		x = x->forward[0];
	}

	/* Check if we found a matching live node */
	if (x != sl->tail && compare_priority(x->priority, priority, sl->priority_size) == 0) {
		/* Mark node as dead */
		x->dead = true;
		assert(sl->length > 0);
		--sl->length;
		delete_node(x, sl);

		return true;
	}

	return false;
}

void skip_list_insert_arr(struct skip_list *sl, void *item, double *priority)
{
	assert(sl);
	assert(item);
	assert(priority);

	/* Find insertion position */
	struct skip_list_node *update[MAX_LEVEL];
	struct skip_list_node *x = sl->head;

	/* For each level, find the insertion point */
	for (int i = sl->level; i >= 0; i--) {
		while (x->forward[i] != sl->tail &&
				compare_priority(x->forward[i]->priority, priority, sl->priority_size) > 0) {
			x = x->forward[i];
		}
		update[i] = x;
	}

	/* Generate random level for new node. This is the maximum level the new node will be inserted at. */
	int new_level = random_level(sl);

	/* If the new level is greater than the current maximum level of the list,
	we "create" new levels by using the sentinel head node. */
	if (new_level > sl->level) {
		for (int i = sl->level + 1; i <= new_level; i++) {
			update[i] = sl->head;
		}
		sl->level = new_level;
	}

	/* Create new node */
	struct skip_list_node *new_node = create_node(new_level, item, priority, sl->priority_size);

	/* Insert node into skip list and maintain backward pointers */
	for (int i = 0; i <= new_level; i++) {
		new_node->forward[i] = update[i]->forward[i];
		new_node->backward[i] = update[i];

		/* Update backward pointer of next node (or tail) */
		update[i]->forward[i]->backward[i] = new_node;
		update[i]->forward[i] = new_node;
	}

	assert(sl->length < UINT_MAX);
	++sl->length;
}
