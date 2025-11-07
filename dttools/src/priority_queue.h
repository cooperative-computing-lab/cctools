/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

#include <stddef.h>

/** 
@file priority_queue.h
A general purpose priority queue.

This priority queue module is implemented as a complete binary heap that manages elements with associated priorities.
Each element in the priority queue has a priority, and the queue ensures that the element with the highest priority
can be accessed in constant time.

Operation complexity:
- Create: O(n)
- Push: O(log n)
- Pop: O(log n)
- Get head: O(1)
- Get element: O(1)
- Get max priority: O(1)
- Get min priority: O(n)
- Remove element: O(log n)

If all elements have the same priority, the priority queue behaves differently from a standard queue or stack.
For example, a priority has elements with (priority, data) tuples
[(1, "a"), (1, "b"), (1, "c"), (1, "d"), (1, "e")]
When the first time to pop, the last element "e" will take over the top of the heap and the sink operation is applied. The contents are -
[(1, "e"), (1, "b"), (1, "c"), (1, "d")]
Then, the second time to pop, the last element "d" will take over as the top and he contents are -
[(1, "d"), (1, "b"), (1, "c")]
Similarly, the third time to pop -
[(1, "c"), (1, "b")]
The fourth -
[(1, "b")]
As seen, after the first element is popped, the access order is reversed from the input order.

When the elements have different priorities, the index order is not the same as the priority order.
For example, a priority is created with elements with (priority, data) tuples
(1, "a") (2, "b") (3, "c") (4, "d") (5, "e")
The order of elements are determined by @ref priority_queue_push.
The first time to push, the contents are -
[(1, "a")]
The second time to push, the priority of the root is less than the new element, the swim operation is applied to the new element -
[(2, "b"), (1, "a")]
The third time -
[(3, "c"), (1, "a"), (2, "b")]
The fourth time -
[(4, "d"), (3, "c"), (2, "b"), (1, "a")]
The fifth time -
[(5, "e"), (4, "d"), (2, "b"), (1, "a"), (3, "c")]
As seen, the iteration order of elements is not the same as the priority order.

Further, the priority queue can be used to store elements with multiple priorities. 
Each element can have an array of priorities, allowing for multi-level priority ordering.
Priorities are compared lexicographically: priority[0] is compared first; if equal, priority[1] 
is compared; if equal, priority[2] is compared, and so on. This allows for sophisticated 
tie-breaking schemes where higher-indexed priorities serve as secondary criteria.

For example, with 3 priority levels:
- Element A with priorities [5.0, 3.0, 7.0]
- Element B with priorities [5.0, 4.0, 1.0]
The comparison checks priority[0]: both are 5.0 (equal), so it moves to priority[1].
Since 3.0 < 4.0, element B has higher overall priority. Note that priority[2] is never examined.

An example to create a priority queue and manipulate its elements:
<pre>
struct priority_queue *pq;
pq = priority_queue_create(10, 2); // intial space for 10 elements and 2 priorities per element

int priority0 = 5;
int priority1 = 3;
void *data = someDataPointer;

priority_queue_push(pq, data, priority0, priority1);
data = priority_queue_pop(pq);
void *headData = priority_queue_peek_top(pq);
</pre>

To list all of the items in a priority queue, use a simple loop:
<pre>
for (int i = 0; i < priority_queue_size(pq); i++) {
    void *data = priority_queue_peek_at(pq, i);
    printf("Priority queue contains: %p\n", data);
}
</pre>

Or use the PRIORITY_QUEUE_BASE_ITERATE macro:

<pre>
int idx;
void *data;
int iter_count = 0;
int iter_depth = priority_queue_size(q->ready_tasks);
PRIORITY_QUEUE_BASE_ITERATE (pq, idx, data, iter_count, iter_depth) {
    printf("Data idx: %d\n", idx);
}
</pre>
*/


/** Create a new priority queue.
Element with a higher priority is at the top of the heap.
@param init_capacity The initial number of elements in the queue. If zero, a default value will be used.
@param priority_count The number of priorities per element.
@return A pointer to a new priority queue.
*/
struct priority_queue *priority_queue_create(int init_capacity, int priority_count);

/** Count the elements in a priority queue.
@param pq A pointer to a priority queue.
@return The number of elements in the queue.
*/
int priority_queue_size(struct priority_queue *pq);

/** Push an element into a priority queue (core implementation).
Internal function that takes an array of priorities.
@param pq A pointer to a priority queue.
@param data A pointer to store in the queue.
@param priorities Array of priority values.
@param priority_count Number of priorities in the array.
@return The index of data if the push succeeded, -1 on failure.
*/
int priority_queue_push_array(struct priority_queue *pq, void *data, const double *priorities, size_t priority_count);

/** Push an element into a priority queue.
The standard push operation. New elements are placed lower than existing elements of the same priority.
Takes priority values as variable arguments.
@param pq A pointer to a priority queue.
@param data A pointer to store in the queue.
@param ... The priority values (priority_0, priority_1, ...) as doubles.
@return The index of data if the push succeeded, -1 on failure.
*/
#define priority_queue_push(pq, data, ...) priority_queue_push_array( \
	pq, data, (const double[]){ __VA_ARGS__ }, \
	sizeof (double[]){ __VA_ARGS__ } / sizeof (double) \
)

/** Pop the element with the highest priority from a priority queue.
@param pq A pointer to a priority queue.
@return The pointer to the top of the queue if any, failure otherwise.
*/
void *priority_queue_pop(struct priority_queue *pq);

/** Get the element with the highest priority from a priority queue.
Similar to @ref priority_queue_pop, but the element is not removed.
@param pq A pointer to a priority queue.
@return The pointer to the top of the queue if any, failure otherwise
*/
void *priority_queue_peek_top(struct priority_queue *pq);

/** Get an element from a priority queue by a specified index.
The first accessible element is at index 0.
@param pq A pointer to a priority queue.
@param index The index of the element to get.
@return The pointer to the element if any, failure otherwise
*/
void *priority_queue_peek_at(struct priority_queue *pq, int index);

/** Get the priority of an element at a specified index.
@param pq A pointer to a priority queue.
@param priority_idx The index of the priority.
@param element_index The index of the element.
@return The priority of the element if any, NAN on failure.
*/
double priority_queue_get_priority_at(struct priority_queue *pq, int priority_idx, int element_index);

/** Get the priority of the top element in a priority queue.
@param pq A pointer to a priority queue.
@return The priority of the top element if any, NAN on failure.
*/
double priority_queue_get_top_priority(struct priority_queue *pq);

/** Update the priority of an element in a priority queue.
@param pq A pointer to a priority queue.
@param data The pointer to the element to update.
@param priority_idx The index of the priority to update.
@param new_priority The new priority of the element.
@return The new index if the update succeeded, -1 on failure.
*/
int priority_queue_update_priority(struct priority_queue *pq, void *data, int priority_idx, double new_priority);

/** Find the index of an element in a priority queue.
@param pq A pointer to a priority queue.
@param data The pointer to the element to find.
@return The index of the element if found, -1 on failure.
*/
int priority_queue_find_idx(struct priority_queue *pq, void *data);

/** Advance the static_cursor to the next element and return the index.
The static_cursor is used to globally iterate over the elements by sequential index.
The position of the static_cursor is automatically remembered and never reset.
@param pq A pointer to a priority queue.
@return The index of the next element if any, -1 on failure.
*/
int priority_queue_static_next(struct priority_queue *pq);

/** Reset the base_cursor to 0.
The base_cursor is used in PRIORITY_QUEUE_BASE_ITERATE to iterate over the elements from the beginning.
@param pq A pointer to a priority queue.
*/
void priority_queue_base_reset(struct priority_queue *pq);

/** Advance the base_cursor to the next element and return the index.
@param pq A pointer to a priority queue.
@return The index of the next element if any, -1 on failure.
*/
int priority_queue_base_next(struct priority_queue *pq);

/** Reset the rotate_cursor to 0.
The rotate_cursor is used to iterate over the elements from the beginning, and reset on demand.
In task scheduling, we tipically iterate over a amall number of tasks at a time. If there is no task to execute,
we remember the position of the cursor and we can start from there the next time.
If there are interesting events happening, we reset the cursor and start from the beginning.
@param pq A pointer to a priority queue.
*/
void priority_queue_rotate_reset(struct priority_queue *pq);

/** Advance the rotate_cursor to the next element and return the index.
@param pq A pointer to a priority queue.
@return The index of the next element if any, -1 on failure.
*/
int priority_queue_rotate_next(struct priority_queue *pq);

/** Remove the element with the specified index from a priority queue.
@param pq A pointer to a priority queue.
@param idx The index of the element to remove.
@return One if the remove succeeded, failure otherwise
*/
int priority_queue_remove(struct priority_queue *pq, int idx);

/** Delete a priority queue.
@param pq A pointer to a priority queue.
*/
void priority_queue_delete(struct priority_queue *pq);

/** Utility macro to simplify common case of iterating over a priority queue.
Use as follows:

<pre>
int idx;
char *data;

int iter_count = 0;
int iter_depth = priority_queue_size(q->ready_tasks);

PRIORITY_QUEUE_BASE_ITERATE(pq, idx, data, iter_count, iter_depth) {
	printf("Data idx: %d\n", idx);
}

int iter_count = 0;
int iter_depth = 4;
PRIORITY_QUEUE_STATIC_ITERATE( pq, idx, data, iter_count, iter_depth ) {
    printf("Has accessed %d of %d elements\n", iter_count, iter_depth);
    printf("Data idx: %d\n", idx);
}

iter_count = 0;
iter_depth = 7;
PRIORITY_QUEUE_ROTATE_ITERATE( pq, idx, data, iter_count, iter_depth ) {
    printf("Has accessed %d of %d elements\n", iter_count, iter_depth);
    printf("Data idx: %d\n", idx);
}
</pre>
*/

/* Iterate from begining to the end every time starts. */
#define PRIORITY_QUEUE_BASE_ITERATE( pq, idx, data, iter_count, iter_depth ) \
    iter_count = 0; \
    priority_queue_base_reset(pq); \
    while ((iter_count < iter_depth) && ((idx = priority_queue_base_next(pq)) >= 0) && (data = priority_queue_peek_at(pq, idx)) && (iter_count += 1))

/* Iterate from last position, never reset. */
#define PRIORITY_QUEUE_STATIC_ITERATE( pq, idx, data, iter_count, iter_depth ) \
    iter_count = 0; \
    while ((iter_count < iter_depth) && ((idx = priority_queue_static_next(pq)) >= 0) && (data = priority_queue_peek_at(pq, idx)) && (iter_count += 1))

/* Iterate from last position, reset to the begining when needed. */
#define PRIORITY_QUEUE_ROTATE_ITERATE( pq, idx, data, iter_count, iter_depth ) \
    iter_count = 0; \
    while ((iter_count < iter_depth) && ((idx = priority_queue_rotate_next(pq)) >= 0) && (data = priority_queue_peek_at(pq, idx)) && (iter_count += 1))

#endif
