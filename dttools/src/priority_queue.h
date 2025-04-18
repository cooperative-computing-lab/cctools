/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

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

An example to create a priority queue and manipulate its elements:
<pre>
struct priority_queue *pq;
pq = priority_queue_create(10);

int priority = 5;
void *data = someDataPointer;

priority_queue_push(pq, data, priority);
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

*/

/** Create a new priority queue.
Element with a higher priority is at the top of the heap.
@param init_capacity The initial number of elements in the queue. If zero, a default value will be used.
@return A pointer to a new priority queue.
*/
struct priority_queue *priority_queue_create(int init_capacity);

/** Count the elements in a priority queue.
@param pq A pointer to a priority queue.
@return The number of elements in the queue.
*/
int priority_queue_size(struct priority_queue *pq);

/** Push an element into a priority queue.
The standard push operation. New elements are placed lower than existing elements of the same priority
@param pq A pointer to a priority queue.
@param data A pointer to store in the queue.
@param priority The specified priority with the given object.
@return The idex of data if the push succeeded, -1 on failure.
*/
int priority_queue_push(struct priority_queue *pq, void *data, double priority);

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
@param index The index of the element.
@return The priority of the element if any, NAN on failure.
*/
double priority_queue_get_priority_at(struct priority_queue *pq, int index);

/** Update the priority of an element in a priority queue.
@param pq A pointer to a priority queue.
@param idx The index of the element to update.
@param new_priority The new priority of the element.
@return The new index if the update succeeded, -1 on failure.
*/
int priority_queue_update_priority_at(struct priority_queue *pq, int idx, double new_priority);

/** Find the index of an element in a priority queue.
@param pq A pointer to a priority queue.
@param data The pointer to the element to find.
@return The index of the element if found, -1 on failure.
*/
int priority_queue_find_idx(struct priority_queue *pq, void *data);

/** Remove the element with the specified index from a priority queue.
@param pq A pointer to a priority queue.
@param idx The index of the element to remove.
@return One if the remove succeeded, failure otherwise
*/
int priority_queue_remove_at(struct priority_queue *pq, int idx);

/** Delete a priority queue.
@param pq A pointer to a priority queue.
*/
void priority_queue_delete(struct priority_queue *pq);

/** Duplicate a priority queue.
@param src A pointer to a priority queue.
@return A pointer to a new priority queue.
*/
struct priority_queue *priority_queue_duplicate(struct priority_queue *src);

/*
 * PRIORITY_QUEUE_SORTED_ITERATE(pq, idx, data, iter_count, iter_depth)
 *
 * Iterate over the priority queue in descending priority order.
 * Internally uses a deep copy of the heap and pops elements one by one.
 * For each popped element `data`, its corresponding `idx` in the original
 * queue is determined via priority_queue_find_idx().
 *
 * You may perform operations such as:
 *   - priority_queue_remove_at(pq, idx)
 *   - priority_queue_update_priority_at(pq, idx, new_priority)
 *   - priority_queue_push(pq, ...)
 *
 * BUT: Any such operation can change the structure of the original heap.
 * If you use `idx` after such changes, it may no longer point to the original `data`.
 * Always retrieve a fresh `idx` with priority_queue_find_idx() if needed again.
 *
 * Do not cache `idx` across modifications.
 */
#define PRIORITY_QUEUE_SORTED_ITERATE(pq, idx, data, iter_count, iter_depth) \
	for ( \
		struct priority_queue *__pq_iter_copy = priority_queue_duplicate(pq); \
		__pq_iter_copy != NULL; \
		priority_queue_delete(__pq_iter_copy), __pq_iter_copy = NULL \
	) \
		for ((iter_count) = 0; \
		     (iter_count) < (iter_depth) && \
		     priority_queue_size(__pq_iter_copy) > 0 && \
		     (data = priority_queue_pop(__pq_iter_copy)) && \
		     (idx = priority_queue_find_idx(pq, data)) >= 0; \
		     (iter_count)++)

#endif
