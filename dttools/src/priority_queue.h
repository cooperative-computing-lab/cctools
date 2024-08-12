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
void *headData = priority_queue_get_head(pq);
</pre>

To list all of the items in a priority queue, use a simple loop:
<pre>
for (int i = 1; i <= priority_queue_size(pq); i++) {
    void *data = priority_queue_get_element(pq, i);
    printf("Priority queue contains: %p\n", data);
}
</pre>

Or use the PRIORITY_QUEUE_ITERATE macro:

<pre>
void *data;
PRIORITY_QUEUE_ITERATE (pq, data) {
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
@param pq A pointer to a priority queue.
@param data A pointer to store in the queue.
@param priority The specified priority with the given object.
@return One if the push succeeded, failure otherwise
*/
int priority_queue_push(struct priority_queue *pq, void *data, double priority);

/** Pop the element with the highest priority from a priority queue.
@param pq A pointer to a priority queue.
@return The pointer to the top of the heap if any, failure otherwise
*/
void *priority_queue_pop(struct priority_queue *pq);

/** Get the element with the highest priority from a priority queue.
Similar to @ref priority_queue_pop, but the element is not removed.
@param pq A pointer to a priority queue.
@return The pointer to the top of the heap if any, failure otherwise
*/
void *priority_queue_get_head(struct priority_queue *pq);

/** Get the element with the specified index from a priority queue.
The first accessible element is at index 1.
@param pq A pointer to a priority queue.
@param index The index of the element to get.
@return The pointer to the element if any, failure otherwise
*/
void *priority_queue_get_element(struct priority_queue *pq, int index);

/** Get the highest priority of all elements from a priority queue.
@param pq A pointer to a priority queue.
@return The highest priority of the queue.
*/
double priority_queue_get_max_priority(struct priority_queue *pq);

/** Get the lowest priority of all elements from a priority queue.
@param pq A pointer to a priority queue.
@return The lowest priority of the queue.
*/
double priority_queue_get_min_priority(struct priority_queue *pq);

/** Remove the element with the specified index from a priority queue.
@param pq A pointer to a priority queue.
@param index The index of the element to remove.
@return One if the remove succeeded, failure otherwise
*/
int priority_queue_remove(struct priority_queue *pq, void *data);

/** Destroy a priority queue.
@param pq A pointer to a priority queue.
*/
void priority_queue_destroy(struct priority_queue *pq);

/** Utility macro to simplify common case of iterating over a priority queue.
Use as follows:

<pre>
char *data;

PRIORITY_QUEUE_ITERATE(pq, data) {
	printf("data: %s\n", data);
}

</pre>
*/
#define PRIORITY_QUEUE_ITERATE( pq, data ) int i = 1; while ((data = priority_queue_get_element(pq, i++)))


#endif
