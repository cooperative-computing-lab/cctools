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

For example, to create a priority queue and manipulate its elements:
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
The 0th element is used as a sentinel with the highest priority to simplify boundary checks in heap operations like swim and sink.
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
function @ref swim is called to maintain the heap property.
@param pq A pointer to a priority queue.
@param data A pointer to store in the queue.
@param priority The specified priority with the given object.
@return One if the push succeeded, failure otherwise
*/
int priority_queue_push(struct priority_queue *pq, void *data, double priority);

/** Pop the element with the highest priority from a priority queue.
function @ref sink is called to maintain the heap property.
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
@param pq A pointer to a priority queue.
@param index The index of the element to get.
@return The pointer to the element if any, failure otherwise
*/
void *priority_queue_get_element(struct priority_queue *pq, int index);

/** Remove the element with the specified index from a priority queue.
function @ref sink is called to maintain the heap property.
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
