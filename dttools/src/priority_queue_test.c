/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include "priority_queue.h"

int main()
{
	struct priority_queue *pq = priority_queue_create(2, 1);
	if (!pq) {
		fprintf(stderr, "Failed to create priority queue.\n");
		return EXIT_FAILURE;
	}

	char *data[] = {"Task A", "Task B", "Task C", "Task D", "Task E", "Task F"};
	double priorities[] = {3.0, 5.0, 1.0, 4.0, 2.0, 6.0};

	// Insert elements
	printf("Inserting elements:\n");
	for (int i = 0; i < 6; i++) {
		int idx = priority_queue_push_varargs(pq, data[i], priorities[i]);
		if (idx >= 0) {
			printf("Inserted '%s' with priority %.1f at index %d\n", data[i], priorities[i], idx);
		} else {
			printf("Failed to insert '%s'.\n", data[i]);
		}
	}

	// Get the size of the priority queue
	int size = priority_queue_size(pq);
	printf("\nCurrent priority queue size: %d\n", size);

	// BASE ITERATE: Starts from the beginning of the queue to the end
	int idx;
	char *item;
	int iter_count = 0;
	int iter_depth = priority_queue_size(pq);
	printf("\nIterating over the priority queue using PRIORITY_QUEUE_BASE_ITERATE:\n");
	PRIORITY_QUEUE_BASE_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}

	// Get the head of the priority queue
	char *head = (char *)priority_queue_peek_top(pq);
	if (head) {
		printf("\nElement at the head of the queue: %s\n", head);
	} else {
		printf("\nThe queue is empty.\n");
	}

	// Access an element by index
	idx = 4;
	char *element = (char *)priority_queue_peek_at(pq, idx);
	if (element) {
		printf("\nElement at index %d: %s\n", idx, element);
	} else {
		printf("\nNo element found at index %d.\n", idx);
	}

	// Find an element
	char *element_to_find = "Task D";
	int found_idx = priority_queue_find_idx(pq, element_to_find);
	printf("\nFinding element '%s':\n", element_to_find);
	if (found_idx >= 0) {
		printf("Element '%s' found at index %d\n", element_to_find, found_idx);
	} else {
		printf("Element '%s' not found in the queue.\n", element_to_find);
	}

	// Update the priority of an element
	int update_idx = priority_queue_update_priority(pq, "Task A", 0, 9.0);
	printf("\nUpdating the priority of 'Task A' to 9.0:\n");
	if (update_idx >= 0) {
		printf("Task A new index after priority update: %d\n", update_idx);
	} else {
		printf("Failed to update priority.\n");
	}

	// Insert an element
	int ins_idx = priority_queue_push_varargs(pq, "Task G", 11.0);
	printf("\nInserting Task G with priority 11.0:\n");
	if (ins_idx >= 0) {
		printf("Inserted Task G at index %d\n", ins_idx);
	} else {
		printf("Failed to insert Task G.\n");
	}

	// Iterate over elements using PRIORITY_QUEUE_BASE_ITERATE
	printf("\nIterating over the priority queue using PRIORITY_QUEUE_BASE_ITERATE:\n");
	idx = 0;
	item = NULL;
	iter_depth = priority_queue_size(pq);
	PRIORITY_QUEUE_BASE_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}

	// Iterate over elements using PRIORITY_QUEUE_ROTATE_ITERATE with a depth 3
	idx = 0;
	item = NULL;
	iter_count = 0;
	iter_depth = 4; // Maximum depth of the iteration
	printf("\nIterating over the priority queue using PRIORITY_QUEUE_ROTATE_ITERATE with a depth %d:\n", iter_depth);
	PRIORITY_QUEUE_ROTATE_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
		// The break check must go after the task is considered, as the rotate cursor is advanced in the macro and must be considered
	}

	priority_queue_rotate_reset(pq);
	iter_count = 0;
	iter_depth = 5;
	printf("\nReset the rotate cursor and Iterate from beginning with a depth %d:\n", iter_depth);
	PRIORITY_QUEUE_ROTATE_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}

	// Iterate over elements using PRIORITY_QUEUE_STATIC_ITERATE
	idx = 0;
	item = NULL;
	iter_count = 0;
	iter_depth = 4;
	printf("\nIterating over the priority queue using PRIORITY_QUEUE_STATIC_ITERATE with a depth %d:\n", iter_depth);
	PRIORITY_QUEUE_STATIC_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}
	iter_count = 0;
	iter_depth = 12;
	printf("Continue iterating from the last position with a depth %d\n", iter_depth);
	PRIORITY_QUEUE_STATIC_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}

	// Remove an element by index using priority_queue_remove
	printf("\nRemoving element at index 2.\n");
	if (priority_queue_remove(pq, 2)) {
		printf("Element at index 2 removed successfully.\n");
	} else {
		printf("Failed to remove element at index 2.\n");
	}

	iter_count = 0;
	iter_depth = priority_queue_size(pq);
	printf("\nIterating over the priority queue using PRIORITY_QUEUE_BASE_ITERATE:\n");
	PRIORITY_QUEUE_BASE_ITERATE(pq, idx, item, iter_count, iter_depth)
	{
		double prio = priority_queue_get_priority_at(pq, 0, idx);
		printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
	}

	// Pop elements from the priority queue using priority_queue_pop
	printf("\nPopping elements from the priority queue:\n");
	while ((item = (char *)priority_queue_peek_top(pq)) != NULL) {
		printf("Popped element: %s  Priority: %d\n", item, (int)priority_queue_get_priority_at(pq, 0, 0));
		priority_queue_pop(pq);
	}

	// Check the size after popping all elements
	size = priority_queue_size(pq);
	printf("\nPriority queue size after popping all elements: %d\n", size);

	priority_queue_delete(pq);

	return EXIT_SUCCESS;
}
