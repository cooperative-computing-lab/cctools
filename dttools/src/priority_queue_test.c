/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include "priority_queue.h"


int main() {
    struct priority_queue *pq = priority_queue_create(2);
    if (!pq) {
        fprintf(stderr, "Failed to create priority queue.\n");
        return EXIT_FAILURE;
    }

    char *data[] = { "Task A", "Task B", "Task C", "Task D", "Task E", "Task F" };
    double priorities[] = { 3.0, 5.0, 1.0, 4.0, 2.0, 6.0 };

    // Insert elements
    printf("Inserting elements:\n");
    for (int i = 0; i < 6; i++) {
        int idx = priority_queue_push(pq, data[i], priorities[i]);
        if (idx > 0) {
            printf("Inserted '%s' with priority %.1f at index %d\n", data[i], priorities[i], idx);
        } else {
            printf("Failed to insert '%s'.\n", data[i]);
        }
    }

    // BASE ITERATE: Starts from the beginning of the queue to the end
    int idx;
    char *item;
    PRIORITY_QUEUE_BASE_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
    }

    // Get the size of the priority queue
    int size = priority_queue_size(pq);
    printf("\nCurrent priority queue size: %d\n", size);

    // Get the head of the priority queue
    char *head = (char *)priority_queue_get_head(pq);
    if (head) {
        printf("\nElement at the head of the queue: %s\n", head);
    } else {
        printf("\nThe queue is empty.\n");
    }

    // Access an element by index
    int index_to_get = 4;
    char *element = (char *)priority_queue_get_element(pq, index_to_get);
    if (element) {
        printf("\nElement at index %d: %s\n", index_to_get, element);
    } else {
        printf("\nNo element found at index %d.\n", index_to_get);
    }

    // Find an element
    char *element_to_find = "Task D";
    int found_idx = priority_queue_find_idx(pq, element_to_find);
    if (found_idx > 0) {
        printf("\nElement '%s' found at index %d\n", element_to_find, found_idx);
    } else {
        printf("\nElement '%s' not found in the queue.\n", element_to_find);
    }

    // Update the priority of an element
    int update_idx = priority_queue_update_priority(pq, "Task A", 9.0);
    if (update_idx > 0) {
        printf("'Task C' new index after priority update: %d\n", update_idx);
    } else {
        printf("Failed to update priority.\n");
    }

    // Insert an element
    int upward_idx = priority_queue_push_upward(pq, "Task G", 9.0);
    if (upward_idx > 0) {
        printf("Inserted 'Task G' with priority 9.0 at index %d\n", upward_idx);
    } else {
        printf("Failed to insert 'Task F'.\n");
    }

    // Iterate over elements using PRIORITY_QUEUE_BASE_ITERATE
    printf("\nIterating over the priority queue using PRIORITY_QUEUE_BASE_ITERATE:\n");
    idx = 0;
    item = NULL;
    PRIORITY_QUEUE_BASE_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
    }

    // Iterate over elements using PRIORITY_QUEUE_ROTATE_ITERATE with a depth 3
    printf("\nIterating over the priority queue using PRIORITY_QUEUE_ROTATE_ITERATE:\n");
    idx = 0;
    item = NULL;
    int iter_depth = 4;   // Maximum depth of the iteration
    PRIORITY_QUEUE_ROTATE_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
        // The break check must go after the task is considered, as the rotate cursor is advanced in the macro and must be considered
        if (iter_depth-- == 0) {
            break;
        }
    }
    printf("Reset the rotate cursor and Iterate from beginning.\n");
    priority_queue_rotate_reset(pq);
    iter_depth = 7;
    PRIORITY_QUEUE_ROTATE_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
        if (iter_depth-- == 0) {
            break;
        }
    }

    // Iterate over elements using PRIORITY_QUEUE_STATIC_ITERATE
    printf("\nIterating over the priority queue using PRIORITY_QUEUE_STATIC_ITERATE:\n");
    idx = 0;
    item = NULL;
    iter_depth = 4;
    PRIORITY_QUEUE_STATIC_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
        if (iter_depth-- == 0) {
            break;
        }
    }
    iter_depth = 8;
    printf("Continue iterating from the last position.\n");
    PRIORITY_QUEUE_STATIC_ITERATE(pq, idx, item) {
        double prio = priority_queue_get_priority(pq, idx);
        printf("Index: %d, Element: %s, Priority: %.1f\n", idx, item, prio);
        if (iter_depth-- == 0) {
            break;
        }
    }

    // Remove an element by index using priority_queue_remove
    printf("\nRemoving element at index 2.\n");
    if (priority_queue_remove(pq, 2)) {
        printf("Element at index 2 removed successfully.\n");
    } else {
        printf("Failed to remove element at index 2.\n");
    }

    // Pop elements from the priority queue using priority_queue_pop
    printf("\nPopping elements from the priority queue:\n");
    while ((item = (char *)priority_queue_get_head(pq)) != NULL) {
        printf("Popped element: %s  Priority: %d\n", item, (int)priority_queue_get_priority(pq, 1));
        priority_queue_pop(pq);
    }

    // Check the size after popping all elements
    size = priority_queue_size(pq);
    printf("\nPriority queue size after popping all elements: %d\n", size);

    priority_queue_delete(pq);

    return EXIT_SUCCESS;
}
