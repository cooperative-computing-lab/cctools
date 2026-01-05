/* Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <assert.h>
#include "list.h"
#include "skip_list.h"

#define MAX_KEY_LEN 32

struct item {
	char key[MAX_KEY_LEN];
	double priority;
};

/* Generate a unique string key from an integer */
void generate_key(int i, char *key)
{
	snprintf(key, MAX_KEY_LEN, "item%d", i);
}

/* Priority function for list_push_priority */
double priority_func(void *item)
{
	struct item *it = (struct item *)item;
	return it->priority;
}

int compare_priority(const double *p1, const double *p2, unsigned size)
{
	for (unsigned i = 0; i < size; i++) {
		if (p1[i] > p2[i])
			return 1;
		if (p1[i] < p2[i])
			return -1;
	}
	return 0;
}

struct item **make_items(int num_elements, int max_element)
{
	struct item **items = malloc(num_elements * sizeof(struct item *));
	for (int i = 0; i < num_elements; i++) {
		items[i] = malloc(sizeof(struct item));
		generate_key(i, items[i]->key);
		items[i]->priority = ceil((double)rand() / RAND_MAX * max_element);
	}
	return items;
}

void free_items(struct item **items, int num_elements)
{
	for (int i = 0; i < num_elements; i++) {
		free(items[i]);
	}
	free(items);
}

/* Measure time for list operations with priority-based insertion */
double measure_list_operations(struct item **items, int num_elements, int operation_type)
{
	struct list *l = list_create();
	struct timespec start, end;

	if (operation_type == 1) {
		/* Measure insertion time with priority-based sorting */
		clock_gettime(CLOCK_MONOTONIC, &start);

		for (int i = 0; i < num_elements; i++) {
			list_push_priority(l, priority_func, items[i]);
		}

		clock_gettime(CLOCK_MONOTONIC, &end);
	} else if (operation_type == 2) {
		/* Pre-populate the list for find test */
		for (int i = 0; i < num_elements; i++) {
			list_push_priority(l, priority_func, items[i]);
		}

		/* Measure remove time - search through list */
		clock_gettime(CLOCK_MONOTONIC, &start);

		/* Remove all elements - must iterate through list */
		for (int i = 0; i < num_elements; i++) {
			/* Linear search through list */
			struct list_cursor *cur = list_cursor_create(l);
			list_seek(cur, 0);
			struct item *found;
			while (list_get(cur, (void **)&found)) {
				if (compare_priority(&found->priority, &items[i]->priority, 1) == 0) {
					list_drop(cur);
					break;
				}
				list_next(cur);
			}
			list_cursor_destroy(cur);
		}

		if (list_length(l) != 0) {
			printf("FAIL: list_length %d is not 0 after removal\n", list_length(l));
			return -1;
		}

		clock_gettime(CLOCK_MONOTONIC, &end);
	}

	list_delete(l);
	return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

/* Measure time for skip_list operations */
double measure_skip_list_operations(struct item **items, int num_elements, int operation_type)
{
	struct skip_list *sl = skip_list_create(1, 0.5); // 1D priority, 0.5 promotion probability
	struct skip_list_cursor *cur = skip_list_cursor_create(sl);

	struct timespec start, end;

	if (operation_type == 1) {
		/* Measure insertion time with priority-based sorting */
		clock_gettime(CLOCK_MONOTONIC, &start);

		for (int i = 0; i < num_elements; i++) {
			/* Insert with priority - skip_list maintains sorted order */
			skip_list_insert(sl, items[i], items[i]->priority);
		}

		clock_gettime(CLOCK_MONOTONIC, &end);
	} else if (operation_type == 2) {
		/* Pre-populate the skip_list for find test */
		for (int i = 0; i < num_elements; i++) {
			skip_list_insert(sl, items[i], items[i]->priority);
		}

		/* Measure find time - use skip_list_search */
		clock_gettime(CLOCK_MONOTONIC, &start);

		/* Remove all elements using skip_list_remove_by_priority */
		for (int i = 0; i < num_elements; i++) {
			skip_list_remove_by_priority(sl, items[i]->priority);
		}
		if (skip_list_size(sl) != 0) {
			printf("FAIL: skip_list_size %d is not 0 after removal\n", skip_list_size(sl));
			return -1;
		}

		clock_gettime(CLOCK_MONOTONIC, &end);
	}

	skip_list_cursor_delete(cur);
	skip_list_delete(sl);
	return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

struct test_result {
	int num_elements;
	double list_time;
	double skip_list_time;
	const char *test_type;
};

int main(int argc, char *argv[])
{
	int min_power = 10; // Default: test up to 2^10 = 1024 elements
	int max_power = 14; // Default: test up to 2^14 = 16384 elements

	if (argc > 1) {
		max_power = atoi(argv[1]);
		if (max_power < 1 || max_power > 32) {
			fprintf(stderr, "Usage: %s [max_power]\n", argv[0]);
			fprintf(stderr, "  max_power: test up to 2^max_power elements (1-20, default: 12)\n");
			return 1;
		}
	}

	// Calculate array sizes
	int insertion_count = max_power - 10 + 1; // power 10 to max_power
	int find_count = max_power - 6 + 1;	  // power 6 to max_power
	int total_count = insertion_count + find_count;

	// Allocate arrays for results
	struct test_result *results = malloc(total_count * sizeof(struct test_result));
	int result_idx = 0;

	printf("=================================================================\n");
	printf("Performance Comparison: list.c vs skip_list.c\n");
	printf("=================================================================\n\n");

	printf("INSERTION TEST:\n");
	printf("-----------------------------------------------------------------\n");
	printf("%10s %15s %15s %15s\n",
			"elements",
			"list.c time(s)",
			"skip_list.c time(s)",
			"speedup");
	printf("-----------------------------------------------------------------\n");

	for (int power = min_power; power <= max_power; power++) {
		int num_elements = (int)pow(2, power);
		struct item **items = make_items(num_elements, RAND_MAX);

		double skip_list_time = measure_skip_list_operations(items, num_elements, 1);
		double speedup = -1;
		double list_time = -1;
		if (power < 16) {
			list_time = measure_list_operations(items, num_elements, 1);
			speedup = list_time / skip_list_time;
		}

		// Store results
		results[result_idx].num_elements = num_elements;
		results[result_idx].list_time = list_time;
		results[result_idx].skip_list_time = skip_list_time;
		results[result_idx].test_type = "insertion";
		result_idx++;

		printf("%10d %15.6f %15.6f %15.2fx\n",
				num_elements,
				list_time,
				skip_list_time,
				speedup);

		free_items(items, num_elements);
	}

	printf("\n");
	printf("REMOVE TEST:\n");
	printf("-----------------------------------------------------------------\n");
	printf("%10s %15s %15s %15s\n",
			"elements",
			"list.c time(s)",
			"skip_list.c time(s)",
			"speedup");
	printf("-----------------------------------------------------------------\n");

	for (int power = min_power; power <= max_power; power++) {
		int num_elements = (int)pow(2, power);
		struct item **items = make_items(num_elements, RAND_MAX);

		double skip_list_time = measure_skip_list_operations(items, num_elements, 2);
		double speedup = -1;

		double list_time = -1;
		if (power < 15) {
			list_time = measure_list_operations(items, num_elements, 2);
			speedup = list_time / skip_list_time;
		}

		// Store results
		results[result_idx].num_elements = num_elements;
		results[result_idx].list_time = list_time;
		results[result_idx].skip_list_time = skip_list_time;
		results[result_idx].test_type = "find";
		result_idx++;

		printf("%10d %15.6f %15.6f %15.2fx\n",
				num_elements,
				list_time,
				skip_list_time,
				speedup);

		free_items(items, num_elements);
	}

	printf("\n");
	printf("=================================================================\n");
	printf("Priority Order Test: Verifying elements are in correct order\n");
	printf("=================================================================\n");

	// Test with min_power elements
	int num_elements = (int)pow(2, min_power);
	struct item **items = make_items(num_elements, 100);

	// Create and populate skip list
	struct skip_list *sl = skip_list_create(2, 0.5);
	struct skip_list_cursor *cur = skip_list_cursor_create(sl);

	for (int i = 0; i < num_elements; i++) {
		skip_list_insert(sl, items[i], items[i]->priority, i);
	}

	printf("Testing %d elements for correct priority ordering...\n", num_elements);

	double prev_priority[2] = {INFINITY, INFINITY};
	double head_priority[2];

	int order_time_test_passed = 1;
	int elements_checked = 0;

	while (skip_list_size(sl) > 0) {
		// Peek at head priority
		memcpy(head_priority, skip_list_peek_head_priority(sl), 2 * sizeof(double));

		// Peek at head data
		const void *peeked_data = skip_list_peek_head(sl);
		if (!peeked_data) {
			printf("FAIL: skip_list_peek_head returned NULL but list is not empty\n");
			order_time_test_passed = 0;
			break;
		}

		// Pop head
		void *popped_data = skip_list_pop_head(sl);
		if (!popped_data) {
			printf("FAIL: skip_list_pop_head returned NULL but list is not empty\n");
			order_time_test_passed = 0;
			break;
		}

		// Verify peeked data matches popped data
		if (peeked_data != popped_data) {
			printf("FAIL: peeked data (%p) does not match popped data (%p)\n",
					peeked_data,
					popped_data);
			order_time_test_passed = 0;
			break;
		}

		// Verify priority is in descending order
		if (compare_priority(head_priority, prev_priority, 2) > 0) {
			printf("FAIL: priority (%.6f, %.6f) is greater than previous priority (%.6f, %.6f)\n",
					head_priority[0],
					head_priority[1],
					prev_priority[0],
					prev_priority[1]);
			order_time_test_passed = 0;
			break;
		}

		memcpy(prev_priority, head_priority, 2 * sizeof(double));
		elements_checked++;
	}

	if (order_time_test_passed && elements_checked == num_elements) {
		printf("PASS: All %d elements were in correct descending priority order\n", elements_checked);
		printf("PASS: Peek and pop operations matched correctly\n");
	} else if (order_time_test_passed) {
		printf("FAIL: Only checked %d elements, expected %d\n", elements_checked, num_elements);
		order_time_test_passed = 0;
	}

	skip_list_cursor_delete(cur);
	skip_list_delete(sl);
	free_items(items, num_elements);

	printf("=================================================================\n\n");
	printf("=================================================================\n");
	printf("Test Results: Verifying skip_list is faster than list\n");
	printf("=================================================================\n");

	int time_test_passed = 1;
	int failures = 0;

	// Check if skip_list is faster than list in all cases
	for (int i = 0; i < result_idx; i++) {
		// Only check cases where both timings were measured
		if (results[i].list_time > 0 && results[i].skip_list_time > 0) {
			if (results[i].list_time < results[i].skip_list_time) {
				printf("FAIL: %s test with %d elements: list.c (%.6fs) faster than skip_list.c (%.6fs)\n",
						results[i].test_type,
						results[i].num_elements,
						results[i].list_time,
						results[i].skip_list_time);
				time_test_passed = 0;
				failures++;
			}
		}
	}

	if (time_test_passed) {
		printf("PASS: skip_list.c was faster than or equal to list.c in all %d compared cases\n", result_idx);
		printf("=================================================================\n");
	} else {
		printf("=================================================================\n");
		printf("TEST FAILED: %d case(s) where list.c was faster than skip_list.c\n", failures);
		printf("=================================================================\n");
	}

	free(results);

	return (time_test_passed && order_time_test_passed) ? 0 : 1;
}
