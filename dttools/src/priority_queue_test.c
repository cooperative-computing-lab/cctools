#include "priority_queue.h"
#include "timestamp.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

int priority_queue_validate(struct priority_queue *pq)
{
	if (!pq) {
		return 0;
	}

	for (int i = 0; i < priority_queue_size(pq); i++) {
		int left = 2 * i + 1;
		int right = 2 * i + 2;
		if (left < priority_queue_size(pq) &&
				priority_queue_get_priority_at(pq, i) < priority_queue_get_priority_at(pq, left)) {
			fprintf(stderr, "heap violation: parent[%d] < left[%d]\n", i, left);
			return 0;
		}
		if (right < priority_queue_size(pq) &&
				priority_queue_get_priority_at(pq, i) < priority_queue_get_priority_at(pq, right)) {
			fprintf(stderr, "heap violation: parent[%d] < right[%d]\n", i, right);
			return 0;
		}
	}
	return 1;
}

void test_correctness(int ops)
{
	int check_interval = ops / 100;
	printf("[Correctness] running with %d ops...\n", ops);
	srand(42);
	struct priority_queue *pq = priority_queue_create(0);

	for (int i = 0; i < ops; i++) {
		int *x = malloc(sizeof(int));
		*x = i;
		priority_queue_push(pq, x, rand() % 10000000);

		if (i % check_interval == 0) {
			assert(priority_queue_validate(pq));
		}
	}

	for (int i = 0; i < 1000; i++) {
		int idx = rand() % priority_queue_size(pq);
		priority_queue_update_priority_at(pq, idx, rand() % 10000000);
		assert(priority_queue_validate(pq));
	}

	for (int i = 0; i < 1000; i++) {
		int idx = rand() % priority_queue_size(pq);
		priority_queue_remove_at(pq, idx);
		assert(priority_queue_validate(pq));
	}

	int iter_count = 0;
	int idx;
	void *data;
	int seen = 0;

	int removed_count = 0;
	int before_size = priority_queue_size(pq);

	PRIORITY_QUEUE_SORTED_ITERATE(pq, idx, data, iter_count, before_size)
	{
		assert(data != NULL);
		assert(idx >= 0 && idx < priority_queue_size(pq));
		if (priority_queue_get_priority_at(pq, idx) < rand() % 1000000) {
			int ok = priority_queue_remove_at(pq, idx);
			removed_count++;
			assert(ok == 1);
			assert(priority_queue_size(pq) == before_size - removed_count);
		}
		seen++;
	}
	assert(seen == before_size);
	assert(priority_queue_size(pq) == before_size - removed_count);
	assert(priority_queue_validate(pq));

	while (priority_queue_size(pq) > 0) {
		void *x = priority_queue_pop(pq);
		free(x);
		assert(priority_queue_validate(pq));
	}

	assert(priority_queue_size(pq) == 0);
	assert(priority_queue_pop(pq) == NULL);
	assert(priority_queue_peek_top(pq) == NULL);
	assert(isnan(priority_queue_get_priority_at(pq, 0)));

	priority_queue_delete(pq);
	printf("[Correctness] passed. Sorted macro tested on %d elements.\n", seen);
}

void test_performance(int ops)
{
	printf("[Performance] running with %d ops...\n", ops);
	struct priority_queue *pq = priority_queue_create(0);
	void **refs = malloc(sizeof(void *) * ops);

	timestamp_t t_push_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		refs[i] = malloc(sizeof(int));
		*(int *)refs[i] = i;
		priority_queue_push(pq, refs[i], rand() % 10000000);
	}
	timestamp_t t_push_end = timestamp_get();

	timestamp_t t_peek_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		int idx = rand() % priority_queue_size(pq);
		priority_queue_peek_at(pq, idx);
	}
	timestamp_t t_peek_end = timestamp_get();

	timestamp_t t_pop_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		void *x = priority_queue_pop(pq);
		free(x);
	}
	timestamp_t t_pop_end = timestamp_get();

	free(refs);
	priority_queue_delete(pq);

	printf("[Performance] push: %.2fs, peek: %.2fs, pop: %.2fs\n", (t_push_end - t_push_start) / 1e6, (t_peek_end - t_peek_start) / 1e6, (t_pop_end - t_pop_start) / 1e6);
}

void test_push_or_update(int ops)
{
    printf("[PQUEUE find+update] running with %d ops...\n", ops);
    struct priority_queue *pq = priority_queue_create(0);
    int *pool = malloc(sizeof(int) * ops);

    for (int i = 0; i < ops; i++) {
        pool[i] = i;
    }

    srand(42);
    timestamp_t t_start = timestamp_get();

    for (int i = 0; i < ops; i++) {
        int *ref = &pool[rand() % ops];
        double new_prio = rand() % 10000000;

        int idx = priority_queue_find_idx(pq, ref);
        if (idx >= 0) {
            priority_queue_update_priority_at(pq, idx, new_prio);
        } else {
            priority_queue_push(pq, ref, new_prio);
        }
    }

    while (priority_queue_size(pq) > 0) {
        int *x = priority_queue_pop(pq);
        assert(x != NULL);
    }

    timestamp_t t_end = timestamp_get();
    printf("[PQUEUE find+update] total time: %.2fs\n", (t_end - t_start) / 1e6);

    priority_queue_delete(pq);
    free(pool);
}

int main()
{
	timestamp_t t1 = timestamp_get();
	test_correctness(1e4);
	timestamp_t t2 = timestamp_get();
	test_performance(1e6);
	timestamp_t t3 = timestamp_get();
	test_push_or_update(1e6);
	timestamp_t t4 = timestamp_get();

	printf("Correctness test time: %.2fs\n", (t2 - t1) / 1e6);
	printf("Performance test time: %.2fs\n", (t3 - t2) / 1e6);
	printf("Push or update test time: %.2fs\n", (t4 - t3) / 1e6);
	return 0;
}
