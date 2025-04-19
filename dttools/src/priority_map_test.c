#include "priority_map.h"
#include "timestamp.h"
#include "xxmalloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>
#include <limits.h>

static char *gen_key(const void *data)
{
	int id = *(int *)data;
	char *key = xxmalloc(32);
	snprintf(key, 32, "key%d", id);
	return key;
}

void test_correctness(int ops)
{
	int check_interval = ops / 100;
	if (check_interval < 1)
		check_interval = 1;

	printf("[Correctness] running with %d ops...\n", ops);
	srand(42);

	struct priority_map *pmap = priority_map_create(0, gen_key);
	int **refs = xxmalloc(sizeof(int *) * ops);

	for (int i = 0; i < ops; i++) {
		refs[i] = xxmalloc(sizeof(int));
		*refs[i] = i;
		assert(priority_map_push(pmap, refs[i], rand() % 10000000) >= 0);
		if (i % check_interval == 0) {
			assert(priority_map_validate(pmap));
		}

		assert(priority_map_push(pmap, refs[i], rand() % 10000000) == 0);
	}

	int *ghost = xxmalloc(sizeof(int));
	*ghost = -1;
	assert(priority_map_update_priority(pmap, ghost, 9999) == 0);
	assert(priority_map_remove(pmap, ghost) == 0);
	free(ghost);

	int *tmp = xxmalloc(sizeof(int));
	*tmp = -999;
	assert(priority_map_push(pmap, tmp, 100));
	assert(priority_map_remove(pmap, tmp));
	assert(priority_map_validate(pmap));
	free(tmp);

	for (int i = 0; i < 1000; i++) {
		int idx = rand() % priority_map_size(pmap);
		assert(priority_map_update_priority(pmap, refs[idx], rand() % 10000000) >= 0);
		assert(priority_map_validate(pmap));
	}

	int removed = 0;
	for (int i = 0; i < ops && removed < 1000; i++) {
		if (refs[i] && priority_map_remove(pmap, refs[i])) {
			refs[i] = NULL;
			removed++;
			assert(priority_map_validate(pmap));
		}
	}

	struct priority_map *copy = priority_map_duplicate(pmap);
	assert(priority_map_validate(copy));
	priority_map_delete(copy);

	while (priority_map_size(pmap) > 0) {
		void *x = priority_map_pop(pmap);
		assert(x != NULL);
		free(x);
		assert(priority_map_validate(pmap));
	}

	assert(priority_map_size(pmap) == 0);
	assert(priority_map_peek_top(pmap) == NULL);
	assert(priority_map_pop(pmap) == NULL);

	priority_map_delete(pmap);

	for (int i = 0; i < ops; i++) {
		free(refs[i]);
	}
	free(refs);

	printf("[Correctness] passed.\n");
}

void test_performance(int ops)
{
	printf("[Performance] running with %d ops...\n", ops);
	struct priority_map *pmap = priority_map_create(0, gen_key);
	int **refs = xxmalloc(sizeof(int *) * ops);

	timestamp_t t_push_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		refs[i] = xxmalloc(sizeof(int));
		*refs[i] = i;
		priority_map_push(pmap, refs[i], rand() % 10000000);
	}
	timestamp_t t_push_end = timestamp_get();

	timestamp_t t_peek_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		priority_map_peek_priority(pmap, refs[rand() % ops]);
	}
	timestamp_t t_peek_end = timestamp_get();

	timestamp_t t_pop_start = timestamp_get();
	for (int i = 0; i < ops; i++) {
		void *x = priority_map_pop(pmap);
		free(x);
	}
	timestamp_t t_pop_end = timestamp_get();

	free(refs);
	priority_map_delete(pmap);

	printf("[Performance] push: %.2fs, peek: %.2fs, pop: %.2fs\n",
			(t_push_end - t_push_start) / 1e6,
			(t_peek_end - t_peek_start) / 1e6,
			(t_pop_end - t_pop_start) / 1e6);
}

void test_push_or_update(int ops)
{
	printf("[PMAP push_or_update] running with %d ops...\n", ops);
	struct priority_map *pmap = priority_map_create(0, gen_key);
	int *pool = malloc(sizeof(int) * ops);

	for (int i = 0; i < ops; i++) {
		pool[i] = i;
	}

	srand(42);
	timestamp_t t_start = timestamp_get();

	for (int i = 0; i < ops; i++) {
		int *ref = &pool[rand() % ops];
		double prio = rand() % 10000000;

		priority_map_push_or_update(pmap, ref, prio);
	}

	while (priority_map_size(pmap) > 0) {
		int *x = priority_map_pop(pmap);
		assert(x != NULL);
	}

	timestamp_t t_end = timestamp_get();
	printf("[PMAP push_or_update] total time: %.2fs\n", (t_end - t_start) / 1e6);

	priority_map_delete(pmap);
	free(pool);
}

int main()
{
	timestamp_t t1 = timestamp_get();
	test_correctness(1e3);
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
