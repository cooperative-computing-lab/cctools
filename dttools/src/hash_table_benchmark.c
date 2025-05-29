#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <stdint.h>
#include "hash_table.h"

#define MAX_KEY_LEN 32

// Generate a unique string key from an integer
void generate_key(int i, char *key)
{
	snprintf(key, MAX_KEY_LEN, "key%d", i);
}

double measure_iteration_time(struct hash_table *h)
{
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);

	char *key;
	void *value;
	HASH_TABLE_ITERATE(h, key, value);

	clock_gettime(CLOCK_MONOTONIC, &end);

	double start_sec = start.tv_sec + start.tv_nsec / 1e9;
	double end_sec = end.tv_sec + end.tv_nsec / 1e9;

	return end_sec - start_sec;
}

int main()
{
	int power_step = 0;
	int power_max_step = 15; // ~64k max entries

	struct hash_table *h = hash_table_create(0, 0);

	char key[MAX_KEY_LEN];
	int entries_counter = 0;

	printf("INSERTION PHASE:\n");
	for (power_step = 0; power_step <= power_max_step; power_step++) {
		double total_time = 0;
		double max_load = hash_table_load(h);
		double entries_to_add_remove = pow(2, power_step);
		for (int i = 0; i < entries_to_add_remove; i++) {
			entries_counter++;
			generate_key(entries_counter, key);
			hash_table_insert(h, key, NULL);
			max_load = hash_table_load(h) > max_load ? hash_table_load(h) : max_load;
			total_time += measure_iteration_time(h);
		}

		printf("step %3d size %8d buckets %8d load_max %3.6f load_now %3.6f time %3.6f time_norm %3.6f\n", power_step, hash_table_size(h), (int)ceil(hash_table_size(h) / hash_table_load(h)), max_load, hash_table_load(h), total_time, total_time / entries_to_add_remove);
	}

	printf("REMOVAL PHASE:\n");

	entries_counter = 0;
	for (power_step = power_max_step; power_step > 0; power_step--) {
		double total_time = 0;
		double min_load = hash_table_load(h);
		double entries_to_add_remove = pow(2, power_step);
		for (int i = 0; i < entries_to_add_remove; i++) {
			entries_counter++;
			generate_key(entries_counter, key);
			hash_table_remove(h, key);
			min_load = hash_table_load(h) < min_load ? hash_table_load(h) : min_load;
			total_time += measure_iteration_time(h);
		}

		printf("step %3d size %8d buckets %8d load_min %3.6f load_now %3.6f time %3.6f time_norm %3.6f\n", power_step, hash_table_size(h), (int)ceil(hash_table_size(h) / hash_table_load(h)), min_load, hash_table_load(h), total_time, total_time / entries_to_add_remove);
	}

	hash_table_delete(h);

	return 0;
}
