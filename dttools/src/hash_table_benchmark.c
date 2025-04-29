#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include "hash_table.h"

#define MAX_KEY_LEN 32
struct entry {
	char *key;
	void *value;
	unsigned hash;
	struct entry *next;
};

struct hash_table {
	hash_func_t hash_func;
	int bucket_count;
	int size;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
};

// Generate a unique string key from an integer
void generate_key(int i, char *key) {
    snprintf(key, MAX_KEY_LEN, "key%d", i);
}
double measure_iteration_time(struct hash_table *h) {
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
// Measure iteration time over the hash table
/*double measure_iteration_time(struct hash_table *h) {
    clock_t start = clock();

    char *key;
    void *value;
    HASH_TABLE_ITERATE(h, key, value);

    clock_t end = clock();
    return (double)(end - start) / CLOCKS_PER_SEC;
}
*/
int main() {
    const int step = 10000000;
    const int max = 90000000;

    struct hash_table *h = hash_table_create(127, NULL);

    printf("INSERTION PHASE:\n");
    for (int n = 0; n <= max; n += step) {
	printf("Num buckets: %10d|", h->bucket_count);
        double t = measure_iteration_time(h);
        printf("Items: %d, Iteration time: %.6f sec\n", n, t);
	if (n == max)
	    break;
        for (int i = n; i < n+step; ++i) {
            char key[MAX_KEY_LEN];
            generate_key(i, key);
            hash_table_insert(h, key, (void*)(intptr_t)i);
        }
    }

    printf("\nREMOVAL PHASE:\n");
    for (int n = max; n >= 0; n -= step) {
        double t = measure_iteration_time(h);
	printf("Num buckets: %10d|", h->bucket_count);
        printf("Items: %d, Iteration time: %.6f sec\n", n, t);
        for (int i = n-step; i < n; ++i) {
            char key[MAX_KEY_LEN];
            generate_key(i, key);
            hash_table_remove(h, key);
        }
    }

    hash_table_delete(h);
    return 0;
}
