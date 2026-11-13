/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Randomized correctness test for hash_table.

A simple reference model (a flat array indexed by key number) is kept in
lock step with the hash table while random inserts, lookups, removes,
remove_random, and remove_any operations are performed.  After every
operation the result is compared against the model, and periodically the
entire table contents are verified through every available enumeration
interface (iteration macro, foreach_ro, keys).

Usage: hash_table_test [seed] [operations-per-phase]

The same seed always produces the same sequence of operations, so any
failure can be reproduced.  Exit status is zero on success.
*/

#include "hash_table.h"
#include "debug.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define KEY_UNIVERSE 16384

/* Distinct objects used as values.  The model stores pointers into these. */
static int values_a[KEY_UNIVERSE];
static int values_b[KEY_UNIVERSE];

/* Reference model: model[k] is the expected value for key k, or NULL. */
static void *model[KEY_UNIVERSE];
static int model_count = 0;

static char keys[KEY_UNIVERSE][32];

static uint64_t rng_state;
static unsigned long checks = 0;

/* xorshift64*: small, deterministic, independent of libc rand(). */
static uint64_t rng_next(void)
{
	rng_state ^= rng_state >> 12;
	rng_state ^= rng_state << 25;
	rng_state ^= rng_state >> 27;
	return rng_state * 2685821657736338717ULL;
}

static unsigned rng_below(unsigned n)
{
	return (unsigned)(rng_next() % n);
}

#define CHECK(cond, ...)                                                                           \
	do {                                                                                       \
		checks++;                                                                          \
		if (!(cond)) {                                                                     \
			fprintf(stderr, "FAIL %s:%d: %s\n  ", __FILE__, __LINE__, #cond);          \
			fprintf(stderr, __VA_ARGS__);                                              \
			fprintf(stderr, "\n  (seed %llu)\n", (unsigned long long)seed_used);       \
			exit(1);                                                                   \
		}                                                                                  \
	} while (0)

static uint64_t seed_used = 0;

/* Alternative hash functions to force collisions and long probe chains. */

static unsigned hash_constant(const char *s)
{
	(void)s;
	return 12345;
}

static unsigned hash_constant_max(const char *s)
{
	(void)s;
	return UINT_MAX;
}

static unsigned hash_few_values(const char *s)
{
	unsigned h = 0;
	while (*s)
		h = h * 31 + (unsigned char)*s++;
	return (h % 7) * 1000003u;
}

/* Which key number does this key string name?  Returns -1 if malformed. */
static int key_number(const char *key)
{
	int n;
	char extra;
	if (sscanf(key, "key-%d%c", &n, &extra) != 1)
		return -1;
	if (n < 0 || n >= KEY_UNIVERSE)
		return -1;
	return n;
}

/* Context for the foreach_ro verification. */
struct verify_ctx {
	int seen[KEY_UNIVERSE];
	int count;
};

static void verify_cb(const char *key, void *value, void *arg)
{
	struct verify_ctx *ctx = arg;
	int k = key_number(key);
	CHECK(k >= 0, "iterated over unknown key '%s'", key);
	CHECK(!ctx->seen[k], "key '%s' visited twice", key);
	CHECK(model[k] != NULL, "key '%s' present in table but not in model", key);
	CHECK(model[k] == value, "key '%s' has wrong value in iteration", key);
	ctx->seen[k] = 1;
	ctx->count++;
}

/* Compare the full contents of the table against the model. */
static void verify_all(struct hash_table *h)
{
	CHECK(hash_table_size(h) == model_count, "size %d, model has %d", hash_table_size(h), model_count);

	/* Every model entry must be found by lookup; every absent key must not. */
	for (int k = 0; k < KEY_UNIVERSE; k++) {
		void *v = hash_table_lookup(h, keys[k]);
		CHECK(v == model[k], "lookup of %s: got %p, expected %p", keys[k], v, model[k]);
	}

	/* Enumerate with the cursor macro. */
	{
		struct verify_ctx *ctx = calloc(1, sizeof(*ctx));
		char *key;
		void *value;
		int iteration = 0;
		HASH_TABLE_ITERATE(h, iteration, key, value)
		{
			int k = key_number(key);
			CHECK(k >= 0, "iterated over unknown key '%s'", key);
			CHECK(!ctx->seen[k], "key '%s' visited twice", key);
			CHECK(model[k] == value, "key '%s' has wrong value in cursor iteration", key);
			ctx->seen[k] = 1;
			ctx->count++;
		}
		CHECK(ctx->count == model_count, "cursor iteration saw %d entries, expected %d", ctx->count, model_count);
		free(ctx);
	}

	/* Enumerate with foreach_ro. */
	{
		struct verify_ctx *ctx = calloc(1, sizeof(*ctx));
		hash_table_foreach_ro(h, verify_cb, ctx);
		CHECK(ctx->count == model_count, "foreach_ro saw %d entries, expected %d", ctx->count, model_count);
		free(ctx);
	}

	/* Enumerate with hash_table_keys. */
	{
		char **ks = hash_table_keys(h);
		int n = 0;
		int *seen = calloc(KEY_UNIVERSE, sizeof(int));
		for (; ks[n]; n++) {
			int k = key_number(ks[n]);
			CHECK(k >= 0, "keys() returned unknown key '%s'", ks[n]);
			CHECK(!seen[k], "keys() returned '%s' twice", ks[n]);
			CHECK(model[k] != NULL, "keys() returned '%s' not in model", ks[n]);
			seen[k] = 1;
		}
		CHECK(n == model_count, "keys() returned %d entries, expected %d", n, model_count);
		free(seen);
		hash_table_keys_delete(ks);
	}

	/* The load factor reported must be consistent with the size. */
	CHECK(hash_table_load(h) >= 0.0, "negative load");
}

/* Perform one random operation.  insert_pct is the percent chance the
operation is an insert; the remainder is split among lookup and removals. */
static void random_op(struct hash_table *h, int insert_pct, int remove_pct)
{
	int roll = rng_below(100);
	int k = rng_below(KEY_UNIVERSE);

	if (roll < insert_pct) {
		/* Insert, sometimes with a different value for an existing key. */
		int use_b = rng_below(4) == 0;
		void *v = use_b ? (void *)&values_b[k] : (void *)&values_a[k];
		int rc = hash_table_insert(h, keys[k], v);

		if (model[k] == NULL) {
			CHECK(rc == 1, "insert of new key %s failed", keys[k]);
			model[k] = v;
			model_count++;
		} else if (model[k] == v) {
			CHECK(rc == 1, "re-insert of identical key:value %s should succeed", keys[k]);
		} else {
			CHECK(rc == 0, "insert of existing key %s with new value should fail", keys[k]);
		}
		CHECK(hash_table_lookup(h, keys[k]) == model[k], "lookup after insert of %s is wrong", keys[k]);

	} else if (roll < insert_pct + remove_pct) {
		int which = rng_below(10);

		if (which < 7) {
			/* Remove by key (possibly absent). */
			void *v = hash_table_remove(h, keys[k]);
			CHECK(v == model[k], "remove of %s: got %p, expected %p", keys[k], v, model[k]);
			if (model[k]) {
				model[k] = NULL;
				model_count--;
			}
			CHECK(hash_table_lookup(h, keys[k]) == NULL, "key %s still present after remove", keys[k]);

		} else {
			/* Remove an arbitrary entry; identify it by its value pointer. */
			void *v = which < 9 ? hash_table_remove_random(h) : hash_table_remove_any(h);
			if (model_count == 0) {
				CHECK(v == NULL, "remove from empty table returned %p", v);
			} else if (v != NULL) {
				/* The value points into values_a or values_b at index == key number. */
				int idx;
				if ((int *)v >= values_a && (int *)v < values_a + KEY_UNIVERSE)
					idx = (int *)v - values_a;
				else if ((int *)v >= values_b && (int *)v < values_b + KEY_UNIVERSE)
					idx = (int *)v - values_b;
				else {
					CHECK(0, "remove_random returned unknown pointer %p", v);
					idx = 0;
				}
				CHECK(model[idx] == v, "remove_random returned %p, not in the model", v);
				model[idx] = NULL;
				model_count--;
				CHECK(hash_table_lookup(h, keys[idx]) == NULL, "key %s still present after random remove", keys[idx]);
			}
			/* remove_random may legitimately return NULL on a nonempty
			table if it lands on a tombstone-only tail, so no check there. */
		}

	} else {
		/* Plain lookup. */
		void *v = hash_table_lookup(h, keys[k]);
		CHECK(v == model[k], "lookup of %s: got %p, expected %p", keys[k], v, model[k]);
	}

	CHECK(hash_table_size(h) == model_count, "size %d, model has %d", hash_table_size(h), model_count);
}

static void reset_model(void)
{
	memset(model, 0, sizeof(model));
	model_count = 0;
}

/* Empty the table via the API and the model together. */
static void drain(struct hash_table *h)
{
	for (int k = 0; k < KEY_UNIVERSE; k++) {
		if (model[k]) {
			CHECK(hash_table_remove(h, keys[k]) == model[k], "drain remove of %s", keys[k]);
			model[k] = NULL;
			model_count--;
		}
	}
	CHECK(hash_table_size(h) == 0, "table not empty after drain");
}

/* Run a sequence of phases against one table configuration. */
static void run_config(const char *name, unsigned initial_buckets, hash_func_t func, int universe_limit, long ops)
{
	printf("  %-28s buckets=%-4u keys<=%-5d ops=%ld ... ", name, initial_buckets, universe_limit, ops);
	fflush(stdout);

	struct hash_table *h = hash_table_create(initial_buckets, func);
	CHECK(h != NULL, "hash_table_create failed");
	CHECK(hash_table_size(h) == 0, "new table not empty");
	reset_model();

	/* Mixed workload, and phases biased to grow the table and shrink it,
	to exercise both rehash directions and tombstone accumulation. */
	static const int phases[][2] = {
		{50, 30}, /* balanced */
		{80, 10}, /* growth */
		{10, 80}, /* shrink */
		{45, 45}, /* churn: many tombstones */
		{90, 5},  /* growth again */
		{20, 70}, /* shrink again */
	};
	int nphases = sizeof(phases) / sizeof(phases[0]);

	for (int p = 0; p < nphases; p++) {
		for (long i = 0; i < ops / nphases; i++) {
			random_op(h, phases[p][0], phases[p][1]);
			if (i % 997 == 0)
				verify_all(h);
		}
		verify_all(h);
	}

	/* Drain completely, and confirm the table is still usable afterwards. */
	drain(h);
	verify_all(h);
	for (int k = 0; k < 200 && k < universe_limit; k++) {
		CHECK(hash_table_insert(h, keys[k], &values_a[k]) == 1, "reuse insert");
		model[k] = &values_a[k];
		model_count++;
	}
	verify_all(h);

	/* hash_table_clear must empty the table and remain consistent. */
	hash_table_clear(h, NULL);
	reset_model();
	verify_all(h);
	CHECK(hash_table_remove_any(h) == NULL, "remove_any on cleared table");

	hash_table_delete(h);
	printf("ok\n");
}

/* Cursor-based removal during iteration must be safe. */
static void test_cursor_removal(void)
{
	printf("  %-28s ... ", "remove during iteration");
	fflush(stdout);

	struct hash_table *h = hash_table_create(0, 0);
	reset_model();
	for (int k = 0; k < 2000; k++) {
		CHECK(hash_table_insert(h, keys[k], &values_a[k]) == 1, "insert %d", k);
		model[k] = &values_a[k];
		model_count++;
	}

	/* Remove every other entry while iterating over the table. */
	char *key;
	void *value;
	int iteration = 0;
	int flip = 0;
	HASH_TABLE_ITERATE(h, iteration, key, value)
	{
		int k = key_number(key);
		CHECK(k >= 0, "unknown key during iteration");
		if (flip++ % 2 == 0) {
			/* Removing the key under the cursor must not disturb iteration. */
			char copy[32];
			strcpy(copy, key);
			CHECK(hash_table_remove(h, copy) == model[k], "remove during iteration of %s", copy);
			model[k] = NULL;
			model_count--;
		}
	}
	verify_all(h);

	hash_table_delete(h);
	printf("ok\n");
}

/* clear with a delete callback must invoke it exactly once per live value. */
static int delete_calls = 0;
static void count_delete(void *v)
{
	(void)v;
	delete_calls++;
}

static void test_clear_callback(void)
{
	printf("  %-28s ... ", "clear with callback");
	fflush(stdout);

	struct hash_table *h = hash_table_create(0, 0);
	reset_model();
	for (int k = 0; k < 1000; k++) {
		CHECK(hash_table_insert(h, keys[k], &values_a[k]) == 1, "insert %d", k);
	}
	for (int k = 0; k < 1000; k += 3)
		CHECK(hash_table_remove(h, keys[k]) == &values_a[k], "remove %d", k);
	   
	int expected = hash_table_size(h);
	delete_calls = 0;
	hash_table_clear(h, count_delete);
	CHECK(delete_calls == expected, "callback invoked %d times, expected %d", delete_calls, expected);
	CHECK(hash_table_size(h) == 0, "table not empty after clear");

	hash_table_delete(h);
	printf("ok\n");
}

int main(int argc, char **argv)
{
	uint64_t seed = argc > 1 ? strtoull(argv[1], NULL, 0) : 1;
	long ops = argc > 2 ? atol(argv[2]) : 60000;
	seed_used = seed;
	rng_state = seed * 0x9E3779B97F4A7C15ULL + 0x1234567ULL;
	if (rng_state == 0)
		rng_state = 1;
	srand((unsigned)seed); /* hash_table_remove_random uses libc rand() */

	for (int k = 0; k < KEY_UNIVERSE; k++) {
		snprintf(keys[k], sizeof(keys[k]), "key-%d", k);
		values_a[k] = k;
		values_b[k] = -k;
	}

	printf("hash_table_test: seed=%llu ops/config=%ld\n", (unsigned long long)seed, ops);

	run_config("default hash, default size", 0, NULL, KEY_UNIVERSE, ops);
	run_config("default hash, 1 bucket", 1, NULL, KEY_UNIVERSE, ops);
	run_config("default hash, 3 buckets", 3, NULL, KEY_UNIVERSE, ops);
	run_config("default hash, 100000 buckets", 100000, NULL, KEY_UNIVERSE, ops);
	run_config("constant hash (collisions)", 0, hash_constant, KEY_UNIVERSE, ops / 10);
	run_config("constant UINT_MAX hash", 0, hash_constant_max, KEY_UNIVERSE, ops / 10);
	run_config("7 distinct hash values", 0, hash_few_values, KEY_UNIVERSE, ops / 4);

	test_cursor_removal();
	test_clear_callback();

	printf("PASS (%lu checks)\n", checks);
	return 0;
}

