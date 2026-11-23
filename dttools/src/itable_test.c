/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
Randomized correctness test for itable.

A simple reference model (a flat array indexed by key number) is kept in
lock step with the itable while random inserts, lookups, removes,
remove_random, and remove_any operations are performed.  After every
operation the result is compared against the model, and periodically the
entire table contents are verified through every available enumeration
interface (ITABLE_ITERATE and itable_foreach_ro).

Several key families (dense, strided, near UINT64_MAX, high-bit) are used
so that both the probe sequences and the modulus arithmetic are exercised
at 64-bit widths.

Additional deterministic tests cover the safe-cursor behavior: removal
during iteration, removal of not-yet-visited entries, nested iteration,
leaving an iteration early with break, and the rule that insertion is
forbidden while a cursor is active.

Usage: itable_test [seed] [operations-per-config]

The same seed always produces the same sequence of operations, so any
failure can be reproduced.  Exit status is zero on success.
*/

#include "debug.h"
#include "itable.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* Defined in itable.c, but not (yet) declared in itable.h. */
void *itable_remove_random(struct itable *table);

#define KEY_UNIVERSE 16384

/* Distinct objects used as values.  The model stores pointers into these. */
static int values_a[KEY_UNIVERSE];
static int values_b[KEY_UNIVERSE];

/* Reference model: model[k] is the expected value for key number k, or NULL. */
static void *model[KEY_UNIVERSE];
static int model_count = 0;

static uint64_t rng_state;
static uint64_t seed_used = 0;
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

/*
Key families.  key_of() maps a key number to the actual 64-bit key, and
key_index() inverts it, returning -1 for any key outside the family.
*/

static int key_mode = 0;

static UINT64_T key_of(int k)
{
	switch (key_mode) {
	case 0:
		return (UINT64_T)k;
	case 1:
		return (UINT64_T)k * 127; /* collides heavily at the default table size */
	case 2:
		return UINT64_MAX - (UINT64_T)k;
	default:
		return ((UINT64_T)k << 40) | 5;
	}
}

static int key_index(UINT64_T key)
{
	UINT64_T k;
	switch (key_mode) {
	case 0:
		k = key;
		break;
	case 1:
		if (key % 127)
			return -1;
		k = key / 127;
		break;
	case 2:
		k = UINT64_MAX - key;
		break;
	default:
		if ((key & 0xFFFFFFFFFFULL) != 5)
			return -1;
		k = key >> 40;
		break;
	}
	return k < KEY_UNIVERSE ? (int)k : -1;
}

/* Which key number does this value pointer belong to?  Returns -1 if none. */
static int value_index(void *v)
{
	if ((int *)v >= values_a && (int *)v < values_a + KEY_UNIVERSE)
		return (int *)v - values_a;
	if ((int *)v >= values_b && (int *)v < values_b + KEY_UNIVERSE)
		return (int *)v - values_b;
	return -1;
}

static void reset_model(void)
{
	memset(model, 0, sizeof(model));
	model_count = 0;
}

/* Context for the foreach_ro verification. */
struct verify_ctx {
	int seen[KEY_UNIVERSE];
	int count;
};

static void verify_cb(UINT64_T key, void *value, void *arg)
{
	struct verify_ctx *ctx = arg;
	int k = key_index(key);
	CHECK(k >= 0, "foreach_ro visited unknown key %llu", (unsigned long long)key);
	CHECK(!ctx->seen[k], "foreach_ro visited key %d twice", k);
	CHECK(model[k] != NULL, "key %d present in table but not in model", k);
	CHECK(model[k] == value, "key %d has wrong value in foreach_ro", k);
	ctx->seen[k] = 1;
	ctx->count++;
}

/* Compare the full contents of the table against the model. */
static void verify_all(struct itable *h)
{
	CHECK(itable_size(h) == model_count, "size %d, model has %d", itable_size(h), model_count);

	/* Every model entry must be found by lookup; every absent key must not. */
	for (int k = 0; k < KEY_UNIVERSE; k++) {
		void *v = itable_lookup(h, key_of(k));
		CHECK(v == model[k], "lookup of key %d: got %p, expected %p", k, v, model[k]);
	}
	CHECK(itable_lookup(h, 0xDEADBEEFULL) == NULL, "lookup of absent key returned a value");
	CHECK(itable_lookup(h, UINT64_MAX / 2) == NULL, "lookup of absent large key returned a value");

	/* Enumerate with the cursor macro. */
	{
		struct verify_ctx *ctx = calloc(1, sizeof(*ctx));
		UINT64_T key;
		void *value;
		int iteration = 0;
		ITABLE_ITERATE(h, iteration, key, value)
		{
			int k = key_index(key);
			CHECK(k >= 0, "iterated over unknown key %llu", (unsigned long long)key);
			CHECK(!ctx->seen[k], "key %d visited twice", k);
			CHECK(model[k] == value, "key %d has wrong value in cursor iteration", k);
			ctx->seen[k] = 1;
			ctx->count++;
		}
		CHECK(ctx->count == model_count, "cursor iteration saw %d entries, expected %d", ctx->count, model_count);
		free(ctx);
	}

	/* Enumerate with foreach_ro. */
	{
		struct verify_ctx *ctx = calloc(1, sizeof(*ctx));
		itable_foreach_ro(h, verify_cb, ctx);
		CHECK(ctx->count == model_count, "foreach_ro saw %d entries, expected %d", ctx->count, model_count);
		free(ctx);
	}
}

/*
Perform one random operation.  insert_pct is the percent chance the
operation is an insert; remove_pct the chance it is a removal; the
remainder are lookups.  Only key numbers below universe_limit are used.
*/
static void random_op(struct itable *h, int insert_pct, int remove_pct, int universe_limit)
{
	int roll = rng_below(100);
	int k = rng_below(universe_limit);

	if (roll < insert_pct) {
		/* Insert, sometimes with a different value for an existing key. */
		int use_b = rng_below(4) == 0;
		void *v = use_b ? (void *)&values_b[k] : (void *)&values_a[k];
		int rc = itable_insert(h, key_of(k), v);

		if (model[k] == NULL) {
			CHECK(rc == 1, "insert of new key %d failed", k);
			model_count++;
		} else if (model[k] == v) {
			CHECK(rc == 1, "re-insert of identical key:value %d should succeed", k);
		} else {
			CHECK(rc == 1, "insert of existing key %d with new value should succeed with warning", k);
		}

		/* Any way we got here, the model should get the new value. */
		model[k] = v;

		CHECK(itable_lookup(h, key_of(k)) == model[k], "lookup after insert of key %d is wrong", k);

	} else if (roll < insert_pct + remove_pct) {
		int which = rng_below(10);

		if (which < 7) {
			/* Remove by key (possibly absent). */
			void *v = itable_remove(h, key_of(k));
			CHECK(v == model[k], "remove of key %d: got %p, expected %p", k, v, model[k]);
			if (model[k]) {
				model[k] = NULL;
				model_count--;
			}
			CHECK(itable_lookup(h, key_of(k)) == NULL, "key %d still present after remove", k);

		} else {
			/* Remove an arbitrary entry; identify it by its value pointer. */
			void *v = which < 9 ? itable_remove_random(h) : itable_remove_any(h);
			if (model_count == 0) {
				CHECK(v == NULL, "remove from empty table returned %p", v);
			} else {
				/* Both functions must find something in a nonempty table. */
				CHECK(v != NULL, "arbitrary remove returned NULL from a table of %d entries", model_count);
				int idx = value_index(v);
				CHECK(idx >= 0, "arbitrary remove returned unknown pointer %p", v);
				CHECK(model[idx] == v, "arbitrary remove returned %p, not in the model", v);
				model[idx] = NULL;
				model_count--;
				CHECK(itable_lookup(h, key_of(idx)) == NULL, "key %d still present after arbitrary remove", idx);
			}
		}

	} else {
		/* Plain lookup. */
		void *v = itable_lookup(h, key_of(k));
		CHECK(v == model[k], "lookup of key %d: got %p, expected %p", k, v, model[k]);
	}

	CHECK(itable_size(h) == model_count, "size %d, model has %d", itable_size(h), model_count);
}

/* Empty the table via the API and the model together. */
static void drain(struct itable *h)
{
	for (int k = 0; k < KEY_UNIVERSE; k++) {
		if (model[k]) {
			CHECK(itable_remove(h, key_of(k)) == model[k], "drain remove of key %d", k);
			model[k] = NULL;
			model_count--;
		}
	}
	CHECK(itable_size(h) == 0, "table not empty after drain");
}

/* Run a sequence of phases against one table configuration. */
static void run_config(const char *name, int mode, unsigned initial_buckets, int universe_limit, long ops)
{
	printf("  %-30s buckets=%-6u keys<=%-5d ops=%ld ... ", name, initial_buckets, universe_limit, ops);
	fflush(stdout);

	key_mode = mode;
	struct itable *h = itable_create(initial_buckets);
	CHECK(h != NULL, "itable_create failed");
	CHECK(itable_size(h) == 0, "new table not empty");
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
			random_op(h, phases[p][0], phases[p][1], universe_limit);
			if (i % 997 == 0)
				verify_all(h);
		}
		verify_all(h);
	}

	/* Drain completely, and confirm the table is still usable afterwards. */
	drain(h);
	verify_all(h);
	for (int k = 0; k < 200 && k < universe_limit; k++) {
		CHECK(itable_insert(h, key_of(k), &values_a[k]) == 1, "reuse insert");
		model[k] = &values_a[k];
		model_count++;
	}
	verify_all(h);

	/* itable_clear must empty the table and remain consistent. */
	itable_clear(h, NULL);
	reset_model();
	verify_all(h);
	CHECK(itable_remove_any(h) == NULL, "remove_any on cleared table");

	itable_delete(h);
	printf("ok\n");
}

/* Insert key numbers [0,n) into a fresh table and the model. */
static struct itable *make_table(int n)
{
	struct itable *h = itable_create(0);
	reset_model();
	for (int k = 0; k < n; k++) {
		CHECK(itable_insert(h, key_of(k), &values_a[k]) == 1, "insert %d", k);
		model[k] = &values_a[k];
		model_count++;
	}
	return h;
}

/* Removing the entry under the cursor during iteration must be safe. */
static void test_cursor_removal(void)
{
	printf("  %-30s ... ", "remove during iteration");
	fflush(stdout);

	key_mode = 0;
	struct itable *h = make_table(2000);

	/* Remove every other entry while iterating over the table. */
	UINT64_T key;
	void *value;
	int iteration = 0;
	int flip = 0;
	int visited = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		int k = key_index(key);
		CHECK(k >= 0, "unknown key during iteration");
		visited++;
		if (flip++ % 2 == 0) {
			CHECK(itable_remove(h, key) == model[k], "remove during iteration of key %d", k);
			model[k] = NULL;
			model_count--;
		}
	}
	CHECK(visited == 2000, "visited %d entries, expected 2000", visited);
	verify_all(h);

	/* Now remove everything during one iteration; the table must end empty and usable. */
	iteration = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		int k = key_index(key);
		CHECK(itable_remove(h, key) == model[k], "remove-all during iteration of key %d", k);
		model[k] = NULL;
		model_count--;
	}
	CHECK(itable_size(h) == 0, "table not empty after removing everything during iteration");
	verify_all(h);
	for (int k = 0; k < 500; k++) {
		CHECK(itable_insert(h, key_of(k), &values_a[k]) == 1, "insert after mass removal");
		model[k] = &values_a[k];
		model_count++;
	}
	verify_all(h);

	itable_delete(h);
	printf("ok\n");
}

/*
Entries removed ahead of the cursor must not be visited, and every entry
that is never removed must be visited exactly once.
*/
static void test_remove_ahead(void)
{
	printf("  %-30s ... ", "remove ahead of cursor");
	fflush(stdout);

	const int n = 500;
	key_mode = 0;
	struct itable *h = make_table(n);
	static char visited[KEY_UNIVERSE];
	memset(visited, 0, sizeof(visited));

	UINT64_T key;
	void *value;
	int iteration = 0;
	int visits = 0;
	int removed_ahead = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		int k = key_index(key);
		CHECK(k >= 0, "unknown key during iteration");
		CHECK(!visited[k], "key %d visited twice", k);
		CHECK(model[k] != NULL, "visited key %d after it was removed", k);
		CHECK(value == model[k], "key %d has wrong value", k);
		visited[k] = 1;

		/* Remove some entry that has not been visited yet. */
		if (visits % 3 == 0) {
			int t = rng_below(n);
			if (t != k && !visited[t] && model[t]) {
				CHECK(itable_remove(h, key_of(t)) == model[t], "remove ahead of key %d", t);
				model[t] = NULL;
				model_count--;
				removed_ahead++;
			}
		}
		/* And sometimes remove the entry under the cursor. */
		if (visits % 5 == 0) {
			CHECK(itable_remove(h, key) == model[k], "remove current key %d", k);
			model[k] = NULL;
			model_count--;
		}
		visits++;
	}
	CHECK(removed_ahead > 0, "test never removed an entry ahead of the cursor");
	for (int k = 0; k < n; k++)
		CHECK(model[k] == NULL || visited[k], "surviving key %d was never visited", k);
	verify_all(h);

	itable_delete(h);
	printf("ok\n");
}

/* Verify safety of nested iteration with removal in the middle. */
static void test_nested_removal(void)
{
	printf("  %-30s ... ", "remove in nested iteration");
	fflush(stdout);

	const int n = 100;
	key_mode = 0;
	struct itable *h = make_table(n);

	/*
	Predict the outcome with a simple simulation.  Keys are below the bucket
	count, so iteration order is ascending key order.
	*/
	int alive[100];
	int expected_total = 0;
	for (int k = 0; k < n; k++)
		alive[k] = 1;
	for (int o = 0; o < n; o++) {
		if (!alive[o])
			continue;
		for (int i = 0; i < n; i++) {
			if (!alive[i])
				continue;
			if (expected_total % 9 == 0)
				alive[i] = 0;
			expected_total++;
		}
	}

	int total_count = 0;

	UINT64_T key;
	void *value;
	int iteration = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		UINT64_T inner_key;
		void *inner_value;
		int inner_iteration = 0;
		ITABLE_ITERATE(h, inner_iteration, inner_key, inner_value)
		{
			if (total_count % 9 == 0) {
				int k = key_index(inner_key);
				CHECK(itable_remove(h, inner_key) == model[k], "nested remove of key %d", k);
				model[k] = NULL;
				model_count--;
			}
			total_count++;
		}
	}
	CHECK(total_count == expected_total, "total_count %d, expected %d", total_count, expected_total);
	for (int k = 0; k < n; k++)
		CHECK((model[k] != NULL) == alive[k], "key %d liveness differs from simulation", k);
	verify_all(h);

	itable_delete(h);
	printf("ok\n");
}

/*
Leaving an iteration early with break must release the cursor, or the
next insert would be refused.  Do it from both a single and a nested loop.
*/
static void test_break_releases_cursor(void)
{
	printf("  %-30s ... ", "break releases cursor");
	fflush(stdout);

	key_mode = 0;
	struct itable *h = make_table(50);

	UINT64_T key;
	void *value;
	int iteration = 0;
	int n = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		if (++n == 3)
			break;
	}
	CHECK(n == 3, "break after %d entries", n);

	/* If the cursor leaked, this insert would be fatal. */
	CHECK(itable_insert(h, key_of(1000), &values_a[1000]) == 1, "insert after break");
	model[1000] = &values_a[1000];
	model_count++;

	UINT64_T key2;
	void *value2;
	int iteration2 = 0;
	int outer = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		outer++;
		ITABLE_ITERATE(h, iteration2, key2, value2)
		{
			break;
		}
		if (outer == 5)
			break;
	}
	CHECK(outer == 5, "outer loop ran %d times", outer);
	CHECK(itable_insert(h, key_of(1001), &values_a[1001]) == 1, "insert after nested break");
	model[1001] = &values_a[1001];
	model_count++;

	verify_all(h);
	itable_delete(h);
	printf("ok\n");
}

/* foreach_ro may be called while another iteration is in progress. */
static void test_foreach_during_iteration(void)
{
	printf("  %-30s ... ", "foreach_ro inside iteration");
	fflush(stdout);

	key_mode = 0;
	struct itable *h = make_table(50);

	UINT64_T key;
	void *value;
	int iteration = 0;
	int outer = 0;
	ITABLE_ITERATE(h, iteration, key, value)
	{
		struct verify_ctx *ctx = calloc(1, sizeof(*ctx));
		itable_foreach_ro(h, verify_cb, ctx);
		CHECK(ctx->count == 50, "foreach_ro saw %d entries, expected 50", ctx->count);
		free(ctx);
		outer++;
	}
	CHECK(outer == 50, "outer iteration saw %d entries, expected 50", outer);

	itable_delete(h);
	printf("ok\n");
}

/* Exercise the raw cursor interface, including a removal under the cursor. */
static void test_cursor_api(void)
{
	printf("  %-30s ... ", "cursor api");
	fflush(stdout);

	key_mode = 0;
	struct itable *h = make_table(10);

	struct itable_cursor *c = itable_cursor_create(h);
	UINT64_T key = 0;
	void *value = NULL;

	CHECK(itable_cursor_get(c, &key, &value) == 1, "new cursor not positioned on first entry");
	int first = key_index(key);
	CHECK(first >= 0 && value == model[first], "cursor returned wrong first entry");

	/* Remove the entry under the cursor: get fails there until the cursor moves. */
	CHECK(itable_remove(h, key) == model[first], "remove entry under cursor");
	model[first] = NULL;
	model_count--;
	CHECK(itable_cursor_get(c, &key, &value) == 0, "get succeeded on a removed entry");

	/* Advancing lands on a different, live entry, and the rest are all reachable. */
	int remaining = 0;
	CHECK(itable_cursor_next(c) == 1, "cursor_next found nothing after the first entry");
	do {
		CHECK(itable_cursor_get(c, &key, &value) == 1, "get failed on a valid position");
		int k = key_index(key);
		CHECK(k >= 0 && k != first && model[k] == value, "cursor returned wrong entry");
		remaining++;
	} while (itable_cursor_next(c));
	CHECK(remaining == 9, "cursor reached %d remaining entries, expected 9", remaining);

	/* Past the end, the cursor stays invalid. */
	CHECK(itable_cursor_get(c, &key, &value) == 0, "get succeeded past the end");
	CHECK(itable_cursor_next(c) == 0, "next succeeded past the end");

	/* Seeking to zero restarts the walk. */
	itable_cursor_seek(c, 0);
	int again = 0;
	while (itable_cursor_get(c, &key, &value)) {
		again++;
		itable_cursor_next(c);
	}
	CHECK(again == 9, "walk after seek(0) saw %d entries, expected 9", again);
	itable_cursor_delete(c);

	verify_all(h);
	itable_delete(h);
	printf("ok\n");
}

/* Inserting while a cursor is active must be refused (fatal), not silently corrupt. */
static void test_insert_with_cursor_is_fatal(void)
{
	printf("  %-30s ... ", "insert with active cursor");
	fflush(stdout);
	fflush(stderr);

	pid_t pid = fork();
	CHECK(pid >= 0, "fork failed");
	if (pid == 0) {
		/* Child: silence the expected fatal message and try the forbidden insert. */
		if (!freopen("/dev/null", "w", stderr))
			_exit(78);
		struct itable *h = itable_create(0);
		itable_insert(h, 1, &values_a[1]);
		struct itable_cursor *c = itable_cursor_create(h);
		itable_insert(h, 2, &values_a[2]);
		/* Only reached if the insert was (wrongly) permitted. */
		itable_cursor_delete(c);
		_exit(77);
	}

	int status = 0;
	CHECK(waitpid(pid, &status, 0) == pid, "waitpid failed");
	CHECK(!(WIFEXITED(status) && WEXITSTATUS(status) == 77), "insert with an active cursor was permitted");
	CHECK(!(WIFEXITED(status) && WEXITSTATUS(status) == 0), "child exited cleanly after forbidden insert");
	CHECK(!(WIFEXITED(status) && WEXITSTATUS(status) == 78), "child could not redirect stderr");
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
	printf("  %-30s ... ", "clear with callback");
	fflush(stdout);

	key_mode = 0;
	struct itable *h = itable_create(0);
	reset_model();
	for (int k = 0; k < 1000; k++) {
		CHECK(itable_insert(h, key_of(k), &values_a[k]) == 1, "insert %d", k);
	}
	for (int k = 0; k < 1000; k += 3)
		CHECK(itable_remove(h, key_of(k)) == &values_a[k], "remove %d", k);

	int expected = itable_size(h);
	delete_calls = 0;
	itable_clear(h, count_delete);
	CHECK(delete_calls == expected, "callback invoked %d times, expected %d", delete_calls, expected);
	CHECK(itable_size(h) == 0, "table not empty after clear");

	itable_delete(h);
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
	srand((unsigned)seed); /* itable_remove_random uses libc rand() */

	for (int k = 0; k < KEY_UNIVERSE; k++) {
		values_a[k] = k;
		values_b[k] = -k;
	}

	printf("itable_test: seed=%llu ops/config=%ld\n", (unsigned long long)seed, ops);

	printf("NOTE: 9 duplicate key warnings and an '[additional warnings suppressed]' line are EXPECTED to follow here.\n");

	run_config("dense keys, default size", 0, 0, KEY_UNIVERSE, ops);
	run_config("dense keys, 1 bucket", 0, 1, KEY_UNIVERSE, ops);
	run_config("dense keys, 2 buckets", 0, 2, KEY_UNIVERSE, ops);
	run_config("dense keys, 100000 buckets", 0, 100000, KEY_UNIVERSE, ops);
	run_config("small universe, tombstone churn", 0, 1, 64, ops);
	run_config("stride-127 keys (collisions)", 1, 0, KEY_UNIVERSE, ops / 2);
	run_config("keys near UINT64_MAX", 2, 0, KEY_UNIVERSE, ops / 2);
	run_config("high-bit keys", 3, 0, KEY_UNIVERSE, ops / 2);

	test_cursor_api();
	test_cursor_removal();
	test_remove_ahead();
	test_nested_removal();
	test_break_releases_cursor();
	test_foreach_during_iteration();
	test_insert_with_cursor_is_fatal();
	test_clear_callback();

	printf("PASS (%lu checks)\n", checks);
	return 0;
}
