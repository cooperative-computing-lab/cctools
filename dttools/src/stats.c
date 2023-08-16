/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include "stats.h"
#include "hash_table.h"
#include "xxmalloc.h"

static struct hash_table *stats = NULL;
static int stats_enabled = 0;

typedef enum {
	STATS_INT,
	STATS_BIN,
} stats_type_t;

typedef struct {
	stats_type_t type;
	union {
		int64_t value;
		unsigned buckets[64];
	} v;
} stats_t;

static void stats_init (void) {
	if (!stats) {
		stats = hash_table_create(0, 0);
	}
}

static stats_t *stats_touch (const char *name, stats_type_t type) {
	assert(name);
	stats_init();
	stats_t *s = hash_table_lookup(stats, name);
	if (s) {
		assert(s->type == type);
	} else {
		s = xxcalloc(1, sizeof(*s));
		s->type = type;
		int rc = hash_table_insert(stats, name, s);
		assert(rc);
	}
	return s;
}

static size_t log2b(uint64_t n) {
	size_t i = 0;
	while (n >>= 1) ++i;
	return i;
}

void stats_enable () {
	stats_enabled = 1;
}

void stats_unset (const char *name) {
	if (!stats_enabled) return;
	assert(name);
	stats_init();
	free(hash_table_remove(stats, name));
}

void stats_set (const char *name, int64_t value) {
	if (!stats_enabled) return;
	stats_t *s = stats_touch(name, STATS_INT);
	s->v.value = value;
}

void stats_inc (const char *name, int64_t offset) {
	if (!stats_enabled) return;
	stats_t *s = stats_touch(name, STATS_INT);
	s->v.value += offset;
}

void stats_bin (const char *name, uint64_t value) {
	if (!stats_enabled) return;
	stats_t *s = stats_touch(name, STATS_BIN);
	++s->v.buckets[log2b(value)];
}

struct jx *stats_get (void) {
	if (!stats_enabled) return jx_null();
	char *k;
	stats_t *s;
	struct jx *out = jx_object(NULL);
	struct jx *log;
	stats_init();
	hash_table_firstkey(stats);
	while (hash_table_nextkey(stats, &k, (void **) &s)) {
		switch (s->type) {
		case STATS_INT:
			jx_insert_integer(out, k, s->v.value);
			break;
		case STATS_BIN:
			log = jx_array(NULL);
			for (size_t i = 0; i < 64; i++) {
				jx_array_append(log, jx_integer(s->v.buckets[i]));
			}
			jx_insert(out, jx_string(k), log);
			break;
		}
	}
	return out;
}

/* vim: set noexpandtab tabstop=8: */
