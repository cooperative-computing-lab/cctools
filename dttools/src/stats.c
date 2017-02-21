/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <inttypes.h>
#include "stats.h"
#include "hash_table.h"
#include "xxmalloc.h"

static struct hash_table *stats = NULL;
static int stats_enabled = 0;

typedef enum {
	STATS_INT,
	STATS_LOG,
} stats_type_t;

typedef struct {
	stats_type_t type;
	union {
		int64_t value;
		unsigned buckets[64];
	} v;
} stats_t;

static void stats_init () {
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

void stats_log (const char *name, uint64_t value) {
	if (!stats_enabled) return;
	stats_t *s = stats_touch(name, STATS_LOG);
	++s->v.buckets[log2b(value)];
}

void stats_print_buffer (buffer_t *b) {
	if (!stats_enabled) return;
	assert(b);
	char *k;
	stats_t *s;
	stats_init();
	hash_table_firstkey(stats);
	while (hash_table_nextkey(stats, &k, (void **) &s)) {
		switch (s->type) {
		case STATS_INT:
			buffer_printf(b, "%s\t%" PRIi64 "\n", k, s->v.value);
			break;
		case STATS_LOG:
			buffer_putstring(b, k);
			for (size_t i = 0; i < 64; i++) {
				buffer_printf(b, "\t%u", s->v.buckets[i]);
			}
			buffer_putstring(b, "\n");
			break;
		}
	}
}

void stats_print_stream (FILE *file) {
	assert(file);
	buffer_t buffer;
	buffer_init(&buffer);
	stats_print_buffer(&buffer);
	fprintf(file, "%s", buffer_tostring(&buffer));
	buffer_free(&buffer);
}

char *stats_print_string () {
	buffer_t buffer;
	char *str;
	buffer_init(&buffer);
	stats_print_buffer(&buffer);
	buffer_dup(&buffer,&str);
	buffer_free(&buffer);
	return str;
}

/* vim: set noexpandtab tabstop=4: */
