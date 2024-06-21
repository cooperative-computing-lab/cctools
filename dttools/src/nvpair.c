/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
#include "debug.h"
#include "nvpair_private.h"

#include "buffer.h"
#include "hash_table.h"
#include "macros.h"
#include "stringtools.h"
#include "timestamp.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>

#define NVPAIR_LINE_MAX 1024

struct nvpair *nvpair_create()
{
	struct nvpair *n;
	n = xxmalloc(sizeof(*n));
	n->table = hash_table_create(7, hash_string);
	return n;
}

void nvpair_delete(struct nvpair *n)
{
	char *key;
	void *value;

	if (!n)
		return;

	hash_table_firstkey(n->table);
	while (hash_table_nextkey(n->table, &key, &value)) {
		hash_table_remove(n->table, key);
		free(value);
	}
	hash_table_delete(n->table);
	free(n);
}

void nvpair_parse(struct nvpair *n, const char *data)
{
	char *text = xxstrdup(data);
	char *name, *value;

	name = strtok(text, " ");
	while (name) {
		value = strtok(0, "\n");
		if (value) {
			nvpair_insert_string(n, name, value);
		} else {
			break;
		}
		name = strtok(0, " ");
	}

	free(text);
}

int nvpair_parse_stream(struct nvpair *n, FILE *stream)
{
	int num_pairs = 0;
	char line[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];
	char key[NVPAIR_LINE_MAX];
	key[0] = '\0';

	while (fgets(line, sizeof(line), stream)) {
		if (line[0] == '\n') {
			if (strlen(key) == 0) {
				sprintf(key, "%s:%s:%s", nvpair_lookup_string(n, "address"), nvpair_lookup_string(n, "port"), nvpair_lookup_string(n, "name"));
				nvpair_insert_string(n, "key", key);
			}
			if (num_pairs) {
				return num_pairs;
			} else {
				continue;
			}
		}

		if (sscanf(line, "%s %[^\r\n]", name, value) == 2) {
			if (strcmp(name, "key") == 0)
				strcpy(key, value);
			// printf("-----%s,%s\n",name,value);
			nvpair_insert_string(n, name, value);
			num_pairs += 1;
		} else {
			debug(D_DEBUG, "corrupt log data: %s", line);
			// return 0;
		}
	}

	return 0;
}

int nvpair_print(struct nvpair *n, char *text, int length)
{
	char *key;
	void *value;

	int actual;
	int total = 0;

	hash_table_firstkey(n->table);
	while (hash_table_nextkey(n->table, &key, &value) && length > 0) {
		actual = snprintf(text, length, "%s %s\n", key, (char *)value);
		total += actual;
		text += actual;
		length -= actual;
	}
	return total;
}

int nvpair_print_alloc(struct nvpair *n, char **text)
{
	size_t needed;
	char *key;
	void *value;
	buffer_t B;

	buffer_init(&B);
	buffer_abortonfailure(&B, 1);

	hash_table_firstkey(n->table);
	while (hash_table_nextkey(n->table, &key, &value)) {
		buffer_putfstring(&B, "%s %s\n", key, (char *)value);
	}

	buffer_dupl(&B, text, &needed);
	buffer_free(&B);

	return needed;
}

void nvpair_remove(struct nvpair *n, const char *name)
{
	char *old = hash_table_remove(n->table, name);
	if (old)
		free(old);
}

void nvpair_insert_string(struct nvpair *n, const char *name, const char *value)
{
	void *old;
	old = hash_table_remove(n->table, name);
	if (old)
		free(old);
	hash_table_insert(n->table, name, xxstrdup(value));
}

void nvpair_insert_integer(struct nvpair *n, const char *name, INT64_T ivalue)
{
	char value[256];
	sprintf(value, INT64_FORMAT, ivalue);
	nvpair_insert_string(n, name, (char *)value);
}

void nvpair_insert_float(struct nvpair *n, const char *name, double fvalue)
{
	char value[256];
	sprintf(value, "%lf", fvalue);
	nvpair_insert_string(n, name, (char *)value);
}

const char *nvpair_lookup_string(struct nvpair *n, const char *name)
{
	return hash_table_lookup(n->table, name);
}

INT64_T nvpair_lookup_integer(struct nvpair *n, const char *name)
{
	const char *value;
	value = hash_table_lookup(n->table, name);
	if (value) {
		return atoll(value);
	} else {
		return 0;
	}
}

double nvpair_lookup_float(struct nvpair *n, const char *name)
{
	const char *value;
	value = hash_table_lookup(n->table, name);
	if (value) {
		return atof(value);
	} else {
		return 0;
	}
}

void nvpair_export(struct nvpair *nv)
{
	char *name, *value;
	nvpair_first_item(nv);
	while (nvpair_next_item(nv, &name, &value)) {
		setenv(name, value, 1);
	}
}

void nvpair_first_item(struct nvpair *nv)
{
	hash_table_firstkey(nv->table);
}

int nvpair_next_item(struct nvpair *nv, char **name, char **value)
{
	return hash_table_nextkey(nv->table, name, (void **)value);
}

void nvpair_print_text(struct nvpair *n, FILE *s)
{
	char *key;
	void *value;

	hash_table_firstkey(n->table);
	while (hash_table_nextkey(n->table, &key, &value)) {
		fprintf(s, "%s %s\n", key, (char *)value);
	}
	fprintf(s, "\n");
}
