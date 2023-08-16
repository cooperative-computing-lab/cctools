/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "get_line.h"
#include "hash_table.h"
#include "stringtools.h"

struct chunk {
	long pos;
	long len;
	char *logical_file_name;

	struct chunk_set *parent;

	struct chunk *next;
};

struct chunk_set {
	char *physical_file_name;
	struct chunk *head;
	struct chunk *tail;

	struct hash_table *file_table;
};

static void chunk_set_add_chunk(struct chunk_set *chunk_set, struct chunk *new_chunk)
{
	if(!chunk_set->head) {
		chunk_set->head = new_chunk;
		chunk_set->tail = new_chunk;
	} else {
		chunk_set->tail->next = new_chunk;
		chunk_set->tail = new_chunk;
	}

	hash_table_insert(chunk_set->file_table, new_chunk->logical_file_name, new_chunk);
}

static struct chunk *chunk_create(struct chunk_set *parent, char *logical_name, long pos)
{
	struct chunk *chunk = malloc(sizeof(*chunk));
	if(!chunk)
		return NULL;

	chunk->pos = pos;
	chunk->len = 0;

	chunk->logical_file_name = strdup(logical_name);

	chunk->parent = parent;
	chunk->next = NULL;

	return chunk;
}

struct chunk_set *chunk_parse_file(char *file_name, char *ln_prefix, char *fc_prefix)
{
	if(string_null_or_empty(file_name))
		return NULL;

	if(string_null_or_empty(ln_prefix) && string_null_or_empty(fc_prefix))
		return NULL;

	struct chunk_set *chunk_set = malloc(sizeof(*chunk_set));
	chunk_set->physical_file_name = strdup(file_name);
	chunk_set->head = NULL;
	chunk_set->tail = NULL;
	chunk_set->file_table = hash_table_create(0, NULL);

	struct chunk *new_chunk = NULL;

	int ln_prefix_len = 0;
	if(ln_prefix)
		ln_prefix_len = strlen(ln_prefix);
	int fc_prefix_len = 0;
	if(fc_prefix)
		fc_prefix_len = strlen(fc_prefix);

	FILE *fp = fopen(file_name, "r");
	if (fp == NULL) return NULL;

	long pos = 0;
	char *line;

	while((line = get_line(fp)) != NULL) {
		if(!string_null_or_empty(ln_prefix) && strncmp(line, ln_prefix, ln_prefix_len) == 0) {
			/* finish current chunk; begin new chunk */
			pos = ftell(fp);

			if(new_chunk) {
				new_chunk->len = pos - strlen(line) - new_chunk->pos;

				chunk_set_add_chunk(chunk_set, new_chunk);
			}

			new_chunk = chunk_create(chunk_set, line + ln_prefix_len, pos);
		} else if(fc_prefix && strncmp(line, fc_prefix, fc_prefix_len) == 0) {
			/* add to len? at any rate just continue */
			continue;
		} else if(string_null_or_empty(ln_prefix)) {
			/* finish current chunk; begin new chunk */
			pos = ftell(fp);

			if(new_chunk) {
				new_chunk->len = pos - strlen(line) - new_chunk->pos;

				chunk_set_add_chunk(chunk_set, new_chunk);
			}

			new_chunk = chunk_create(chunk_set, line + ln_prefix_len, pos);
		} else if(new_chunk) {
			/* assert string_null_or_empty(fc_prefix) and we are
			   in the middle of a chunk */

			/* add to len? at any rate just continue */
			continue;
		} else {
			/* we are before all the chunks... just ignore */
			continue;
		}
	}

	if(!chunk_set->head) {
		/* Chunks could not be parsed, so return null */
		free(chunk_set->physical_file_name);
		free(chunk_set);
		return NULL;
	}

	return chunk_set;

}

char *chunk_read(struct chunk_set *chunk_set, const char *file_name, int *size)
{
	struct chunk *the_chunk = (struct chunk *) hash_table_lookup(chunk_set->file_table, file_name);

	FILE *fp = fopen(chunk_set->physical_file_name, "r");
	fseek(fp, the_chunk->pos, SEEK_SET);

	char *content = malloc(the_chunk->len);

	ssize_t amt = fread(content, sizeof(*content), the_chunk->len, fp);

	if(amt != the_chunk->len && ferror(fp)) {
		/* error reading stream */
		free(content);
		fclose(fp);
		return NULL;
	}

	*size = the_chunk->len;
	fclose(fp);
	return content;
}

int chunk_concat(const char *new_name, const char * const *filenames, int num_files, char *ln_prefix, char *fc_prefix)
{
	FILE *old_file, *new_file;
	char *line;

	if(string_null_or_empty(ln_prefix) && string_null_or_empty(fc_prefix))
		return 0;

	new_file = fopen(new_name, "w");
	if(!new_file)
		return 0;

	if(!ln_prefix)
		ln_prefix = "";
	if(!fc_prefix)
		fc_prefix = "";

	int i;
	for(i = 0; i < num_files; ++i) {
		const char *current_file_name = filenames[i];
		old_file = fopen(current_file_name, "r");
		if (old_file == NULL) return 0;

		fprintf(new_file, "%s%s\n", ln_prefix, current_file_name);

		while((line = get_line(old_file)) != NULL)
			fprintf(new_file, "%s%s", fc_prefix, line);

		fclose(old_file);
	}

	fclose(new_file);

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
