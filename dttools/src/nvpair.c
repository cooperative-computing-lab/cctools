/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "macros.h"
#include "timestamp.h"

#include <stdlib.h>
#include <string.h>

#define NVPAIR_LINE_MAX 1024

struct nvpair {
	struct hash_table *table;
};

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

	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
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
	while(name) {
		value = strtok(0, "\n");
		if(value) {
			nvpair_insert_string(n, name, value);
		} else {
			break;
		}
		name = strtok(0, " ");
	}

	free(text);
}

int nvpair_parse_stream(struct nvpair *n, FILE * stream)
{
	int got_something = 0;
	char line[NVPAIR_LINE_MAX];
	char name[NVPAIR_LINE_MAX];
	char value[NVPAIR_LINE_MAX];

	while(fgets(line, sizeof(line), stream)) {
		if(line[0] == '\n') {
			if(got_something) {
				return 1;
			} else {
				continue;
			}
		}

		if(sscanf(line, "%s %[^\r\n]", name, value) == 2) {
			nvpair_insert_string(n, name, value);
			got_something = 1;
		} else {
			return 0;
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
	while(hash_table_nextkey(n->table, &key, &value)) {
		actual = snprintf(text, length, "%s %s\n", key, (char *) value);
		total += actual;
		text += actual;
		length -= actual;

	}
	return total;
}

int nvpair_print_alloc(struct nvpair *n, char **text)
{
	int length = 1024;
	int needed;

	*text = malloc(length);
	if(!*text)
		return 0;

	needed = nvpair_print(n, *text, length);
	if(needed >= length) {
		free(*text);
		*text = malloc(needed + 1);
		if(!*text)
			return 0;
		nvpair_print(n, *text, needed + 1);
	}

	return needed;
}

void nvpair_insert_string(struct nvpair *n, const char *name, const char *value)
{
	void *old;
	old = hash_table_remove(n->table, name);
	if(old)
		free(old);
	hash_table_insert(n->table, name, xxstrdup(value));
}

void nvpair_insert_integer(struct nvpair *n, const char *name, INT64_T ivalue)
{
	char value[256];
	sprintf(value, INT64_FORMAT, ivalue);
	nvpair_insert_string(n, name, (char *) value);
}

const char *nvpair_lookup_string(struct nvpair *n, const char *name)
{
	return hash_table_lookup(n->table, name);
}

INT64_T nvpair_lookup_integer(struct nvpair * n, const char *name)
{
	const char *value;
	value = hash_table_lookup(n->table, name);
	if(value) {
		return atoll(value);
	} else {
		return 0;
	}
}

void nvpair_print_text(struct nvpair *n, FILE * s)
{
	char *key;
	void *value;

	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
		fprintf(s, "%s %s\n", key, (char *) value);
	}
	fprintf(s, "\n");
}

void nvpair_print_json(struct nvpair *n, FILE * s)
{
	char *key;
	void *value;

	int i = 0;
	int count = hash_table_size(n->table);

	fprintf(s, "{\n");
	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {

		fprintf(s,"\"%s\":",key);

		if(string_is_integer(value)) {
			fprintf(s,"%s",(char*)value);
		} else {
			fprintf(s,"\"%s\"",(char*)value);
		}		

		i++;
		if(i<count) fprintf(s,",\n");
	}
	fprintf(s, "\n}\n");
}

void nvpair_print_xml(struct nvpair *n, FILE * s)
{
	char *key;
	void *value;

	fprintf(s, "<item>\n");
	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
		fprintf(s, "<%s>%s</%s>\n", key, (char *) value, key);
	}
	fprintf(s, "</item>\n\n");
}

void nvpair_print_old_classads(struct nvpair *n, FILE * s)
{
	char *key;
	void *value;

	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
		fprintf(s, "%s = \"%s\"\n", key, (char *) value);
	}
	fprintf(s, "\n");
}

void nvpair_print_new_classads(struct nvpair *n, FILE * s)
{
	char *key;
	void *value;

	fprintf(s, "[\n");
	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
		fprintf(s, "%s = \"%s\";\n", key, (char *) value);
	}
	fprintf(s, "]\n");
}

#define COLOR_ONE "#aaaaff"
#define COLOR_TWO "#bbbbbb"

static int color_counter = 0;

static const char *align_string(struct nvpair_header *h)
{
	if(h->align == NVPAIR_ALIGN_RIGHT) {
		return "right";
	} else {
		return "left";
	}
}

void nvpair_print_html_solo(struct nvpair *n, FILE * stream)
{
	char *key;
	void *value;

	fprintf(stream, "<table bgcolor=%s>\n", COLOR_TWO);
	fprintf(stream, "<tr bgcolor=%s>\n", COLOR_ONE);

	color_counter = 0;

	hash_table_firstkey(n->table);
	while(hash_table_nextkey(n->table, &key, &value)) {
		fprintf(stream, "<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
		color_counter++;
		fprintf(stream, "<td align=left><b>%s</b>\n", key);
		if(!strcmp(key, "url")) {
			fprintf(stream, "<td align=left><a href=%s>%s</a>\n", (char *) value, (char *) value);
		} else {
			fprintf(stream, "<td align=left>%s\n", (char *) value);
		}
	}
	fprintf(stream, "</table>\n");
}

void nvpair_print_html_header(FILE * s, struct nvpair_header *h)
{
	fprintf(s, "<table bgcolor=%s>\n", COLOR_TWO);
	fprintf(s, "<tr bgcolor=%s>\n", COLOR_ONE);
	while(h->name) {
		fprintf(s, "<td align=%s><b>%s</b>\n", align_string(h), h->title);
		h++;
	}
	color_counter = 0;
}

void nvpair_print_html(struct nvpair *n, FILE * s, struct nvpair_header *h)
{
	return nvpair_print_html_with_link(n, s, h, 0, 0);
}

void nvpair_print_html_with_link(struct nvpair *n, FILE * s, struct nvpair_header *h, const char *linkname, const char *linktext)
{
	fprintf(s, "<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
	color_counter++;
	while(h->name) {
		const char *text = nvpair_lookup_string(n, h->name);
		if(!text)
			text = "???";
		fprintf(s, "<td align=%s>", align_string(h));
		if(h->mode == NVPAIR_MODE_URL) {
			fprintf(s, "<a href=%s>%s</a>\n", text, text);
		} else if(h->mode == NVPAIR_MODE_METRIC) {
			char line[1024];
			string_metric(atof(text), -1, line);
			fprintf(s, "%sB\n", line);
		} else {
			if(linkname && !strcmp(linkname, h->name)) {
				fprintf(s, "<a href=%s>%s</a>\n", linktext, text);
			} else {
				fprintf(s, "%s\n", text);
			}
		}
		h++;
	}
}

void nvpair_print_html_footer(FILE * s, struct nvpair_header *h)
{
	fprintf(s, "</table>\n");
}

static void fill_string(const char *str, char *buf, int buflen, nvpair_align_t align)
{
	int stlen = strlen(str);
	memset(buf, ' ', buflen);
	buf[buflen] = 0;
	if(align == NVPAIR_ALIGN_LEFT) {
		while(stlen > 0 && buflen > 0) {
			*buf++ = *str++;
			stlen--;
			buflen--;
		}
	} else {
		str = str + stlen - 1;
		buf = buf + buflen - 1;
		while(stlen > 0 && buflen > 0) {
			*buf-- = *str--;
			stlen--;
			buflen--;
		}
	}
}

void nvpair_print_table_header(FILE * s, struct nvpair_header *h)
{
	while(h->name) {
		char *n = xxmalloc(h->width + 1);
		fill_string(h->title, n, h->width, h->align);
		string_toupper(n);
		printf("%s ", n);
		free(n);
		h++;
	}
	printf("\n");
}

void nvpair_print_table(struct nvpair *n, FILE * s, struct nvpair_header *h)
{
	while(h->name) {
		const char *text = nvpair_lookup_string(n, h->name);
		char *aligned = xxmalloc(h->width + 1);
		char *line;
		if(!text) {
			line = xxstrdup("???");
		} else if(h->mode == NVPAIR_MODE_METRIC) {
			line = xxmalloc(10);
			string_metric(atof(text), -1, line);
			strcat(line, "B");
		} else if(h->mode == NVPAIR_MODE_TIMESTAMP || h->mode == NVPAIR_MODE_TIME) {
			line = xxmalloc(h->width);
			timestamp_t ts;
			int ret = 0;
			if(sscanf(text, "%llu", &ts) == 1) {
				if(h->mode == NVPAIR_MODE_TIME) {
					ts *= 1000000;
				}
				ret = timestamp_fmt(line, h->width, "%R %b %d, %Y", ts);
			}
			if(ret == 0) {
				strcpy(line, "???");
			}
		} else {
			line = xxmalloc(strlen(text) + 1);
			strcpy(line, text);
		}
		fill_string(line, aligned, h->width, h->align);
		printf("%s ", aligned);
		free(line);
		free(aligned);
		h++;
	}
	printf("\n");
}

void nvpair_print_table_footer(FILE * s, struct nvpair_header *h)
{
}
