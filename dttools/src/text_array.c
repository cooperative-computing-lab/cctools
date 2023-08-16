
/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "text_array.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

struct text_array {
	int width;
	int height;
	char **data;
};

struct text_array *text_array_create(int w, int h)
{
	int size = w * h * sizeof(char *);
	struct text_array *t = malloc(sizeof(*t));
	t->width = w;
	t->height = h;
	t->data = malloc(size);
	memset(t->data, 0, size);
	return t;
}

void text_array_delete(struct text_array *t)
{
	int n = t->width * t->height;
	int i;
	for(i = 0; i < n; i++) {
		if(t->data[i])
			free(t->data[i]);
	}
	free(t->data);
	free(t);
}

int text_array_width(struct text_array *t)
{
	return t->width;
}

int text_array_height(struct text_array *t)
{
	return t->height;
}

const char *text_array_get(struct text_array *t, int x, int y)
{
	if(x < 0 || y < 0 || x >= t->width || y >= t->height)
		return 0;
	return t->data[y * t->width + x];
}

int text_array_set(struct text_array *t, int x, int y, const char *c)
{
	char *value;

	if(x < 0 || y < 0 || x >= t->width || y >= t->height)
		return 0;

	if(c) {
		value = strdup(c);
	} else {
		value = 0;
	}

	t->data[y * t->width + x] = value;

	return 1;
}

int text_array_load(struct text_array *t, const char *filename)
{
	int count = 0;
	int x, y;
	char value[4096];
	FILE *file;

	file = fopen(filename, "r");
	if(!file)
		return 0;

	while(fscanf(file, "%d %d %[^\n]\n", &x, &y, value) == 3) {
		text_array_set(t, x, y, value);
		count++;
	}

	fclose(file);
	return count;
}

int text_array_save(struct text_array *t, const char *filename)
{
	return text_array_save_range(t, filename, 0, 0, t->width, t->height);
}

int text_array_save_range(struct text_array *t, const char *filename, int x, int y, int w, int h)
{
	int count = 0;
	FILE *file;
	int i, j;

	file = fopen(filename, "w");
	if(!file)
		return 0;

	for(j = y; j < (y + h); j++) {
		for(i = x; i < (x + w); i++) {
			const char *v = text_array_get(t, i, j);
			if(v) {
				fprintf(file, "%d %d %s\n", i, j, v);
				count++;
			}
		}
	}

	fclose(file);
	return count;
}

/* vim: set noexpandtab tabstop=8: */
