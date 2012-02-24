#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "text_list.h"

struct text_list {
	char **items;
	int alloc_length;
	int used_length;
};

struct text_list *text_list_create()
{
	struct text_list *t = malloc(sizeof(*t));
	t->alloc_length = 8;
	t->used_length = 0;
	t->items = malloc(sizeof(*t->items) * t->alloc_length);
	return t;
}

struct text_list *text_list_load(const char *path)
{
	char line[1024];

	FILE *file = fopen(path, "r");
	if(!file)
		return 0;

	struct text_list *t = text_list_create();

	while(fgets(line, sizeof(line), file)) {
		line[strlen(line) - 1] = 0;
		text_list_append(t, line);
	}

	fclose(file);

	return t;
}

char *text_list_get(struct text_list *t, int i)
{
	if(i >= 0 && i < t->used_length) {
		return t->items[i];
	} else {
		return 0;
	}
}

int text_list_append(struct text_list *t, const char *str)
{
	if(t->used_length == t->alloc_length) {
		t->alloc_length *= 2;
		t->items = realloc(t->items, sizeof(*t->items) * t->alloc_length);
	}

	t->items[t->used_length] = strdup(str);
	return t->used_length++;
}

int text_list_size(struct text_list *t)
{
	return t->used_length;
}

void text_list_delete(struct text_list *t)
{
	int i;
	for(i = 0; i < t->used_length; i++) {
		free(t->items[i]);
	}
	free(t->items);
	free(t);
}
