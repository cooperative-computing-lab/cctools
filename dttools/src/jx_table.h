#ifndef JX_TABLE_H
#define JX_TABLE_H

#include "jx.h"
#include <stdio.h>

typedef enum {
	JX_TABLE_MODE_PLAIN,
	JX_TABLE_MODE_METRIC,
	JX_TABLE_MODE_GIGABYTES,
	JX_TABLE_MODE_URL
} jx_table_mode_t;

typedef enum {
	JX_TABLE_ALIGN_LEFT,
	JX_TABLE_ALIGN_RIGHT
} jx_table_align_t;

struct jx_table {
	const char *name;
	const char *title;
	jx_table_mode_t mode;
	jx_table_align_t align;
	int width;
};

void jx_table_print_header( struct jx_table *t, FILE *f, int columns );
void jx_table_print( struct jx_table *t, struct jx *j, FILE *f, int columns );
void jx_table_print_footer( struct jx_table *t, FILE *f, int columns );

#endif
