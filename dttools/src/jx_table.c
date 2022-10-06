/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_table.h"
#include "jx_print.h"

#include "stringtools.h"
#include "macros.h"

#include <stdlib.h>
#include <string.h>

static char *fill_string(const char *str, int width, jx_table_align_t align, int columns_left, int columns_extra)
{
	/* no more columns available */
	if(columns_left < 1 || width == 0) {
		return NULL;
	}

	/* this field autoexpands */
	if(width < 0) {
		width = abs(width) + columns_extra;
	}

	int stlen = strlen(str);

	char *buf_init = malloc(width + 1);
	memset(buf_init, ' ', width);
	buf_init[width] = 0;

	char *buf = buf_init;
	if(align == JX_TABLE_ALIGN_LEFT) {
		while(stlen > 0 && width > 0) {
			*buf++ = *str++;
			stlen--;
			width--;
		}
	} else {
		str = str + stlen - 1;
		buf = buf + width - 1;
		while(stlen > 0 && width > 0) {
			*buf-- = *str--;
			stlen--;
			width--;
		}
	}

	return buf_init;
}

void count_columns(struct jx_table *t, int columns_max, int *columns_total, int *columns_extra) {
	int proportional_fields = 0;

	*columns_total      = 0;
	*columns_extra      = 0;

	while(t->name) {
		if(t->width < 0) {
			proportional_fields++;
		}

		/* + 1 for the space separating the columns */
		*columns_total += abs(t->width) + 1;
		t++;
	}

	if(proportional_fields > 0) {
		*columns_extra = MAX(0, columns_max - *columns_total)/proportional_fields;
	}

	if(columns_max > 0) {
		*columns_total = MIN(*columns_total, columns_max);
	}
}

void jx_table_print_header( struct jx_table *t, FILE *f, int columns_max )
{
	int columns_total, columns_extra;
	count_columns(t, columns_max, &columns_total, &columns_extra);

	while(t->name) {
		char *n = fill_string(t->title, t->width, t->align, columns_total, columns_extra);

		if(n) {
			string_toupper(n);
			fprintf(f,"%s ", n);
			free(n);
		}

		t++;

		/* + 1 because of the space separating the columns. */
		columns_total -= (abs(t->width) + 1);
	}
	fprintf(f,"\n");
}

void jx_table_print( struct jx_table *t, struct jx *j, FILE * f, int columns_max )
{
	int columns_total, columns_extra;
	count_columns(t, columns_max, &columns_total, &columns_extra);

	while(t->name) {
		char *line;
		if(t->mode == JX_TABLE_MODE_METRIC) {
			line = malloc(10);
			string_metric(jx_lookup_integer(j,t->name), -1, line);
			strcat(line, "B");
		}
		else if(t->mode == JX_TABLE_MODE_GIGABYTES) {
			line = malloc(10);
			string_metric(jx_lookup_integer(j,t->name), 1, line);
			for (int i = 0; line[i] != '\0'; i++){
				if (line[i] == '.'){
					line[i] = '\0';
					break;
				}				
			}
		} else {
			int found;
			struct jx *v = jx_lookup_guard(j,t->name,&found);
			if(!found) {
				line = strdup("???");
			} else if(v->type==JX_STRING) {
				// special case b/c we want to see
				// a raw string without quotes or escapes.
				line = string_format("%s",v->u.string_value);
			} else {
				// other types should be printed natively
				line = jx_print_string(v);
			}
		}
		char *aligned = fill_string(line, t->width, t->align, columns_total, columns_extra);

		if(aligned) {
			fprintf(f,"%s ", aligned);
			free(aligned);
		}

		free(line);
		t++;

		/* + 1 because of the space separating the columns. */
		columns_total -= (abs(t->width) + 1);
	}
	fprintf(f,"\n");
}

void jx_table_print_footer( struct jx_table *t, FILE * f, int columns )
{
}
