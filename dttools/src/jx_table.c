/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_table.h"
#include "jx_print.h"

#include "stringtools.h"

#include <stdlib.h>
#include <string.h>

static void fill_string(const char *str, char *buf, int buflen, jx_table_align_t align)
{
	int stlen = strlen(str);
	memset(buf, ' ', buflen);
	buf[buflen] = 0;
	if(align == JX_TABLE_ALIGN_LEFT) {
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

void jx_table_print_header( struct jx_table *t, FILE *f )
{
	while(t->name) {
		char *n = malloc(t->width + 1);
		fill_string(t->title, n, t->width, t->align);
		string_toupper(n);
		fprintf(f,"%s ", n);
		free(n);
		t++;
	}
	fprintf(f,"\n");
}

void jx_table_print( struct jx_table *t, struct jx *j, FILE * f )
{
	while(t->name) {
		char *line;
		if(t->mode == JX_TABLE_MODE_METRIC && j->type==JX_INTEGER) {
			line = malloc(10);
			string_metric(j->integer_value, -1, line);
			strcat(line, "B");
		} else {
			line = jx_print_string(j);
			if(!line) line = strdup("???");
		}
		char *aligned = malloc(t->width + 1);
		fill_string(line, aligned, t->width, t->align);
		fprintf(f,"%s ", aligned);
		free(line);
		free(aligned);
		t++;
	}
	fprintf(f,"\n");
}

void jx_table_print_footer( struct jx_table *t, FILE * f )
{
}
