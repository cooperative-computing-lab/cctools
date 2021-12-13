/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "jx_pretty_print.h"

#include <ctype.h>

#define SPACES 2

static void jx_pretty_print_buffer( struct jx *j, buffer_t *b, int level );

static void jx_pretty_print_pair( struct jx_pair *pair, buffer_t *b, int level)
{
	if(!pair) return;

	buffer_printf(b,"%*s", level*SPACES, "");

	jx_pretty_print_buffer(pair->key, b, level );
	buffer_printf(b,":");
	jx_pretty_print_buffer(pair->value, b, level+1 );

	jx_comprehension_print(pair->comp, b);

	if(pair->next) {
		buffer_putstring(b,",\n");
		jx_pretty_print_pair(pair->next, b, level);
	} else {
		buffer_putstring(b,"\n");
	}
}

static void jx_pretty_print_item( struct jx_item *item, buffer_t *b, int level)
{
	if(!item) return;

	buffer_printf(b,"%*s", level*SPACES, "");

	jx_pretty_print_buffer(item->value, b, level );
	jx_comprehension_print(item->comp, b);

	if(item->next) {
		buffer_putstring(b,",\n");
		jx_pretty_print_item(item->next, b, level);
	} else {
		buffer_putstring(b,"\n");
	}
}

static void jx_pretty_print_buffer( struct jx *j, buffer_t *b, int level )
{
	if(!j) return;

	if(j->type==JX_OBJECT) {
		buffer_printf(b,"\n%*s{\n", level*SPACES, "");
		jx_pretty_print_pair(j->u.pairs, b, level+1);
		buffer_printf(b,"%*s}", level*SPACES, "");
	} else if(j->type==JX_ARRAY) {
		buffer_printf(b,"\n%*s[\n", level*SPACES, "");
		jx_pretty_print_item(j->u.items, b, level+1);
		buffer_printf(b,"%*s]", level*SPACES, "");
	} else {
		jx_print_buffer(j, b);
	}
}

void jx_pretty_print_stream( struct jx *j, FILE *file )
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_pretty_print_buffer(j, &buffer, 0 );
	fprintf(file,"%s",buffer_tostring(&buffer));
	buffer_free(&buffer);
}
