/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "jx_pretty_print.h"

#include <ctype.h>

#define SPACES 2

void jx_pretty_print_buffer( struct jx *j, buffer_t *b, int level, int key);


static void jx_pretty_print_pair( struct jx_pair *pair, buffer_t *b, int level)
{
	if(!pair) return;

	jx_pretty_print_buffer(pair->key, b, level, /* key */ 1);
	jx_pretty_print_buffer(pair->value, b, level+1, 0);

	if(pair->next) {
		buffer_putstring(b,",\n");
		jx_pretty_print_pair(pair->next, b, level);
	} else {
		buffer_putstring(b,"\n");
	}
}

void jx_pretty_print_buffer( struct jx *j, buffer_t *b, int level, int key)
{
	if(!j) return;

	switch(j->type) {
		case JX_NULL:
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_SYMBOL:
		case JX_STRING:
		case JX_ARRAY:
			buffer_printf(b,"%*s", level*SPACES, "");
			jx_print_buffer(j, b);
			if(key)
				buffer_printf(b,":");
			break;
		case JX_OBJECT:
			buffer_printf(b,"\n%*s{\n", level*SPACES, "");
			jx_pretty_print_pair(j->u.pairs, b, level+1);
			buffer_printf(b,"%*s}", level*SPACES, "");
			break;
		default:
			return;
	}
}

void jx_pretty_print_stream( struct jx *j, FILE *file )
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_pretty_print_buffer(j, &buffer, 0, 0);
	fprintf(file,"%s",buffer_tostring(&buffer));
	buffer_free(&buffer);
}
