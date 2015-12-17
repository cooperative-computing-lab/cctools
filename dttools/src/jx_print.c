/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"

#include <ctype.h>

static void jx_pair_print( struct jx_pair *pair, buffer_t *b )
{
	if(!pair) return;

	jx_print_buffer(pair->key,b);
	buffer_putstring(b,":");
	jx_print_buffer(pair->value,b);
	if(pair->next) {
		buffer_putstring(b,",");
		jx_pair_print(pair->next,b);
	}
}

static void jx_item_print( struct jx_item *item, buffer_t *b )
{
	if(!item) return;

	jx_print_buffer(item->value,b);
	if(item->next) {
		buffer_putstring(b,",");
		jx_item_print(item->next,b);
	}
}

void jx_escape_string( const char *s, buffer_t *b )
{
	if(!s) return;

	buffer_putstring(b,"\"");
	while(*s) {
		switch(*s) {
			case '\"':
				buffer_putstring(b,"\\\"");
				break;
			case '\'':
				buffer_putstring(b,"\\\'");
				break;
			case '\\':
				buffer_putstring(b,"\\\\");
				break;
			case '\b':
				buffer_putstring(b,"\\b");
				break;
			case '\f':
				buffer_putstring(b,"\\f");
				break;
			case '\n':
				buffer_putstring(b,"\\n");
				break;
			case '\r':
				buffer_putstring(b,"\\r");
				break;
			case '\t':
				buffer_putstring(b,"\\t");
				break;
			default:
				if(isprint(*s)) {
					buffer_printf(b,"%c",*s);
				} else {
					buffer_printf(b,"\\u%04x",(int)*s);
				}
				break;
		}
		s++;
	}
	buffer_putstring(b,"\"");
}

void jx_print_buffer( struct jx *j, buffer_t *b )
{
	if(!j) return;

	switch(j->type) {
		case JX_NULL:
			buffer_putstring(b,"null");
			break;
		case JX_DOUBLE:
			buffer_printf(b,"%lf",j->u.double_value);
			break;
		case JX_BOOLEAN:
			buffer_printf(b,"%s",j->u.boolean_value ? "true" : "false");
			break;
		case JX_INTEGER:
			buffer_printf(b,"%lld",(long long)j->u.integer_value);
			break;
		case JX_SYMBOL:
			buffer_printf(b,"%s",j->u.symbol_name);
			break;
		case JX_STRING:
			jx_escape_string(j->u.string_value,b);
			break;
		case JX_ARRAY:
			buffer_putstring(b,"[");
			jx_item_print(j->u.items,b);
			buffer_putstring(b,"]");
			break;
		case JX_OBJECT:
			buffer_putstring(b,"{");
			jx_pair_print(j->u.pairs,b);
			buffer_putstring(b,"}");
			break;
	}
}

void jx_print_stream( struct jx *j, FILE *file )
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_print_buffer(j,&buffer);
	fprintf(file,"%s",buffer_tostring(&buffer));
	buffer_free(&buffer);
}

void jx_print_link( struct jx *j, struct link *l, time_t stoptime )
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_print_buffer(j,&buffer);
	link_write(l,buffer.buf,buffer.len,stoptime);
	buffer_free(&buffer);
}

char * jx_print_string( struct jx *j )
{
	buffer_t buffer;
	char *str;
	buffer_init(&buffer);
	jx_print_buffer(j,&buffer);
	buffer_dup(&buffer,&str);
	buffer_free(&buffer);
	return str;
}
