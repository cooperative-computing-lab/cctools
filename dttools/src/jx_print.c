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

static void jx_string_print( const char *s, buffer_t *b )
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
		case JX_FLOAT:
			buffer_printf(b,"%lg",j->float_value);
			break;
		case JX_BOOLEAN:
			buffer_printf(b,"%s",j->boolean_value ? "true" : "false");
			break;
		case JX_INTEGER:
			buffer_printf(b,"%d",j->integer_value);
			break;
		case JX_SYMBOL:
			buffer_printf(b,"%s",j->symbol_name);
			break;
		case JX_STRING:
			jx_string_print(j->string_value,b);
			break;
		case JX_ARRAY:
			buffer_putstring(b,"[");
			jx_item_print(j->items,b);
			buffer_putstring(b,"]");
			break;
		case JX_OBJECT:
			buffer_putstring(b,"{");
			jx_pair_print(j->pairs,b);
			buffer_putstring(b,"}");
			break;
	}
}

void jx_print_file( struct jx *j, FILE *file )
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_print_buffer(j,&buffer);
	fprintf(file,"%s",buffer_tostring(&buffer));
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
