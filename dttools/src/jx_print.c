/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "jx_parse.h"
#include "jx_function.h"

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

const char * jx_operator_string( jx_operator_t type )
{
	switch(type) {
		case JX_OP_EQ: return "==";
		case JX_OP_NE: return "!=";
		case JX_OP_LT: return "<";
		case JX_OP_LE: return "<=";
		case JX_OP_GT: return ">";
		case JX_OP_GE: return ">=";
		case JX_OP_ADD: return "+";
		case JX_OP_SUB: return "-";
		case JX_OP_MUL: return "*";
		case JX_OP_DIV: return "/";
		case JX_OP_MOD: return "%";
		case JX_OP_AND:	return "&&";
		case JX_OP_OR:	return "||";
		case JX_OP_NOT:	return "!";
		// note that the closing bracket is in jx_print_subexpr
		case JX_OP_LOOKUP: return "[";
		default:        return "???";
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

void jx_print_subexpr( struct jx *j, jx_operator_t parent, buffer_t *b )
{
	if(!j) return;

	int do_parens = 0;

	if(j->type==JX_OPERATOR && jx_operator_precedence(parent) < jx_operator_precedence(j->u.oper.type)) {
		do_parens = 1;
	} else {
		do_parens = 0;
	}

	if(do_parens) buffer_putstring(b,"(");
	jx_print_buffer(j,b);
	if(do_parens) buffer_putstring(b,")");
}

void jx_print_args( struct jx *j, buffer_t *b )
{
	if(!jx_istype(j, JX_ARRAY)) return;
	jx_item_print(j->u.items, b);
}

void jx_print_buffer( struct jx *j, buffer_t *b )
{
	if(!j) return;

	switch(j->type) {
		case JX_NULL:
			buffer_putstring(b,"null");
			break;
		case JX_DOUBLE:
			buffer_printf(b,"%g",j->u.double_value);
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
		case JX_OPERATOR:
			jx_print_subexpr(j->u.oper.left,j->u.oper.type,b);
			buffer_putstring(b,jx_operator_string(j->u.oper.type));
			jx_print_subexpr(j->u.oper.right,j->u.oper.type,b);
			if(j->u.oper.type==JX_OP_LOOKUP) buffer_putstring(b,"]");
			break;
		case JX_FUNCTION:
			buffer_putstring(b, jx_function_name_to_string(j->u.func.function));
			buffer_putstring(b, "(");
			jx_print_args(j->u.func.arguments, b);
			buffer_putstring(b, ")");
			break;
		case JX_ERROR:
			buffer_putstring(b,"Error");
			jx_print_buffer(j->u.err, b);
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

	size_t len;
	const char *str = buffer_tolstring(&buffer, &len);

	link_write(l,str,len,stoptime);
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
