/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_expr.h"
#include "deltadb_value.h"
#include "deltadb_functions.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern struct deltadb_value * deltadb_symbol_lookup( const char *name );

struct deltadb_expr * deltadb_expr_create( deltadb_expr_type_t type, struct deltadb_expr *left, struct deltadb_expr *right )
{
	struct deltadb_expr *e;

	e = malloc(sizeof(struct deltadb_expr));
	if(!e) return 0;

	memset(e,0,sizeof(*e));

	e->type = type;
	e->left = left;
	e->right = right;

	return e;
}

struct deltadb_expr * deltadb_expr_create_symbol( const char *n )
{
	struct deltadb_expr *e = deltadb_expr_create(DELTADB_EXPR_SYMBOL,0,0);
	e->symbol = n;
	return e;
}

struct deltadb_expr * deltadb_expr_create_fcall( const char *fname, struct deltadb_expr *args )
{
	struct deltadb_expr *e = deltadb_expr_create(DELTADB_EXPR_FCALL,0,args);
	e->symbol = fname;
	return e;
}

struct deltadb_expr * deltadb_expr_create_list( struct deltadb_expr *list )
{
	return deltadb_expr_create(DELTADB_EXPR_LIST,0,list);
}

struct deltadb_expr * deltadb_expr_create_value( struct deltadb_value *v )
{
	struct deltadb_expr *e = deltadb_expr_create(DELTADB_EXPR_VALUE,0,0);
	e->value = v;
	return e;
}

void deltadb_expr_delete( struct deltadb_expr *e )
{
	if(!e) return;

	deltadb_expr_delete(e->left);
	deltadb_expr_delete(e->right);
	deltadb_expr_delete(e->next);
	deltadb_value_delete(e->value);

	if(e->symbol) free((char*) e->symbol);

	free(e);
}

static const char * deltadb_expr_to_string( int type )
{
	switch(type) {
		case DELTADB_EXPR_LT:	return "<";
		case DELTADB_EXPR_LE:	return "<=";
		case DELTADB_EXPR_EQ:	return "==";
		case DELTADB_EXPR_NE:	return "!=";
		case DELTADB_EXPR_GT:	return ">";
		case DELTADB_EXPR_GE:	return ">=";
		case DELTADB_EXPR_NOT:	return "!";
		case DELTADB_EXPR_ADD:	return "+";
		case DELTADB_EXPR_SUB:	return "-";
		case DELTADB_EXPR_MUL:	return "*";
		case DELTADB_EXPR_DIV:	return "/";
		case DELTADB_EXPR_NEG:	return "-";
		case DELTADB_EXPR_MOD:	return "%";
		case DELTADB_EXPR_POW:	return "^";
		case DELTADB_EXPR_OR:	return "||";
		case DELTADB_EXPR_AND:	return "&&";
		default:	return "???";
	}
}

void deltadb_expr_print( FILE *file, struct deltadb_expr *e )
{
	if(!e) return;

	switch(e->type) {
		case DELTADB_EXPR_LT:
		case DELTADB_EXPR_LE:
		case DELTADB_EXPR_EQ:
		case DELTADB_EXPR_NE:
		case DELTADB_EXPR_GT:
		case DELTADB_EXPR_GE:
		case DELTADB_EXPR_ADD:
		case DELTADB_EXPR_SUB:
		case DELTADB_EXPR_MUL:
		case DELTADB_EXPR_DIV:
		case DELTADB_EXPR_MOD:
		case DELTADB_EXPR_OR:
		case DELTADB_EXPR_AND:
		case DELTADB_EXPR_POW:
			fprintf(file,"(");
			deltadb_expr_print(file,e->left);
			fprintf(file,"%s",deltadb_expr_to_string(e->type));
			deltadb_expr_print(file,e->right);
			fprintf(file,")");
			break;
		case DELTADB_EXPR_NOT:
		case DELTADB_EXPR_NEG:
			fprintf(file,"%s",deltadb_expr_to_string(e->type));
			deltadb_expr_print(file,e->left);
			break;
		case DELTADB_EXPR_LIST:
			fprintf(file,"[");
			for(e=e->right;e;e=e->next) {
				deltadb_expr_print(file,e);
				if(e->next) fprintf(file,",");
			}
			fprintf(file,"]");			
			break;
		case DELTADB_EXPR_VALUE:
			deltadb_value_print(file,e->value);
			break;
		case DELTADB_EXPR_SYMBOL:
			fprintf(file,"%s",e->symbol);
			break;
		case DELTADB_EXPR_FCALL:
			fprintf(file,"%s(",e->symbol);
			for(e=e->right;e;e=e->next) {
				deltadb_expr_print(file,e);
				if(e->next) fprintf(file,",");
			}
			fprintf(file,")");
			break;
	}
}

struct deltadb_value * deltadb_expr_eval( struct deltadb_expr *e )
{
	struct deltadb_value *a = 0;
	struct deltadb_value *b = 0;
	struct deltadb_value *result = 0;

	if(!e) return 0;

	if(e->left)  a = deltadb_expr_eval(e->left);
	if(e->right) b = deltadb_expr_eval(e->right);

	switch(e->type) {
		case DELTADB_EXPR_ADD:
			result = deltadb_value_add(a,b);
			break;
		case DELTADB_EXPR_SUB:
			result = deltadb_value_subtract(a,b);
			break;
		case DELTADB_EXPR_MUL:
			result = deltadb_value_multiply(a,b);
			break;
		case DELTADB_EXPR_DIV:
			result = deltadb_value_divide(a,b);
			break;
		case DELTADB_EXPR_MOD:
			result = deltadb_value_modulus(a,b);
			break;
		case DELTADB_EXPR_POW:
			result = deltadb_value_power(a,b);
			break;
		case DELTADB_EXPR_NEG:
			result = deltadb_value_negate(a);
			break;
		case DELTADB_EXPR_AND:
			result = deltadb_value_and(a,b);
			break;
		case DELTADB_EXPR_OR:
			result = deltadb_value_or(a,b);
			break;
		case DELTADB_EXPR_NOT:
			result = deltadb_value_not(a);
			break;
		case DELTADB_EXPR_LT:
			result = deltadb_value_lt(a,b);
			break;
		case DELTADB_EXPR_LE:
			result = deltadb_value_le(a,b);
			break;
		case DELTADB_EXPR_EQ:
			result = deltadb_value_eq(a,b);
			break;
		case DELTADB_EXPR_NE:
			result = deltadb_value_ne(a,b);
			break;
		case DELTADB_EXPR_GT:
			result = deltadb_value_gt(a,b);
			break;
		case DELTADB_EXPR_GE:
			result = deltadb_value_ge(a,b);
			break;
		case DELTADB_EXPR_SYMBOL:
			result = deltadb_symbol_lookup(e->symbol);
			if(!result) result = deltadb_value_create_error();
			break;
		case DELTADB_EXPR_LIST:
			result = deltadb_value_create_list(b);
			break;
		case DELTADB_EXPR_VALUE:
			result = deltadb_value_copy(e->value);
			break;
		case DELTADB_EXPR_FCALL:
			result = deltadb_function_call(e->symbol,b);
			break;
	}

	if(e->next && result) {
		result->next = deltadb_expr_eval(e->next);
	}

	return result;
}
