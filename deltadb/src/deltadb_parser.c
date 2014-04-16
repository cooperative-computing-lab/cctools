/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_scanner.h"
#include "deltadb_parser.h"
#include "deltadb_value.h"
#include "deltadb_expr.h"

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#define DELTADB_ERROR_MAX 1024

struct deltadb_parser {
	struct deltadb_scanner *scanner;
	char error_string[DELTADB_ERROR_MAX];
};

struct deltadb_expr * deltadb_parse_string_as_expr( const char *s )
{
	struct deltadb_parser * p = deltadb_parser_create( deltadb_scanner_create_from_string(s) );
	struct deltadb_expr *e = deltadb_parse_expr(p);
	deltadb_parser_delete(p);
	return e;
}

struct deltadb_value * deltadb_parse_string_as_value( const char *s )
{
	struct deltadb_parser * p = deltadb_parser_create( deltadb_scanner_create_from_string(s) );
	struct deltadb_value *v = deltadb_parse_value(p);
	deltadb_parser_delete(p);
	return v;
}

struct deltadb_parser * deltadb_parser_create( struct deltadb_scanner *s )
{
	struct deltadb_parser *p = malloc(sizeof(*p));
	p->scanner = s;
	p->error_string[0] = 0;
	return p;
}

const char * deltadb_parser_error_string( struct deltadb_parser *p )
{
	return p->error_string;
}

void deltadb_parser_delete( struct deltadb_parser *p )
{
	if(p) {
		deltadb_scanner_delete(p->scanner);
		free(p);
	}
}

int deltadb_parser_expect( struct deltadb_parser *p, deltadb_token_t t )
{
	if(deltadb_scanner_accept(p->scanner,t)) {
		return 1;
	} else {
		deltadb_token_t actual = deltadb_scanner_lookahead(p->scanner);
	
		sprintf(p->error_string,"expected %s but found %s instead",
			deltadb_scanner_get_token_name(t),
			deltadb_scanner_get_token_name(actual)
		);
		return 0;
	}
}

struct deltadb_expr * deltadb_parse_expr_list( struct deltadb_parser *p )
{
	struct deltadb_expr *head = 0;
	struct deltadb_expr *tail = 0;
	struct deltadb_expr *e;

	while(1) {
		e = deltadb_parse_expr(p);
		if(e) {
			if(!head) {
				head = tail = e;
			} else {
				tail->next = e;
				tail = e;
			}
		} else {
			deltadb_expr_delete(head);
			return 0;
		}

		if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_COMMA)) {
			continue;
		} else {
			return head;
		}
	}
}

struct deltadb_expr * deltadb_parse_atomic_expr( struct deltadb_parser *p )
{
	struct deltadb_expr *e;
	char *symbol;

	if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_LPAREN)) {
		e = deltadb_parse_expr(p);
		if(!e) return 0;

		if(deltadb_scanner_expect(p->scanner,DELTADB_TOKEN_RPAREN)) {
			return e;
		} else {
			deltadb_expr_delete(e);
			return 0;
		}
	} if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_LBRACKET)) {
		e = deltadb_parse_expr_list(p);
		if(!e) return 0;

		if(deltadb_scanner_expect(p->scanner,DELTADB_TOKEN_RBRACKET)) {
			return deltadb_expr_create_list(e);
		} else {
			deltadb_expr_delete(e);
			return 0;
		}
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_SYMBOL)) {
		symbol = strdup(deltadb_scanner_get_string_value(p->scanner));

		if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_LPAREN)) {
			e = deltadb_parse_expr_list(p);
			if(deltadb_parser_expect(p,DELTADB_TOKEN_RPAREN)) {
				return deltadb_expr_create_fcall(symbol,e);
			} else {
				deltadb_expr_delete(e);
				free(symbol);
				return 0;
			}
		} else {
			return deltadb_expr_create_symbol(symbol);
		}

	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_REAL_LITERAL)) {
		return deltadb_expr_create_value(deltadb_value_create_real(deltadb_scanner_get_real_value(p->scanner)));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_STRING_LITERAL)) {
		return deltadb_expr_create_value(deltadb_value_create_string(deltadb_scanner_get_string_value(p->scanner)));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_INTEGER_LITERAL)) {
		return deltadb_expr_create_value(deltadb_value_create_integer(deltadb_scanner_get_integer_value(p->scanner)));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_TRUE)) {
		return deltadb_expr_create_value(deltadb_value_create_boolean(1));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_FALSE)) {
		return deltadb_expr_create_value(deltadb_value_create_boolean(0));
	} else {
		sprintf(p->error_string,"expected value or symbol, but got %s instead",
			deltadb_scanner_get_token_name(deltadb_scanner_lookahead(p->scanner)));
		return 0;
	}

}

struct deltadb_expr * deltadb_parse_unary_expr( struct deltadb_parser *p )
{
	struct deltadb_expr *e;

	if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_NOT)) {
		e = deltadb_parse_unary_expr(p);
		if(e) {
			return deltadb_expr_create(DELTADB_EXPR_NOT,e,0);
		} else {
			return 0;
		}
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_SUB)) {
		e = deltadb_parse_unary_expr(p);
		if(e) {
			return deltadb_expr_create(DELTADB_EXPR_NEG,e,0);
		} else {
			return 0;
		}
	} else {
		return deltadb_parse_atomic_expr(p);
	}	
}

int token_precedence( deltadb_token_t t )
{
	switch(t) {
		case DELTADB_TOKEN_POW:	return 6;
		case DELTADB_TOKEN_MUL:	return 5;
		case DELTADB_TOKEN_DIV:	return 5;
		case DELTADB_TOKEN_MOD:	return 5;
		case DELTADB_TOKEN_ADD:	return 4;
		case DELTADB_TOKEN_SUB:	return 4;
		case DELTADB_TOKEN_LE:	return 3;
		case DELTADB_TOKEN_LT:	return 3;
		case DELTADB_TOKEN_GE:	return 3;
		case DELTADB_TOKEN_GT:	return 3;
		case DELTADB_TOKEN_NE:	return 2;
		case DELTADB_TOKEN_EQ:	return 2;
		case DELTADB_TOKEN_AND:	return 1;
		case DELTADB_TOKEN_OR:	return 0;
		default:	return -1;
	}
}

deltadb_expr_type_t token_to_expr( deltadb_token_t t )
{
	switch(t) {
		case DELTADB_TOKEN_POW:	return DELTADB_EXPR_POW;
		case DELTADB_TOKEN_MUL:	return DELTADB_EXPR_MUL;
		case DELTADB_TOKEN_DIV:	return DELTADB_EXPR_DIV;
		case DELTADB_TOKEN_MOD:	return DELTADB_EXPR_MOD;
		case DELTADB_TOKEN_ADD:	return DELTADB_EXPR_ADD;
		case DELTADB_TOKEN_SUB:	return DELTADB_EXPR_SUB;
		case DELTADB_TOKEN_LE:	return DELTADB_EXPR_LE;
		case DELTADB_TOKEN_LT:	return DELTADB_EXPR_LT;
		case DELTADB_TOKEN_GE:	return DELTADB_EXPR_GE;
		case DELTADB_TOKEN_GT:	return DELTADB_EXPR_GT;
		case DELTADB_TOKEN_NE:	return DELTADB_EXPR_NE;
		case DELTADB_TOKEN_EQ:	return DELTADB_EXPR_EQ;
		case DELTADB_TOKEN_AND:	return DELTADB_EXPR_AND;
		case DELTADB_TOKEN_OR:	return DELTADB_EXPR_OR;
		default:	return DELTADB_EXPR_ADD;
	}
}


struct deltadb_expr * deltadb_parse_binary_expr( struct deltadb_parser *p, int level )
{
	struct deltadb_expr *left, *right;
	deltadb_token_t t;

	if(level>6) return deltadb_parse_unary_expr(p);

	left = deltadb_parse_binary_expr(p,level+1);
	if(!left) return 0;

	while(1) {
		t = deltadb_scanner_lookahead(p->scanner);
		if(token_precedence(t)==level) {
			deltadb_scanner_accept(p->scanner,t);
			right = deltadb_parse_binary_expr(p,level+1);
			if(right) {
				left = deltadb_expr_create(token_to_expr(t),left,right);
				continue;
			} else {
				deltadb_expr_delete(left);
				return 0;
			}
		} else {
			return left;
		}
	}
}

struct deltadb_expr * deltadb_parse_expr( struct deltadb_parser *p )
{
	return deltadb_parse_binary_expr(p,0);
}

struct deltadb_value * deltadb_parse_value_list( struct deltadb_parser *p )
{
	struct deltadb_value *head=0, *tail=0, *v=0;

	while(1) {
		v = deltadb_parse_value(p);
		if(v) {
			if(!head) {
				head = tail = v;
			} else {
				tail->next = v;
				tail = v;
			}
		} else {
			deltadb_value_delete(head);
			return 0;
		}

		if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_COMMA)) {
			continue;
		} else {
			return head;
		}
	}
}

struct deltadb_value * deltadb_parse_value( struct deltadb_parser *p )
{
	struct deltadb_value *v;

	if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_LPAREN)) {
		v = deltadb_parse_value(p);
		if(!v) return 0;

		if(deltadb_scanner_expect(p->scanner,DELTADB_TOKEN_RPAREN)) {
			return v;
		} else {
			deltadb_value_delete(v);
			return 0;
		}
	} if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_LBRACKET)) {
		v = deltadb_parse_value_list(p);
		if(!v) return 0;

		if(deltadb_scanner_expect(p->scanner,DELTADB_TOKEN_RBRACKET)) {
			return deltadb_value_create_list(v);
		} else {
			deltadb_value_delete(v);
			return 0;
		}
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_REAL_LITERAL)) {
		return deltadb_value_create_real(deltadb_scanner_get_real_value(p->scanner));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_STRING_LITERAL)) {
		return deltadb_value_create_string(deltadb_scanner_get_string_value(p->scanner));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_INTEGER_LITERAL)) {
		return deltadb_value_create_integer(deltadb_scanner_get_integer_value(p->scanner));
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_TRUE)) {
		return deltadb_value_create_boolean(1);
	} else if(deltadb_scanner_accept(p->scanner,DELTADB_TOKEN_FALSE)) {
		return deltadb_value_create_boolean(0);
	} else {
		sprintf(p->error_string,"expected value or symbol, but got %s instead",
			deltadb_scanner_get_token_name(deltadb_scanner_lookahead(p->scanner)));
		return 0;
	}

}

