/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_parse.h"
#include "jx_function.h"

#include "stringtools.h"
#include "debug.h"

#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

typedef enum {
	JX_TOKEN_SYMBOL,
	JX_TOKEN_INTEGER,
	JX_TOKEN_DOUBLE,
	JX_TOKEN_STRING,
	JX_TOKEN_FUNCTION,
	JX_TOKEN_ERROR,
	JX_TOKEN_LBRACKET,
	JX_TOKEN_RBRACKET,
	JX_TOKEN_LBRACE,
	JX_TOKEN_RBRACE,
	JX_TOKEN_COMMA,
	JX_TOKEN_COLON,
	JX_TOKEN_SEMI,
	JX_TOKEN_TRUE,
	JX_TOKEN_FALSE,
	JX_TOKEN_EQ,
	JX_TOKEN_NE,
	JX_TOKEN_LT,
	JX_TOKEN_LE,
	JX_TOKEN_GT,
	JX_TOKEN_GE,
	JX_TOKEN_ADD,
	JX_TOKEN_SUB,
	JX_TOKEN_MUL,
	JX_TOKEN_DIV,
	JX_TOKEN_MOD,
	JX_TOKEN_AND,
	JX_TOKEN_OR,
	JX_TOKEN_NOT,
	JX_TOKEN_NULL,
	JX_TOKEN_LPAREN,
	JX_TOKEN_RPAREN,
	JX_TOKEN_PARSE_ERROR,
	JX_TOKEN_EOF,
} jx_token_t;

#define MAX_TOKEN_SIZE 65536

struct jx_parser {
	char token[MAX_TOKEN_SIZE];
	FILE *source_file;
	const char *source_string;
	struct link *source_link;
	unsigned line;
	time_t stoptime;
	char *error_string;
	int errors;
	int strict_mode;
	int putback_char_valid;
	int putback_char;
	int putback_token_valid;
	jx_token_t putback_token;
	jx_int_t integer_value;
	double double_value;
};

struct jx_parser * jx_parser_create( int strict_mode )
{
	struct jx_parser *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));
	p->strict_mode = strict_mode;
	p->line = 1;
	return p;
}

void jx_parser_read_stream( struct jx_parser *p, FILE *file )
{
	p->source_file = file;
}

void jx_parser_read_string( struct jx_parser *p, const char *str )
{
	p->source_string = str;
}

void jx_parser_read_link( struct jx_parser *p, struct link *l, time_t stoptime )
{
	p->source_link = l;
	p->stoptime = stoptime;
}

int jx_parser_errors( struct jx_parser *p )
{
	return p->errors;
}

const char * jx_parser_error_string( struct jx_parser *p )
{
	return p->error_string;
}

void jx_parser_delete( struct jx_parser *p )
{
	if(p->error_string) free(p->error_string);
	free(p);
}

static void jx_parse_error( struct jx_parser *p, const char *str )
{
	free(p->error_string);
	p->error_string = string_format("line %u: %s", p->line, str);
	p->errors++;
}

static struct jx *jx_add_lineno(struct jx_parser *p, struct jx *j) {
	assert(p);
	if (!j) return NULL;
	j->line = p->line;
	return j;
}

static int jx_getchar( struct jx_parser *p )
{
	int c=0;

	if(p->putback_char_valid) {
		p->putback_char_valid = 0;
		if (p->putback_char == '\n') ++p->line;
		return p->putback_char;
	}

	if(p->source_file) {
		c = fgetc(p->source_file);
	} else if(p->source_string) {
		c = *p->source_string;
		if(c) {
			p->source_string++;
		} else {
			c = EOF;
		}
	} else if(p->source_link) {
		char ch;
		int result = link_read(p->source_link,&ch,1,p->stoptime);
		if(result==1) {
			c = ch;
		} else {
			c = EOF;
		}
	}

	if (c == '\n') ++p->line;
	return c;
}

static void jx_ungetchar( struct jx_parser *p, int c )
{
	if (c == '\n') --p->line;
	p->putback_char = c;
	p->putback_char_valid = 1;
}

static int jx_scan_unicode( struct jx_parser *s )
{
	int i;
	char str[5];

	for(i=0;i<4;i++) {
		str[i] = jx_getchar(s);
	}
	str[4] = 0;

	int uc;
	if(sscanf(str,"%x",&uc)) {
		if(uc<=0x7f) {
			// only accept basic ascii characters
			return uc;
		} else {
			jx_parse_error(s,"unsupported unicode escape string");
			return -1;
		}
	} else {
		jx_parse_error(s,"invalid unicode escape string");
		return -1;
	}
}

static int jx_scan_string_char( struct jx_parser *s )
{
	int c = jx_getchar(s);
	if(c==EOF) {
		return EOF;
	} else if(c=='\"') {
		return 0;
	} else if(c=='\\') {
		c = jx_getchar(s);
		switch(c) {
			case 'b':	return '\b';
			case 'f':	return '\f';
			case 'n':	return '\n';
			case 'r':	return '\r';
			case 't':	return '\t';
			case 'u':	return jx_scan_unicode(s);
			default:	return c;
		}
	} else {
		return c;
	}
}

static void jx_unscan( struct jx_parser *s, jx_token_t t )
{
	s->putback_token = t;
	s->putback_token_valid = 1;
}

static jx_token_t jx_scan( struct jx_parser *s )
{
	int c;

	if(s->putback_token_valid) {
		s->putback_token_valid = 0;
		return s->putback_token;
	}

	retry:
	c = jx_getchar(s);

	if(isspace(c)) {
		goto retry;
	} else if(c==EOF) {
		return JX_TOKEN_EOF;
	} else if(c=='{') {
		return JX_TOKEN_LBRACE;
	} else if(c=='}') {
		return JX_TOKEN_RBRACE;
	} else if(c=='[') {
		return JX_TOKEN_LBRACKET;
	} else if(c==']') {
		return JX_TOKEN_RBRACKET;
	} else if(c==',') {
		return JX_TOKEN_COMMA;
	} else if(c==':') {
		return JX_TOKEN_COLON;
	} else if(c==';') {
		return JX_TOKEN_SEMI;
	} else if(c=='+') {
		return JX_TOKEN_ADD;
	} else if(c=='-') {
		return JX_TOKEN_SUB;
	} else if(c=='*') {
		return JX_TOKEN_MUL;
	} else if(c=='/') {
		return JX_TOKEN_DIV;
	} else if(c=='%') {
		return JX_TOKEN_MOD;
	} else if(c=='&') {
		char d = jx_getchar(s);
		if(d=='&') return JX_TOKEN_AND;
		jx_parse_error(s,"invalid character: &");
		return JX_TOKEN_PARSE_ERROR;
	} else if(c=='|') {
		char d = jx_getchar(s);
		if(d=='|') return JX_TOKEN_OR;
		jx_parse_error(s,"invalid character: |");
		return JX_TOKEN_PARSE_ERROR;
	} else if(c=='!') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_NE;
		jx_ungetchar(s,d);
		return JX_TOKEN_NOT;
	} else if(c=='=') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_EQ;
		jx_parse_error(s,"invalid character: =");
		return JX_TOKEN_PARSE_ERROR;
	} else if(c=='<') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_LE;
		jx_ungetchar(s,d);
		return JX_TOKEN_LT;
	} else if(c=='>') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_GE;
		jx_ungetchar(s,d);
		return JX_TOKEN_GT;
	} else if(c=='\"') {
		int i;
		for(i=0;i<MAX_TOKEN_SIZE;i++) {
			int n = jx_scan_string_char(s);
			if(n==EOF) {
				jx_parse_error(s,"missing end quote");
				return JX_TOKEN_PARSE_ERROR;
			} else if(n==0) {
				s->token[i] = n;
				return JX_TOKEN_STRING;
			} else {
				s->token[i] = n;
			}
		}
		jx_parse_error(s,"string constant too long");
		return JX_TOKEN_PARSE_ERROR;

	} else if(c=='(') {
		return JX_TOKEN_LPAREN;
	} else if(c==')') {
		return JX_TOKEN_RPAREN;
	} else if (c=='#') {
		while (c != '\n' && c != '\r' && c != EOF) c = jx_getchar(s);
		jx_ungetchar(s, c);
		goto retry;
	} else if(strchr("0123456789.",c)) {
		s->token[0] = c;
		int i;
		for(i=1;i<MAX_TOKEN_SIZE;i++) {
			c = jx_getchar(s);
			if(strchr("0123456789.",c)) {
				s->token[i] = c;
			}
			else if(strchr("eE",c)) {
				s->token[i] = c;
				c = jx_getchar(s);
				if(strchr("-+",c)) {
					i++;
					s->token[i] = c;
				} else {
					jx_ungetchar(s, c);
				}
			}
			else {
				s->token[i] = 0;
				jx_ungetchar(s,c);

				char *endptr;

				s->integer_value = strtoll(s->token,&endptr,10);
				if(!*endptr) return JX_TOKEN_INTEGER;

				s->double_value = strtod(s->token,&endptr);
				if(!*endptr) return JX_TOKEN_DOUBLE;

				jx_parse_error(s,"invalid number format");
				return JX_TOKEN_PARSE_ERROR;
			}
		}
		jx_parse_error(s,"integer constant too long");
		return JX_TOKEN_PARSE_ERROR;
	} else if(isalpha(c) || c=='_') {
		s->token[0] = c;
		int i;
		for(i=1;i<MAX_TOKEN_SIZE;i++) {
			c = jx_getchar(s);
			if(isalnum(c) || c=='_') {
				s->token[i] = c;
			} else {
				jx_ungetchar(s,c);
				s->token[i] = 0;
				if(!strcmp(s->token,"true")) {
					return JX_TOKEN_TRUE;
				} else if(!strcmp(s->token,"false")) {
					return JX_TOKEN_FALSE;
				} else if(!strcmp(s->token,"null")) {
					return JX_TOKEN_NULL;
				} else if(!strcmp(s->token, "Error")) {
					return JX_TOKEN_ERROR;
				} else if (jx_function_name_from_string(s->token)) {
					return JX_TOKEN_FUNCTION;
				} else {
					return JX_TOKEN_SYMBOL;
				}
			}
		}
		jx_parse_error(s,"symbol too long");
		return JX_TOKEN_PARSE_ERROR;
	} else {
		s->token[0] = c;
		s->token[1] = 0;
		jx_parse_error(s,"invalid character");
		return JX_TOKEN_PARSE_ERROR;
	}
}

static struct jx_item * jx_parse_item_list( struct jx_parser *s, int arglist )
{
	jx_token_t rdelim = arglist ? JX_TOKEN_RPAREN : JX_TOKEN_RBRACKET;
	jx_token_t t = jx_scan(s);
	if(t==rdelim) {
		// empty list
		return 0;
	}

	jx_unscan(s,t);

	struct jx_item *i = jx_item(0,0);
	i->line = s->line;

	i->value = jx_parse(s);
	if(!i->value) {
		// error set by deeper layer
		jx_item_delete(i);
		return 0;
	}

	t = jx_scan(s);
	if(t==JX_TOKEN_COMMA) {
		i->next = jx_parse_item_list(s, arglist);
		if (jx_parser_errors(s)) {
			// error set by deeper layer
			jx_item_delete(i);
			return 0;
		}
	} else if(t==rdelim) {
		i->next = 0;
	} else {
		jx_parse_error(s,"list of items missing a comma or closing delimiter");
		jx_item_delete(i);
		return 0;
	}

	return i;
}

static struct jx_pair * jx_parse_pair_list( struct jx_parser *s )
{
	jx_token_t t = jx_scan(s);
	if(t==JX_TOKEN_RBRACE) {
		// empty list
		return 0;
	}

	jx_unscan(s,t);

	struct jx_pair *p = jx_pair(0,0,0);

	p->key = jx_parse(s);
	if(!p->key) {
		// error set by deeper layer
		jx_pair_delete(p);
		return 0;
	}

	if(s->strict_mode) {
		if(p->key->type!=JX_STRING) {
			jx_parse_error(s,"key-value pair must have a string as the key");
			jx_pair_delete(p);
			return 0;
		}
	}

	t = jx_scan(s);
	if(t!=JX_TOKEN_COLON) {
		jx_parse_error(s,"key-value pair must be separated by a colon");
		jx_pair_delete(p);
		return 0;
	}

	p->line = s->line;
	p->value = jx_parse(s);
	if(!p->value) {
		// error set by deeper layer
		jx_pair_delete(p);
		return 0;
	}

	t = jx_scan(s);
	if(t==JX_TOKEN_COMMA) {
		p->next = jx_parse_pair_list(s);
		if (jx_parser_errors(s)) {
			// error set by deeper layer
			jx_pair_delete(p);
			return 0;
		}
	} else if(t==JX_TOKEN_RBRACE) {
		p->next = 0;
	} else {
		jx_parse_error(s,"key-value pairs missing a comma or closing brace");
		jx_pair_delete(p);
		return 0;
	}

	return p;
}

static struct jx * jx_parse_atomic( struct jx_parser *s, int arglist )
{
	jx_token_t t = jx_scan(s);

	if (arglist) {
		if (t == JX_TOKEN_LPAREN) {
			t = JX_TOKEN_LBRACKET;
		} else {
			jx_parse_error(s,"function missing opening parenthesis");
			return NULL;
		}
	}

	switch (t) {
		case JX_TOKEN_EOF:
		case JX_TOKEN_RPAREN:
			return NULL;
		case JX_TOKEN_LBRACE: {
			unsigned line = s->line;
			struct jx_pair *p = jx_parse_pair_list(s);
			if (jx_parser_errors(s)) {
				// error set by deeper level
				jx_pair_delete(p);
				return NULL;
			}
			struct jx *j = jx_object(p);
			j->line = line;
			return j;
		}
		case JX_TOKEN_LBRACKET:	{
			unsigned line = s->line;
			struct jx_item *i = jx_parse_item_list(s, arglist);
			if (jx_parser_errors(s)) {
				// error set by deeper level
				jx_item_delete(i);
				return NULL;
			}
			struct jx *j = jx_array(i);
			j->line = line;
			return j;
		}
		case JX_TOKEN_STRING:
			return jx_add_lineno(s, jx_string(s->token));
		case JX_TOKEN_INTEGER:
			return jx_add_lineno(s, jx_integer(s->integer_value));
		case JX_TOKEN_DOUBLE:
			return jx_add_lineno(s, jx_double(s->double_value));
		case JX_TOKEN_TRUE:
			return jx_add_lineno(s, jx_boolean(1));
		case JX_TOKEN_FALSE:
			return jx_add_lineno(s, jx_boolean(0));
		case JX_TOKEN_NULL:
			return jx_add_lineno(s, jx_null());
		case JX_TOKEN_SYMBOL: {
			if(s->strict_mode) {
				jx_parse_error(s,"symbols are not allowed in strict parsing mode");
				return NULL;
			}
			return jx_add_lineno(s, jx_symbol(s->token));
		}
		case JX_TOKEN_LPAREN: {
			struct jx *j = jx_parse(s);
			if (!j) return NULL;

			t = jx_scan(s);
			if (t != JX_TOKEN_RPAREN) {
				jx_parse_error(s, "missing closing parenthesis");
				jx_delete(j);
				return NULL;
			}
			return j;
		}
		default: {
			jx_parse_error(s,"unexpected token");
			return NULL;
		}
	}

	/*
	We shouldn't get here, since all the token types
	should be handled above.  But just in case...
	*/

	jx_parse_error(s,"parse error");
	return 0;
}

#define JX_PRECEDENCE_MAX 5

int jx_operator_precedence( jx_operator_t t )
{
	switch(t) {
		case JX_OP_AND:	return 5;
		case JX_OP_OR:	return 4;
		case JX_OP_EQ:	return 3;
		case JX_OP_NE:	return 3;
		case JX_OP_LE:	return 3;
		case JX_OP_LT:	return 3;
		case JX_OP_GE:	return 3;
		case JX_OP_GT:	return 3;
		case JX_OP_ADD: return 2;
		case JX_OP_SUB:	return 2;
		case JX_OP_MUL:	return 1;
		case JX_OP_DIV:	return 1;
		case JX_OP_MOD:	return 1;
		case JX_OP_LOOKUP: return 0;
		default:	return 0;
	}
}

static jx_operator_t jx_token_to_operator( jx_token_t t )
{
	switch(t) {
		case JX_TOKEN_EQ:	return JX_OP_EQ;
		case JX_TOKEN_NE:	return JX_OP_NE;
		case JX_TOKEN_LE:	return JX_OP_LE;
		case JX_TOKEN_LT:	return JX_OP_LT;
		case JX_TOKEN_GE:	return JX_OP_GE;
		case JX_TOKEN_GT:	return JX_OP_GT;
		case JX_TOKEN_ADD:	return JX_OP_ADD;
		case JX_TOKEN_SUB:	return JX_OP_SUB;
		case JX_TOKEN_MUL:	return JX_OP_MUL;
		case JX_TOKEN_DIV:	return JX_OP_DIV;
		case JX_TOKEN_MOD:	return JX_OP_MOD;
		case JX_TOKEN_AND:	return JX_OP_AND;
		case JX_TOKEN_OR:	return JX_OP_OR;
		case JX_TOKEN_NOT:	return JX_OP_NOT;
		case JX_TOKEN_LBRACKET:	return JX_OP_LOOKUP;
		default:		return JX_OP_INVALID;
	}
}

static int jx_operator_is_unary( jx_operator_t op )
{
	switch(op) {
		case JX_OP_NOT:
			return 1;
		default:
			return 0;
	}
}

static struct jx * jx_parse_postfix( struct jx_parser *s, int arglist )
{
	struct jx *a = jx_parse_atomic(s, arglist);
	if(!a) return 0;

	jx_token_t t = jx_scan(s);
	if(t==JX_TOKEN_LBRACKET) {
		unsigned line = s->line;
		struct jx *b = jx_parse(s);
		if(!b) {
			jx_delete(a);
			// parse error already set
			return 0;
		}

		t = jx_scan(s);
		if(t!=JX_TOKEN_RBRACKET) {
			jx_parse_error(s,"missing closing bracket");
			jx_delete(a);
			jx_delete(b);
			return 0;
		} else {
			struct jx *j = jx_operator(JX_OP_LOOKUP, a, b);
			j->line = line;
			j->u.oper.line = line;
			return j;
		}
	} else {
		jx_unscan(s,t);
		return a;
	}
}

static struct jx * jx_parse_unary( struct jx_parser *s )
{
	jx_token_t t = jx_scan(s);
	switch (t) {
		case JX_TOKEN_SUB:
		case JX_TOKEN_ADD:
		case JX_TOKEN_NOT: {
			unsigned line = s->line;
			struct jx *j = jx_parse_postfix(s, 0);
			if (!j) {
				// error set by deeper level
				return NULL;
			}
			j = jx_operator(jx_token_to_operator(t), NULL, j);
			j->line = line;
			j->u.oper.line = line;
			return j;
		}
		case JX_TOKEN_FUNCTION: {
			jx_function_t f = jx_function_name_from_string(s->token);
			if (!f) {
				jx_parse_error(s,"invalid function");
				return NULL;
			}
			unsigned line = s->line;
			struct jx *j = jx_parse_postfix(s, 1);
			if (!j) {
				// error set by deeper level
				return NULL;
			}
			if (!jx_istype(j, JX_ARRAY)) {
				jx_parse_error(s, "malformed function");
				return NULL;
			}
			j = jx_function(f, j);
			j->line = line;
			j->u.func.line = line;
			return j;
		}
		case JX_TOKEN_ERROR: {
			unsigned line = s->line;
			struct jx *j = jx_parse_postfix(s, 0);
			if (!j) {
				jx_parse_error(s, "error is missing a required field");
				return NULL;
			}
			if (!jx_error_valid(j)) {
				jx_delete(j);
				jx_parse_error(s, "invalid error specification");
				return NULL;
			}
			j = jx_error(j);
			j->line = line;
			j->u.err->line = line;
			return j;
		}
		default: {
			jx_unscan(s,t);
			return jx_parse_postfix(s, 0);
		}
	}
}

static struct jx * jx_parse_binary( struct jx_parser *s, int precedence )
{
	struct jx *a;

	if(precedence<=0) {
		a = jx_parse_unary(s);
	} else {
		a = jx_parse_binary(s,precedence-1);
	}

	if(!a) return 0;

	jx_token_t t = jx_scan(s);
	jx_operator_t op = jx_token_to_operator(t);

	if(op!=JX_OP_INVALID && !jx_operator_is_unary(op) && jx_operator_precedence(op)==precedence ) {
		unsigned line = s->line;
		struct jx *b = jx_parse_binary(s,precedence);
		if (b) {
			struct jx *j = jx_operator(op, a, b);
			j->line = line;
			j->u.oper.line = line;
			return j;
		} else {
			jx_delete(a);
			return NULL;
		}
	} else {
		jx_unscan(s,t);
		return a;
	}
}

struct jx * jx_parse( struct jx_parser *s )
{
	struct jx *j = jx_parse_binary(s,JX_PRECEDENCE_MAX);
	if(!j) return 0;

	jx_token_t t = jx_scan(s);
	if(t!=JX_TOKEN_SEMI) jx_unscan(s,t);

	return j;
}

static struct jx * jx_parse_finish( struct jx_parser *p )
{
	struct jx * j = jx_parse(p);
	if(jx_parser_errors(p)) {
		debug(D_JX|D_NOTICE, "parse error: %s", jx_parser_error_string(p));
		jx_parser_delete(p);
		jx_delete(j);
		return 0;
	}
	jx_parser_delete(p);
	return j;
}

struct jx * jx_parser_yield( struct jx_parser *p )
{
	struct jx * j = jx_parse(p);
	if(jx_parser_errors(p)) {
		debug(D_JX|D_NOTICE, "parse error: %s", jx_parser_error_string(p));
		jx_delete(j);
		return 0;
	}
	return j;
}

struct jx * jx_parse_string( const char *str )
{
	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_string(p,str);
	return jx_parse_finish(p);
}

struct jx * jx_parse_link( struct link *l, time_t stoptime )
{
	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_link(p,l,stoptime);
	return jx_parse_finish(p);
}

struct jx * jx_parse_stream( FILE *file )
{
	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_stream(p,file);
	return jx_parse_finish(p);
}

struct jx * jx_parse_file( const char *name )
{
	FILE *file = fopen(name,"r");
	if(!file) return 0;
	struct jx *j = jx_parse_stream(file);
	fclose(file);
	return j;
}
