/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_parse.h"
#include "jx_print.h"
#include "jx_eval.h"

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
	JX_TOKEN_C_AND,
	JX_TOKEN_OR,
	JX_TOKEN_C_OR,
	JX_TOKEN_DOT,
	JX_TOKEN_NOT,
	JX_TOKEN_C_NOT,
	JX_TOKEN_NULL,
	JX_TOKEN_LPAREN,
	JX_TOKEN_RPAREN,
	JX_TOKEN_FOR,
	JX_TOKEN_IN,
	JX_TOKEN_IF,
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
	bool strict_mode;
	bool putback_char_valid;
	int putback_char;
	bool putback_token_valid;
	jx_token_t putback_token;
	jx_int_t integer_value;
	double double_value;
};

static bool static_mode = false;

void jx_parse_set_static_mode( bool mode ) {
	static_mode = mode;
}

struct jx_parser *jx_parser_create(bool strict_mode) {
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
	free(p->error_string);
	free(p);
}

/*
Record a parse error for later retrieval.
For clarity to the user, only the first error encountered is saved.
str should be an allocated string which the parser will free.
*/

static void jx_parse_error_a( struct jx_parser *p, char *str )
{
	p->errors++;
	if(!p->error_string) {
		p->error_string = string_format("line %u: %s", p->line, str);
	}
	free(str);
}

/*
Same as above, but accepts a constant string that will not be freed.
*/

static void jx_parse_error_c( struct jx_parser *p, const char *str )
{
	jx_parse_error_a(p,strdup(str));
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
		p->putback_char_valid = false;
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
	p->putback_char_valid = true;
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
			jx_parse_error_a(s,string_format("unsupported unicode escape string: %s",str));
			return -1;
		}
	} else {
		jx_parse_error_a(s,string_format("invalid unicode escape string: %s",str));
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
	s->putback_token_valid = true;
}

static jx_token_t jx_scan( struct jx_parser *s )
{
	int c;

	if(s->putback_token_valid) {
		s->putback_token_valid = false;
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
	} else if(c=='!') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_NE;
		jx_ungetchar(s,d);
		return JX_TOKEN_C_NOT;
	} else if(c=='=') {
		char d = jx_getchar(s);
		if(d=='=') return JX_TOKEN_EQ;
		jx_parse_error_c(s,"single = must be == instead");
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
	} else if (c=='&') {
		char d = jx_getchar(s);
		if(d=='&') return JX_TOKEN_C_AND;
		jx_parse_error_c(s,"single & must be && instead");
		return JX_TOKEN_PARSE_ERROR;
	} else if (c=='|') {
		char d = jx_getchar(s);
		if(d=='|') return JX_TOKEN_C_OR;
		jx_parse_error_c(s,"single | must be || instead");
		return JX_TOKEN_PARSE_ERROR;
	} else if(c=='\"') {
		int i;
		for(i=0;i<MAX_TOKEN_SIZE;i++) {
			int n = jx_scan_string_char(s);
			if(n==EOF) {
				if(i>10) i = 10;
				s->token[i] = 0;
				jx_parse_error_a(s,string_format("missing end quote: \"%s...",s->token));
				return JX_TOKEN_PARSE_ERROR;
			} else if(n==0) {
				s->token[i] = n;
				return JX_TOKEN_STRING;
			} else {
				s->token[i] = n;
			}
		}
		s->token[10] = 0;
		jx_parse_error_a(s,string_format("string constant too long: \"%s...",s->token));
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
		if (c=='.') {
			char d = jx_getchar(s);
			jx_ungetchar(s, d);
			if (!strchr("0123456789",d)) return JX_TOKEN_DOT;
		}
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

				jx_parse_error_a(s,string_format("invalid number format: %s",s->token));
				return JX_TOKEN_PARSE_ERROR;
			}
		}
		jx_parse_error_a(s,string_format("integer constant too long: %s",s->token));
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
				if(!strcmp(s->token,"null")) {
					return JX_TOKEN_NULL;
				} else if(!strcmp(s->token,"true")) {
					return JX_TOKEN_TRUE;
				} else if(!strcmp(s->token,"false")) {
					return JX_TOKEN_FALSE;
				} else if(!strcmp(s->token,"or")) {
					return JX_TOKEN_OR;
				} else if(!strcmp(s->token,"and")) {
					return JX_TOKEN_AND;
				} else if(!strcmp(s->token,"not")) {
					return JX_TOKEN_NOT;
				} else if (!strcmp(s->token, "for")) {
					return JX_TOKEN_FOR;
				} else if (!strcmp(s->token, "in")) {
					return JX_TOKEN_IN;
				} else if (!strcmp(s->token, "if")) {
					return JX_TOKEN_IF;
				} else if(!strcmp(s->token, "error")) {
					return JX_TOKEN_ERROR;
				} else {
					return JX_TOKEN_SYMBOL;
				}
			}
		}
		jx_parse_error_a(s,string_format("symbol too long: %s",s->token));
		return JX_TOKEN_PARSE_ERROR;
	} else {
		s->token[0] = c;
		s->token[1] = 0;
		jx_parse_error_a(s,string_format("invalid character: %c",c));
		return JX_TOKEN_PARSE_ERROR;
	}
}

static struct jx_comprehension *jx_parse_comprehension(struct jx_parser *s) {
	jx_token_t t = jx_scan(s);
	if (t != JX_TOKEN_FOR) {
		jx_unscan(s, t);
		return NULL;
	}

	unsigned line = s->line;
	char *variable = NULL;
	struct jx *elements = NULL;
	struct jx *condition = NULL;
	struct jx_comprehension *result = NULL;

	t = jx_scan(s);
	if (t != JX_TOKEN_SYMBOL) {
		jx_parse_error_a(s, string_format("expected 'for' to be followed by a variable name, not '%s'",s->token));
		goto FAILURE;
	}
	variable = strdup(s->token);

	t = jx_scan(s);
	if (t != JX_TOKEN_IN) {
		jx_parse_error_a(s,string_format("expected 'for %s' to be followed by 'in', not '%s'",variable,s->token));
		goto FAILURE;
	}

	elements = jx_parse(s);
	if (!elements) {
		jx_parse_error_a(s, string_format("unexpected EOF following 'for %s'",variable));
		goto FAILURE;
	}

	t = jx_scan(s);
	if (t == JX_TOKEN_IF) {
		condition = jx_parse(s);
		if (!condition) {
			jx_parse_error_c(s, "unexpected EOF after 'if'" );
			goto FAILURE;
		}
	} else {
		jx_unscan(s, t);
	}

	result = jx_comprehension(
		variable,
		elements,
		condition,
		jx_parse_comprehension(s));
	result->line = line;
	free(variable);
	return result;

FAILURE:
	free(variable);
	jx_delete(elements);
	jx_delete(condition);
	jx_comprehension_delete(result);
	return NULL;
}

static struct jx_item *jx_parse_item_list(struct jx_parser *s, bool arglist)
{
	struct jx_item *head = 0;
	struct jx_item **tail = 0;

	/* Function arguments end with parens, but lists with brackets. */
	jx_token_t delim_token = arglist ? JX_TOKEN_RPAREN : JX_TOKEN_RBRACKET;

	while(1) {
		/* Check for an empty list, or a close brace following a trailing comma. */
		jx_token_t t = jx_scan(s);
		if(t==delim_token) return head;
		jx_unscan(s,t);

		struct jx_item *i = jx_item(NULL, NULL);
		i->line = s->line;

		/* Parse the next value in the list */

		i->value = jx_parse(s);
		if(!i->value) {
			// error set by deeper layer
			jx_item_delete(i);
			return head;
		}

		/* A value could be followed by a list comprehension */

		i->comp = jx_parse_comprehension(s);
		if (jx_parser_errors(s)) {
			// error set by deeper layer
			jx_item_delete(i);
			return head;
		}

		/* First item becomes the head, others added to the tail. */

		if(!head) {
			head = i;
		} else {
			*tail = i;
		}			

		/* Update the tail to the end of this pair. */

		tail = &i->next;

		/* Is this the end of the list or is there more? */

		t = jx_scan(s);
		if(t==JX_TOKEN_COMMA) {
			/* keep going */
		} else if(t==delim_token) {
			/* end of list */
			return head;
		} else {
			jx_parse_error_c(s,"list of items missing a comma or closing delimiter");
			return head;
		}
	}
}

static struct jx_pair * jx_parse_pair_list( struct jx_parser *s )
{
	struct jx_pair *head = 0;
	struct jx_pair **tail = 0;

	while(1) {
		/* Check for an empty list, or a close brace following a trailing comma. */
		jx_token_t t = jx_scan(s);
		if(t==JX_TOKEN_RBRACE) return head;
		jx_unscan(s,t);

		struct jx_pair *p = jx_pair(NULL, NULL, NULL);

		/* Parse the key of the pair, which should be a string */

		p->key = jx_parse(s);
		if(!p->key) {
			// error set by deeper layer
			jx_pair_delete(p);
			return head;
		}

		if(s->strict_mode) {
			if(p->key->type!=JX_STRING) {
				jx_parse_error_c(s,"key-value pair must have a string as the key");
				jx_pair_delete(p);
				return head;
			}
		}

		/* Now look for a colon and value to complete the pair. */

		t = jx_scan(s);
		if(t!=JX_TOKEN_COLON) {
			char *pstr = jx_print_string(p->key);
			jx_parse_error_a(s,string_format("key %s must be followed by a colon",pstr));
			free(pstr);
			jx_pair_delete(p);
			return head;
		}

		p->line = s->line;
		p->value = jx_parse(s);
		if(!p->value) {
			// error set by deeper layer
			jx_pair_delete(p);
			return head;
		}

		/* A value could be followed by a dict comprehension */

		p->comp = jx_parse_comprehension(s);
		if (jx_parser_errors(s)) {
			// error set by deeper layer
			jx_pair_delete(p);
			return head;
		}

		/* First item becomes the head, others added to the tail. */

		if(!head) {
			head = p;
		} else {
			*tail = p;
		}			

		/* Update the tail to the end of this pair. */

		tail = &p->next;

		/* Is this the end of the list, or is there more? */

		t = jx_scan(s);
		if(t==JX_TOKEN_COMMA) {
			/* keep going */
		} else if(t==JX_TOKEN_RBRACE) {
			/* end of list */
			return head;
		} else {
			jx_parse_error_c(s,"key-value pairs missing a comma or closing brace");
			return head;
		}
	}
}

static struct jx *jx_parse_atomic(struct jx_parser *s, bool arglist) {
	jx_token_t t = jx_scan(s);

	if (arglist) {
		if (t == JX_TOKEN_LPAREN) {
			t = JX_TOKEN_LBRACKET;
		} else {
			jx_parse_error_c(s,"function call missing opening parenthesis");
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
		case JX_TOKEN_TRUE: return jx_add_lineno(s, jx_boolean(true));
		case JX_TOKEN_FALSE: return jx_add_lineno(s, jx_boolean(false));
		case JX_TOKEN_NULL:
			return jx_add_lineno(s, jx_null());
		case JX_TOKEN_SYMBOL: {
			if(s->strict_mode) {
				jx_parse_error_a(s,string_format("unquoted strings (%s) are not allowed in strict parsing mode",s->token));
				return NULL;
			}
			return jx_add_lineno(s, jx_symbol(s->token));
		}
		case JX_TOKEN_LPAREN: {
			struct jx *j = jx_parse(s);
			if (!j) return NULL;

			t = jx_scan(s);
			if (t != JX_TOKEN_RPAREN) {
				jx_parse_error_c(s, "missing closing parenthesis");
				jx_delete(j);
				return NULL;
			}
			return j;
		}
		default: {
			char *str = string_format("unexpected token: %s",s->token);
			jx_parse_error_c(s,str);
			free(str);
			return NULL;
		}
	}

	/*
	We shouldn't get here, since all the token types
	should be handled above.  But just in case...
	*/

	jx_parse_error_c(s,"parse error");
	return NULL;
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
		case JX_OP_CALL: return 0;
		case JX_OP_DOT: return 0;
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
		case JX_TOKEN_C_AND:return JX_OP_AND;
		case JX_TOKEN_OR:	return JX_OP_OR;
		case JX_TOKEN_C_OR:	return JX_OP_OR;
		case JX_TOKEN_NOT:	return JX_OP_NOT;
		case JX_TOKEN_C_NOT:return JX_OP_NOT;
		case JX_TOKEN_LBRACKET:	return JX_OP_LOOKUP;
		case JX_TOKEN_LPAREN: return JX_OP_CALL;
		case JX_TOKEN_DOT: return JX_OP_DOT;
		default:		return JX_OP_INVALID;
	}
}

static bool jx_operator_is_unary(jx_operator_t op) {
	switch(op) {
		case JX_OP_NOT: return true;
		default: return false;
	}
}

/*
An array index can consist of a plain expression,
or a range of values separated by a colon, indicating
a slice of the indexed array.
*/

static struct jx *jx_parse_array_index(struct jx_parser *s)
{
	struct jx *left = NULL;
	struct jx *right = NULL;

	jx_token_t t = jx_scan(s);
	if (t == JX_TOKEN_COLON) {
		jx_unscan(s, t);
	} else {
		jx_unscan(s, t);
		left = jx_parse(s);
		// error set by deeper level
		if (!left) goto FAIL;
	}

	t = jx_scan(s);
	if (t != JX_TOKEN_COLON) {
		jx_unscan(s, t);
		return left;
	}
	unsigned line = s->line;

	t = jx_scan(s);
	if (t == JX_TOKEN_RBRACKET) {
		jx_unscan(s, t);
	} else {
		jx_unscan(s, t);
		right = jx_parse(s);
		// error set by deeper level
		if (!right) goto FAIL;
	}

	struct jx *result = jx_operator(JX_OP_SLICE, left, right);
	result->line = line;
	return result;
FAIL:
	jx_delete(left);
	jx_delete(right);
	return NULL;
}

/*
jx_parse_postfix_oper looks for zero or more postfix operators
(such as function arguments or array indexes) that follow an
atomic expression a.  This function will return either the
original expression a, or a postfix operator on the original
expression a.  On error, the expression a is deleted.
*/

static struct jx *jx_parse_postfix_oper(struct jx_parser *s, struct jx *a )
{
	jx_token_t t = jx_scan(s);
	switch (t) {
		case JX_TOKEN_LBRACKET: {
			unsigned line = s->line;

			// Parse the index expression inside the bracket.
			struct jx *b = jx_parse_array_index(s);
			if (!b) {
				jx_delete(a);
				return 0;
			}

			// Must be followed by a closing bracket.
			t = jx_scan(s);
			if (t != JX_TOKEN_RBRACKET) {
				jx_parse_error_c(s, "missing closing bracket");
				jx_delete(a);
				jx_delete(b);
				return NULL;
			}

			// Create a new expression on the two values.
			struct jx *j = jx_operator(JX_OP_LOOKUP, a, b);
			j->line = line;
			j->u.oper.line = line;

			// Multiple postfix operations can be stacked
			return jx_parse_postfix_oper(s,j);
		}
		case JX_TOKEN_LPAREN: {
			if (!jx_istype(a, JX_SYMBOL)) {
					jx_parse_error_c(s, "function names must be symbols");
					jx_delete(a);
					return NULL;
			}
			unsigned line = s->line;
			jx_unscan(s, t);

			// The left side must be a function name.
			if(!jx_istype(a,JX_SYMBOL)) {
				jx_parse_error_c(s, "function arguments () must follow a function name");
				jx_delete(a);
				return 0;
			}

			// Get the function arguments, including both parens.
			struct jx *args = jx_parse_atomic(s, true);
			if (!args) {
				jx_delete(a);
				return NULL; 
			}

			// Create a new expression on the two values.
			struct jx *j = jx_operator(JX_OP_CALL, a, args);
			j->line = line;
			j->u.oper.line = line;

			// Multiple postfix operations can be stacked
			return jx_parse_postfix_oper(s,j);
		}
		case JX_TOKEN_DOT: {
			// Get function name
			struct jx *func_name = jx_parse_atomic(s, false);
			if (!func_name || !jx_istype(func_name, JX_SYMBOL)) {
				jx_parse_error_c(s, "dot operator must be followed by a symbol");
				jx_delete(func_name);
				jx_delete(a);
				return NULL;
			}

			unsigned line = s->line;

			// Get the function arguments, including both parens.
			struct jx *args = jx_parse_atomic(s, true);
			if (!args) {
				jx_delete(a);
				return NULL;
			}

			// Create a new expression for the funcion call
			struct jx *call = jx_operator(JX_OP_CALL, func_name, args);
			call->line = line;
			call->u.oper.line = line;

			// Create a new expression for the anaphoric operation
			struct jx *j = jx_operator(JX_OP_DOT, a, call);

			// Multiple postfix operations can be stacked
			return jx_parse_postfix_oper(s,j);
		}
		default: {
			// No postfix operator, so return the atomic value.
			jx_unscan(s, t);
			return a;
		}
	}
}

/*
jx_parse_postfix_expr looks for an atomic expression,
followed by zero or more postfix operators, together
making a postfix expression.
*/

static struct jx *jx_parse_postfix_expr(struct jx_parser *s)
{
	struct jx *a = jx_parse_atomic(s, false);
	if(!a) return 0;

	return jx_parse_postfix_oper(s,a);
}

static struct jx * jx_parse_unary( struct jx_parser *s )
{
	jx_token_t t = jx_scan(s);
	switch (t) {
		case JX_TOKEN_SUB:
		case JX_TOKEN_ADD:
		case JX_TOKEN_C_NOT:
		case JX_TOKEN_NOT: {
			unsigned line = s->line;
			struct jx *j = jx_parse_unary(s);
			if (!j) {
				// error set by deeper level
				return NULL;
			}

			// For the special case of + or - followed by a numeric literal,
			// don't create an operator in the AST. This plain-JSON syntax
			// should result in a constant, so we negate as necessary here
			// and return just a number.
			if (t == JX_TOKEN_SUB && jx_istype(j, JX_INTEGER)) {
				j->u.integer_value *= -1;
			} else if (t == JX_TOKEN_SUB && jx_istype(j, JX_DOUBLE)) {
				j->u.double_value *= -1;
			} else if (t == JX_TOKEN_ADD && jx_istype(j, JX_INTEGER)) {
				// don't need to do anything here
			} else if (t == JX_TOKEN_ADD && jx_istype(j, JX_DOUBLE)) {
				// don't need to do anything here
			} else {
				j = jx_operator(jx_token_to_operator(t), NULL, j);
				j->u.oper.line = line;
			}
			j->line = line;
			return j;
		}
		case JX_TOKEN_ERROR: {
			unsigned line = s->line;

			t = jx_scan(s);
			if (t != JX_TOKEN_LPAREN) {
				jx_parse_error_c(s, "expected parentheses following error()");
				return NULL;
			}

			struct jx *j = jx_parse_postfix_expr(s);
			if (!j) {
				// error set by deeper level
				return NULL;
			}

			t = jx_scan(s);
			if (t != JX_TOKEN_RPAREN) {
				jx_delete(j);
				jx_parse_error_c(s, "expected closing parenthesis for error()");
				return NULL;
			}

			j = jx_error(j);
			j->line = line;
			j->u.err->line = line;
			return j;
		}
		default: {
			jx_unscan(s,t);
			return jx_parse_postfix_expr(s);
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

	if (!a)
		return NULL;

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
	struct jx *j = NULL;
	if (static_mode) {
		j = jx_parse_unary(s);
	} else {
		j = jx_parse_binary(s,JX_PRECEDENCE_MAX);
	}

	if (!j)
		return NULL;

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
		return NULL;
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
		return NULL;
	}
	return j;
}

struct jx * jx_parse_string( const char *str )
{
	struct jx_parser *p = jx_parser_create(false);
	jx_parser_read_string(p,str);
	return jx_parse_finish(p);
}

struct jx * jx_parse_link( struct link *l, time_t stoptime )
{
	struct jx_parser *p = jx_parser_create(false);
	jx_parser_read_link(p,l,stoptime);
	return jx_parse_finish(p);
}

struct jx * jx_parse_stream( FILE *file )
{
	struct jx_parser *p = jx_parser_create(false);
	jx_parser_read_stream(p,file);
	return jx_parse_finish(p);
}

struct jx * jx_parse_file( const char *name )
{
	FILE *file = fopen(name,"r");
	if (!file) {
		debug(D_JX, "Could not open jx file: %s", name);
		return NULL;
	}
	struct jx *j = jx_parse_stream(file);
	fclose(file);
	return j;
}

struct jx *jx_parse_cmd_args(struct jx *jx_args, char *args_file) {
	struct jx *jx_expr = NULL;
	struct jx *jx_tmp = NULL;
	struct jx *out = NULL;

	jx_expr = jx_parse_file(args_file);
	if (!jx_expr){
		debug(D_JX, "failed to parse context");
		goto FAILURE;
	}

	jx_tmp = jx_eval(jx_expr, jx_args);
	jx_delete(jx_expr);
	jx_expr = NULL;
	if (jx_istype(jx_tmp, JX_ERROR)) {
		debug(D_JX, "\nError in JX args");
		jx_print_stream(jx_tmp, stderr);
		goto FAILURE;
	}

	if (!jx_istype(jx_tmp, JX_OBJECT)){
		debug(D_JX, "Args file must contain a JX object");
		goto FAILURE;
	}

	out = jx_merge(jx_args, jx_tmp, NULL);
FAILURE:
	jx_delete(jx_expr);
	jx_delete(jx_args);
	jx_delete(jx_tmp);

	return out;
}

int jx_parse_cmd_define(struct jx *jx_args, char *define_stmt) {
	char *s;
    struct jx *jx_expr = NULL;
	struct jx *jx_tmp = NULL;
	struct jx *key = NULL;

	s = strchr(define_stmt, '=');
    if (!s){
        debug(D_JX, "JX variable must be of the form VAR=EXPR");
		return 0;
	}
    *s = '\0';
    jx_expr = jx_parse_string(s + 1);
    if (!jx_expr){
        debug(D_JX, "Invalid JX expression");
		return 0;
	}

	jx_tmp = jx_eval(jx_expr, jx_args);
	jx_delete(jx_expr);

	if (jx_istype(jx_tmp, JX_ERROR)) {
		debug(D_JX, "\nError in JX define");
		jx_print_stream(jx_tmp, stderr);
		jx_delete(jx_tmp);
		return 0;
	}

	key = jx_string(optarg);
	for (struct jx *r; (r = jx_remove(jx_args, key)); jx_delete(r));
	jx_insert(jx_args, key, jx_tmp);

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
