/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_parse.h"

#include "stringtools.h"

#include <ctype.h>
#include <string.h>
#include <stdio.h>

typedef enum {
	JX_TOKEN_SYMBOL,
	JX_TOKEN_INTEGER,
	JX_TOKEN_FLOAT,
	JX_TOKEN_STRING,
	JX_TOKEN_LBRACKET,
	JX_TOKEN_RBRACKET,
	JX_TOKEN_LBRACE,
	JX_TOKEN_RBRACE,
	JX_TOKEN_COMMA,
	JX_TOKEN_COLON,
	JX_TOKEN_TRUE,
	JX_TOKEN_FALSE,
	JX_TOKEN_NULL,
	JX_TOKEN_ERROR,
	JX_TOKEN_EOF,
} jx_token_t;

#define MAX_TOKEN_SIZE 4096

struct jx_parser {
	char token[MAX_TOKEN_SIZE];
	FILE *source_file;
	const char *source_string;
	int errors;
};

static struct jx_parser * jx_parser_create()
{
	struct jx_parser *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));
	return p;
}

static void jx_parser_delete( struct jx_parser *p )
{
	free(p);
}

static void jx_parse_error( struct jx_parser *p )
{
	p->errors++;
}

static int jx_getchar( struct jx_parser *p )
{
	int c;

	if(p->source_file) {
		c = fgetc(p->source_file);
	} else {
		c = *p->source_string;
		if(c) {
			p->source_string++;
		} else {
			c = EOF;
		}
	}

	return c;
}

static void jx_ungetchar( struct jx_parser *p, int c )
{
	if(p->source_file) {
		ungetc(c,p->source_file);
	} else {
		p->source_string--;
	}
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
			return uc;
		} else {
			jx_parse_error(s);
			return -1;
		}
	} else {
		jx_parse_error(s);
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

static jx_token_t jx_scan( struct jx_parser *s )
{
	int c;

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
	} else if(c=='\"') {
		int i;
		for(i=0;i<MAX_TOKEN_SIZE;i++) {
			int n = jx_scan_string_char(s);
			if(n==EOF) {
				return JX_TOKEN_ERROR;
			} else if(n==0) {
				s->token[i] = n;
				return JX_TOKEN_STRING;
			} else {
				s->token[i] = n;
			}
		}
		jx_parse_error(s);
		return JX_TOKEN_ERROR;
	} else if(strchr("+-0123456789.",c)) {
		s->token[0] = c;
		int i;
		for(i=1;i<MAX_TOKEN_SIZE;i++) {
			c = jx_getchar(s);
			if(strchr("0123456789.",c)) {
				s->token[i] = c;
			} else {
				s->token[i] = 0;
				jx_ungetchar(s,c);
				if(strchr(s->token,'.')) {
					return JX_TOKEN_FLOAT;
				} else {
					return JX_TOKEN_INTEGER;
				}
			}
		}
		jx_parse_error(s);
		return JX_TOKEN_ERROR;
	} else if(isalpha(c)) {
		s->token[0] = c;
		int i;
		for(i=1;i<MAX_TOKEN_SIZE;i++) {
			c = jx_getchar(s);
			if(isalpha(c)) {	
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
				} else {
					return JX_TOKEN_SYMBOL;
				}
			}
		}
		jx_parse_error(s);
		return JX_TOKEN_ERROR;
	} else {
		s->token[0] = c;
		s->token[1] = 0;
		return JX_TOKEN_ERROR;
	}
}

static struct jx * jx_parse( struct jx_parser *s );

static struct jx_item * jx_parse_item_list( struct jx_parser *s )
{
	struct jx_item *i = jx_item(0,0);

	i->value = jx_parse(s);
	if(!i->value) {
		// error set by deeper layer
		jx_item_delete(i);
		return 0;
	}

	jx_token_t t = jx_scan(s);
	if(t==JX_TOKEN_COMMA) {
		i->next = jx_parse_item_list(s);
	} else if(t==JX_TOKEN_RBRACKET) {
		i->next = 0;
	} else {
		jx_parse_error(s);
	}

	return i;
}
		
static struct jx_pair * jx_parse_pair_list( struct jx_parser *s )
{
	struct jx_pair *p = jx_pair(0,0,0);

	p->key = jx_parse(s);
	if(!p->key) {
		// error set by deeper layer
		jx_pair_delete(p);
		return 0;
	}

	jx_token_t t = jx_scan(s);
	if(t!=JX_TOKEN_COLON) {
		jx_parse_error(s);
		jx_pair_delete(p);
		return 0;
	}

	p->value = jx_parse(s);
	if(!p->value) {
		// error set by deeper layer
		jx_pair_delete(p);
		return 0;
	}

	t = jx_scan(s);
	if(t==JX_TOKEN_COMMA) {
		p->next = jx_parse_pair_list(s);
	} else if(t==JX_TOKEN_RBRACE) {
		p->next = 0;
	} else {
		jx_parse_error(s);
	}

	return p;
}
		
static struct jx * jx_parse( struct jx_parser *s )
{
	jx_token_t t = jx_scan(s);

	switch(t) {
	case JX_TOKEN_EOF:
		return 0;
	case JX_TOKEN_LBRACE:
		return jx_object(jx_parse_pair_list(s));
	case JX_TOKEN_LBRACKET:
		return jx_array(jx_parse_item_list(s));
	case JX_TOKEN_STRING:
		return jx_string(strdup(s->token));
	case JX_TOKEN_INTEGER:
		return jx_integer(atoi(s->token));
	case JX_TOKEN_FLOAT:
		return jx_float(atof(s->token));
	case JX_TOKEN_TRUE:
		return jx_boolean(1);
	case JX_TOKEN_FALSE:
		return jx_boolean(0);
	case JX_TOKEN_NULL:
		return jx_null();
	case JX_TOKEN_SYMBOL:
		return jx_symbol(s->token);
	case JX_TOKEN_RBRACE:
	case JX_TOKEN_RBRACKET:
	case JX_TOKEN_COMMA:
	case JX_TOKEN_COLON:
	case JX_TOKEN_ERROR:
		jx_parse_error(s);
		return 0;
	}

	/*
	We shouldn't get here, since all the token types
	should be handled above.  But just in case...
	*/

	jx_parse_error(s);
	return 0;
}

struct jx * jx_parse_string( const char *str )
{
	struct jx_parser *s = jx_parser_create();
       	s->source_string = str;
	struct jx * result = jx_parse(s);
	jx_parser_delete(s);
	if(s->errors) {
		jx_delete(result);
		return 0;
	} else {
		return result;
	}
}

struct jx * jx_parse_file( FILE *file )
{
	struct jx_parser *s = jx_parser_create();
	s->source_file = file;
	struct jx * result = jx_parse(s);
	if(s->errors) {
		jx_parser_delete(s);
		jx_delete(result);
		return 0;
	} else {
		jx_parser_delete(s);
		return result;
	}
}
