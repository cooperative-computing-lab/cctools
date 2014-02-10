/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_scanner.h"

#include <stdlib.h>
#include <string.h>

#define DELTADB_TOKEN_LENGTH_MAX 1024

struct deltadb_scanner {
	const char *string_source;
	const char *string_position;
	FILE *file_source;	
	char token_data[DELTADB_TOKEN_LENGTH_MAX];
	deltadb_token_t lookahead;
	int lookahead_avail;
};

struct deltadb_scanner * deltadb_scanner_create_from_string( const char *str )
{
	struct deltadb_scanner *s = malloc(sizeof(*s));
	s->string_source = str;
	s->string_position = str;
	s->file_source = 0;
	s->lookahead_avail =0;
	return s;
}

struct deltadb_scanner * deltadb_scanner_create_from_file( FILE * file )
{
	struct deltadb_scanner *s = malloc(sizeof(*s));
	s->string_source = 0;
	s->string_position = 0;
	s->file_source = file;
	s->lookahead_avail = 0;
	return s;
}

static int isalphauc( char c )
{
	return (c>='a' && c<='z') || (c>='A' && c<='Z') || c=='_';
}

static int isalphanumuc( char c )
{
	return (c>='0' && c<='9') || (c>='a' && c<='z') || (c>='A' && c<='Z') || c=='_';
}

static int isdigitdot( char c )
{
	return (c>='0' && c<='9') || c=='.';
}

static void deltadb_scanner_unget_char( struct deltadb_scanner *s, char c )
{
	if(c==0) return;  // cannot unget EOF
	if(s->string_source) {
		s->string_position--;
	} else {
		ungetc(c,s->file_source);
	}
}

static char deltadb_scanner_get_char( struct deltadb_scanner *s )
{
	char c;

	if(s->string_source) {
		c = *s->string_position;
		if(c) s->string_position++;
		return c;
	} else {
		c = fgetc(s->file_source);
		if(feof(s->file_source)) {
			return 0;
		} else {
			return c;
		}
	}
}

deltadb_token_t deltadb_scanner_get_token( struct deltadb_scanner *s )
{
	int i;

	while(1) {
		char c = deltadb_scanner_get_char(s);

		switch(c) {
			case ' ':
			case '\t':
			case '\n':
					continue;
			case 0:		return DELTADB_TOKEN_EOF;
			case '{':	return DELTADB_TOKEN_LBRACE;
			case '}':	return DELTADB_TOKEN_RBRACE;
			case '(':	return DELTADB_TOKEN_LPAREN;
			case ')':	return DELTADB_TOKEN_RPAREN;
			case '[':	return DELTADB_TOKEN_LBRACKET;
			case ']':	return DELTADB_TOKEN_RBRACKET;
			case '+':	return DELTADB_TOKEN_ADD;
			case '-':	return DELTADB_TOKEN_SUB;
			case '*':	return DELTADB_TOKEN_MUL;
			case '/':	return DELTADB_TOKEN_DIV;
			case '%':	return DELTADB_TOKEN_MOD;
			case '^':	return DELTADB_TOKEN_POW;
			case ',':	return DELTADB_TOKEN_COMMA;
		}

		if(c=='!') {
			c = deltadb_scanner_get_char(s);
			if(c=='=') {
				return DELTADB_TOKEN_NE;
			} else {
				deltadb_scanner_unget_char(s,c);
				return DELTADB_TOKEN_NOT;
			}
		}

		if(c=='<') {
			c = deltadb_scanner_get_char(s);
			if(c=='=') {
				return DELTADB_TOKEN_LE;
			} else {
				deltadb_scanner_unget_char(s,c);
				return DELTADB_TOKEN_LT;
			}
		}

		if(c=='>') {
			c = deltadb_scanner_get_char(s);
			if(c=='=') {
				return DELTADB_TOKEN_GE;
			} else {
				deltadb_scanner_unget_char(s,c);
				return DELTADB_TOKEN_GT;
			}
		}

		if(c=='=') {
			c = deltadb_scanner_get_char(s);
			if(c=='=') {
				return DELTADB_TOKEN_EQ;
			} else {
				deltadb_scanner_unget_char(s,c);
				return DELTADB_TOKEN_ERROR;
			}
		}

		if(c=='&') {
			c = deltadb_scanner_get_char(s);
			if(c=='&') {
				return DELTADB_TOKEN_AND;
			} else {
				return DELTADB_TOKEN_ERROR;
			}
		}

		if(c=='|') {
			c = deltadb_scanner_get_char(s);
			if(c=='|') {
				return DELTADB_TOKEN_OR;
			} else {
				return DELTADB_TOKEN_ERROR;
			}
		}

		if(c=='|') {
			c = deltadb_scanner_get_char(s);
			if(c=='|') {
				return DELTADB_TOKEN_OR;
			} else {
				return DELTADB_TOKEN_ERROR;
			}
		}

		if(isalphauc(c)) {
			s->token_data[0] = c;
			for(i=1;i<DELTADB_TOKEN_LENGTH_MAX;i++) {
				c = deltadb_scanner_get_char(s);
				if(!isalphanumuc(c)) {
					deltadb_scanner_unget_char(s,c);
					break;
				} else {
					s->token_data[i] = c;
					continue;
				}
			}
			s->token_data[i] = 0;
			if(!strcmp(s->token_data,"true")) {
				return DELTADB_TOKEN_TRUE;
			} else if(!strcmp(s->token_data,"false")) {
				return DELTADB_TOKEN_FALSE;
			} else {
				return DELTADB_TOKEN_SYMBOL;
			}
		}

		if(isdigitdot(c)) {
			s->token_data[0] = c;
			for(i=1;i<DELTADB_TOKEN_LENGTH_MAX;i++) {
				c = deltadb_scanner_get_char(s);
				if(!isdigitdot(c)) {
					deltadb_scanner_unget_char(s,c);
					break;
				} else {
					s->token_data[i] = c;
					continue;
				}
			}
			s->token_data[i] = 0;
			if(strchr(s->token_data,'.')) {
				return DELTADB_TOKEN_REAL_LITERAL;
			} else {
				return DELTADB_TOKEN_INTEGER_LITERAL;
			}
		}

		if(c=='\"') {
			for(i=0;i<DELTADB_TOKEN_LENGTH_MAX;i++) {
				c = deltadb_scanner_get_char(s);
				if(c=='\"') {
					s->token_data[i] = 0;
					return DELTADB_TOKEN_STRING_LITERAL;
				} else if(c==0) {
					return DELTADB_TOKEN_ERROR;
				} else if(c=='\\') {
					c = deltadb_scanner_get_char(s);
					if(c==0) {
						return DELTADB_TOKEN_ERROR;
					} else {
						s->token_data[i] = c;
					}
				} else {
					s->token_data[i] = c;
				}
			}
			return DELTADB_TOKEN_ERROR;
		}

		if(c=='\'') {
			for(i=0;i<DELTADB_TOKEN_LENGTH_MAX;i++) {
				c = deltadb_scanner_get_char(s);
				if(c==0) {
					return DELTADB_TOKEN_ERROR;
				} else if(c=='\\') {
					c = deltadb_scanner_get_char(s);
					if(c==0) {
						return DELTADB_TOKEN_ERROR;
					} else {
						s->token_data[i] = c;
					}
				} else {
					s->token_data[i] = c;
				}
			}
			return DELTADB_TOKEN_ERROR;
		}

		return DELTADB_TOKEN_ERROR;
	}
}

deltadb_token_t deltadb_scanner_lookahead( struct deltadb_scanner *s )
{
	if(!s->lookahead_avail) {
		s->lookahead = deltadb_scanner_get_token(s);
		s->lookahead_avail = 1;
	}

	return s->lookahead;
}

int deltadb_scanner_accept( struct deltadb_scanner *s, deltadb_token_t t )
{
	if(t==deltadb_scanner_lookahead(s)) {
		s->lookahead_avail = 0;
		return 1;
	} else {
		return 0;
	}
}

int deltadb_scanner_expect( struct deltadb_scanner *s, deltadb_token_t t )
{
	if(deltadb_scanner_accept(s,t)) {
		return 1;
	} else {
		// XXX this should move to the parser so that we can generate a proper error.
	  fprintf(stderr,"expected %s but found %s instead",deltadb_scanner_get_token_name(t),deltadb_scanner_get_token_name(s->lookahead));
		return 0;
	}
}

char * deltadb_scanner_get_string_value( struct deltadb_scanner *s )
{
	return s->token_data;
}

char * deltadb_scanner_get_file_value( struct deltadb_scanner *s )
{
	return s->token_data;
}

int    deltadb_scanner_get_integer_value( struct deltadb_scanner *s )
{
	return atoi(s->token_data);
}

double deltadb_scanner_get_real_value( struct deltadb_scanner *s )
{
	return atof(s->token_data);
}

void   deltadb_scanner_delete( struct deltadb_scanner *s )
{
	free(s);
}

char * deltadb_scanner_get_token_name( deltadb_token_t t )
{
	switch(t) {
		case DELTADB_TOKEN_STRING_LITERAL:	return "string";
		case DELTADB_TOKEN_INTEGER_LITERAL:	return "integer";
		case DELTADB_TOKEN_REAL_LITERAL:	return "real";

		case DELTADB_TOKEN_LBRACE:	return "{";
		case DELTADB_TOKEN_RBRACE:	return "}";
		case DELTADB_TOKEN_LPAREN:	return "(";
		case DELTADB_TOKEN_RPAREN:	return ")";
		case DELTADB_TOKEN_LBRACKET:	return "[";
		case DELTADB_TOKEN_RBRACKET:	return "]";
		case DELTADB_TOKEN_COMMA:	return ",";

		case DELTADB_TOKEN_ADD:		return "+";
		case DELTADB_TOKEN_SUB:		return "-";
		case DELTADB_TOKEN_MUL:		return "*";
		case DELTADB_TOKEN_DIV:		return "/";
		case DELTADB_TOKEN_MOD:		return "%";
		case DELTADB_TOKEN_POW:		return "^";
	
		case DELTADB_TOKEN_LT:		return "<";
		case DELTADB_TOKEN_LE:		return "<=";
		case DELTADB_TOKEN_GT:		return ">";
		case DELTADB_TOKEN_GE:		return ">=";
		case DELTADB_TOKEN_EQ:		return "==";
		case DELTADB_TOKEN_NE:		return "!=";

		case DELTADB_TOKEN_AND:		return "&&";
		case DELTADB_TOKEN_OR:		return "||";
		case DELTADB_TOKEN_NOT:		return "!";

		case DELTADB_TOKEN_TRUE:	return "TRUE";
		case DELTADB_TOKEN_FALSE:	return "FALSE";
		case DELTADB_TOKEN_SYMBOL:	return "SYMBOL";
		case DELTADB_TOKEN_ERROR:	return "ERROR";
		case DELTADB_TOKEN_EOF:		return "EOF";
		default:		return "UNKNOWN";
	}
}


