/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_SCANNER_H
#define DELTADB_SCANNER_H

#include <stdio.h>

typedef enum {
	DELTADB_TOKEN_STRING_LITERAL,
	DELTADB_TOKEN_INTEGER_LITERAL,
	DELTADB_TOKEN_REAL_LITERAL,

	DELTADB_TOKEN_LBRACE,
	DELTADB_TOKEN_RBRACE,
	DELTADB_TOKEN_LPAREN,
	DELTADB_TOKEN_RPAREN,
	DELTADB_TOKEN_LBRACKET,
	DELTADB_TOKEN_RBRACKET,
	DELTADB_TOKEN_COMMA,

	DELTADB_TOKEN_ADD,
	DELTADB_TOKEN_SUB,
	DELTADB_TOKEN_MUL,
	DELTADB_TOKEN_DIV,
	DELTADB_TOKEN_MOD,
	DELTADB_TOKEN_POW,
	
	DELTADB_TOKEN_LT,
	DELTADB_TOKEN_LE,
	DELTADB_TOKEN_GT,
	DELTADB_TOKEN_GE,
	DELTADB_TOKEN_EQ,
	DELTADB_TOKEN_NE,

	DELTADB_TOKEN_AND,
	DELTADB_TOKEN_OR,
	DELTADB_TOKEN_NOT,

	DELTADB_TOKEN_TRUE,
	DELTADB_TOKEN_FALSE,
	DELTADB_TOKEN_SYMBOL,

	DELTADB_TOKEN_ERROR,
	DELTADB_TOKEN_EOF
} deltadb_token_t;

struct deltadb_scanner * deltadb_scanner_create_from_string( const char *str );
struct deltadb_scanner * deltadb_scanner_create_from_file( FILE * file );

deltadb_token_t deltadb_scanner_lookahead( struct deltadb_scanner *s );
int     deltadb_scanner_accept( struct deltadb_scanner *s, deltadb_token_t t );
int     deltadb_scanner_expect( struct deltadb_scanner *s, deltadb_token_t t );

char * deltadb_scanner_get_string_value( struct deltadb_scanner *s );
char * deltadb_scanner_get_file_value( struct deltadb_scanner *s );
int    deltadb_scanner_get_integer_value( struct deltadb_scanner *s );
double deltadb_scanner_get_real_value( struct deltadb_scanner *s );
char * deltadb_scanner_get_token_name( deltadb_token_t t );

void   deltadb_scanner_delete( struct deltadb_scanner *s );

#endif
