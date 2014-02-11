/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_EXPR_H
#define DELTADB_EXPR_H

#include "deltadb_value.h"

#include <stdio.h>

typedef enum {
	DELTADB_EXPR_ADD,
	DELTADB_EXPR_SUB,
	DELTADB_EXPR_MUL,
	DELTADB_EXPR_DIV,
	DELTADB_EXPR_MOD,
	DELTADB_EXPR_POW,
	DELTADB_EXPR_NEG,
	DELTADB_EXPR_AND,
	DELTADB_EXPR_OR,
	DELTADB_EXPR_NOT,
	DELTADB_EXPR_LT,
	DELTADB_EXPR_LE,
	DELTADB_EXPR_EQ,
	DELTADB_EXPR_NE,
	DELTADB_EXPR_GT,
	DELTADB_EXPR_GE,
	DELTADB_EXPR_LIST,
	DELTADB_EXPR_VALUE,
	DELTADB_EXPR_SYMBOL,
	DELTADB_EXPR_FCALL
} deltadb_expr_type_t;

struct deltadb_expr {
	deltadb_expr_type_t type;
	struct deltadb_expr *left;
	struct deltadb_expr *right;
	struct deltadb_value *value;
	const char *symbol;
	struct deltadb_expr *next;
};

struct deltadb_expr * deltadb_expr_create( deltadb_expr_type_t type, struct deltadb_expr *left, struct deltadb_expr *right );
struct deltadb_expr * deltadb_expr_create_symbol( const char *n );
struct deltadb_expr * deltadb_expr_create_list( struct deltadb_expr *list );
struct deltadb_expr * deltadb_expr_create_value( struct deltadb_value *value );
struct deltadb_expr * deltadb_expr_create_fcall( const char *fname, struct deltadb_expr *args );

void deltadb_expr_print( FILE *file, struct deltadb_expr *e );
void deltadb_expr_delete( struct deltadb_expr *e );

struct deltadb_value * deltadb_expr_eval( struct deltadb_expr *e );

#endif
