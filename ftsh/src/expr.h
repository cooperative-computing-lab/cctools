/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef EXPR_H
#define EXPR_H

#include "ast.h"
#include <stdio.h>
#include <sys/time.h>

struct ast_word;

enum expr_type_t {
	EXPR_ADD,
	EXPR_SUB,
	EXPR_MUL,
	EXPR_DIV,
	EXPR_MOD,
	EXPR_POW,

	EXPR_TO,

	EXPR_EQ,
	EXPR_NE,
	EXPR_EQL,
	EXPR_NEQL,
	EXPR_LT,
	EXPR_LE,
	EXPR_GT,
	EXPR_GE,
	EXPR_AND,
	EXPR_OR,
	EXPR_NOT,

	EXPR_EXISTS,
	EXPR_ISR,
	EXPR_ISW,
	EXPR_ISX,

	EXPR_ISBLOCK,
	EXPR_ISCHAR,
	EXPR_ISDIR,
	EXPR_ISFILE,
	EXPR_ISLINK,
	EXPR_ISPIPE,
	EXPR_ISSOCK,

	EXPR_LITERAL,
	EXPR_FCALL,
	EXPR_EXPR
};

typedef long ftsh_integer_t;
typedef int ftsh_boolean_t;

struct expr {
	int line;
	enum expr_type_t type;
	struct expr *a, *b, *c;
	struct ast_word *literal;
	struct expr *next;
};

struct expr * expr_create( int line, enum expr_type_t type, struct ast_word *literal, struct expr *a, struct expr *b, struct expr *c );

char * expr_eval( struct expr *e, time_t stoptime );
int expr_to_boolean( struct expr *e, ftsh_boolean_t *b, time_t stoptime );
int expr_to_integer( struct expr *e, ftsh_integer_t *i, time_t stoptime );
int expr_is_list( struct expr *e );

void expr_print( FILE *file, struct expr *e );

#endif
