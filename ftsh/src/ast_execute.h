/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef AST_EXECUTE_H
#define AST_EXECUTE_H

#include "ast.h"
#include <time.h>

char * ast_function_execute( int line, int argc, char **argv, time_t stoptime );

int ast_program_execute( struct ast_group *g, time_t stoptime );
int ast_group_execute( struct ast_group *g, time_t stoptime );
int ast_command_execute( struct ast_command *s, time_t stoptime );
int ast_conditional_execute( struct ast_conditional *c, time_t stoptime );
int ast_try_execute( struct ast_try *t, time_t stoptime );
int ast_whileloop_execute( struct ast_whileloop *f, time_t stoptime );
int ast_forloop_execute( struct ast_forloop *f, time_t stoptime );
int ast_shift_execute( struct ast_shift *s, time_t stoptime );
int ast_return_execute( struct ast_return *s, time_t stoptime );
int ast_assign_execute( struct ast_assign *s, time_t stoptime );
int ast_simple_execute( struct ast_simple *s, time_t stoptime );
char * ast_word_execute( int line, struct ast_word *w );
char * ast_word_list_execute( int line, struct ast_word *w );
int ast_number_execute( int line, struct ast_word *w, int *value );

#endif
