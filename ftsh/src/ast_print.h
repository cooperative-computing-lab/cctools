/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef AST_PRINT_H
#define AST_PRINT_H

#include "ast.h"
#include <stdio.h>

void ast_group_print( FILE *file, struct ast_group *g, int level );
void ast_command_print( FILE *file, struct ast_command *c, int level );
void ast_function_print( FILE *file, struct ast_function *f, int level );
void ast_conditional_print( FILE *file, struct ast_conditional *c, int level );
void ast_try_print( FILE *file, struct ast_try *t, int level );
void ast_try_limit_print( FILE *file, struct ast_try_limit *l, const char *prefix );
void ast_whileloop_print( FILE *file, struct ast_whileloop *l, int level );
void ast_forloop_print( FILE *file, struct ast_forloop *f, int level );
void ast_shift_print( FILE *file, struct ast_shift *s, int level );
void ast_return_print( FILE *file, struct ast_return *s, int level );
void ast_assign_print( FILE *file, struct ast_assign *s, int level );
void ast_simple_print( FILE *file, struct ast_simple *s, int level );
void ast_redirect_print( FILE *file, struct ast_redirect *r  );
void ast_word_print( FILE *file, struct ast_word *w );

#endif

