/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PARSER_H
#define PARSER_H

#include "ast.h"
#include <stdio.h>

union yystype {
	struct ast_group *group;
	struct ast_command *command;
	struct ast_function *function;
	struct ast_conditional *conditional;
	struct ast_try *try;
	struct ast_try_limit *try_limit;
	struct ast_whileloop *whileloop;
	struct ast_forloop *forloop;
	struct ast_shift *shift;
	struct ast_return *rtn;
	struct ast_assign *assign;
	struct ast_simple *simple;
	struct ast_redirect *redirect;
	struct ast_word *word;
	struct ast_token token;
	struct expr *expr;
	int number;
};

#define YYSTYPE union yystype

struct ast_group * parse_file( FILE *file, int do_debug );

#endif
