/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AST_H
#define AST_H

#include "expr.h"

struct ast_group {
	struct ast_command *command;
	struct ast_group *next;
};

struct ast_command {
	int type;
	union {
		struct ast_function *function;
		struct ast_conditional *conditional;
		struct ast_try *try;
		struct ast_forloop *forloop;
		struct ast_whileloop *whileloop;
		struct ast_simple *simple;
		struct ast_assign *assign;
		struct ast_shift *shift;
		struct ast_return *rtn;
		void *data;
	} u;
};

#define AST_COMMAND_FUNCTION 0
#define AST_COMMAND_CONDITIONAL 1
#define AST_COMMAND_TRY 2
#define AST_COMMAND_FORLOOP 3
#define AST_COMMAND_WHILELOOP 4
#define AST_COMMAND_SIMPLE 5
#define AST_COMMAND_SHIFT 6
#define AST_COMMAND_ASSIGN 7
#define AST_COMMAND_EMPTY 8
#define AST_COMMAND_RETURN 9

#define AST_FOR    0
#define AST_FORANY 1
#define AST_FORALL 2

struct ast_function {
	int function_line, end_line;
	struct ast_word *name;
	struct ast_group *body;
};

struct ast_conditional {
	int if_line, then_line, else_line, end_line;
	struct expr *expr;
	struct ast_group *positive;
	struct ast_group *negative;
};

struct ast_try {
	int try_line, catch_line, end_line;
	struct ast_try_limit *loop_limit;
	struct ast_try_limit *time_limit;
	struct ast_try_limit *every_limit;
	struct ast_group *body;
	struct ast_group *catch_block;
};

struct ast_try_limit {
	struct expr *expr;
	int units;
};

struct ast_whileloop {
	int while_line, do_line, end_line;
	struct expr *expr;
	struct ast_group *body;
};

struct ast_forloop {
	int type;
	int for_line, end_line;
	struct ast_word *name;
	struct expr *list;
	struct ast_group *body;
};

struct ast_shift {
	int line;
	struct expr *expr;
};

struct ast_return {
	int line;
	struct expr *expr;
};

struct ast_simple {
	int line;
	struct ast_word     *words;
	struct ast_redirect *redirects;
};

struct ast_assign {
	int line;
	struct ast_word *name;
	struct expr *expr;
};

struct ast_redirect {
	int kind;
	int mode;
	int source;
	int actual;
	struct ast_word *target;
	struct ast_redirect *next;
};

#define AST_REDIRECT_FILE 0
#define AST_REDIRECT_BUFFER 1
#define AST_REDIRECT_FD 2

#define AST_REDIRECT_INPUT 0
#define AST_REDIRECT_OUTPUT 1
#define AST_REDIRECT_APPEND 2

struct ast_word {
	int line;
	char *text;
	struct ast_word *next;
};

struct ast_token {
	int line;
	int type;
	int firstint;
};

struct ast_group       * ast_group_create( struct ast_command *s, struct ast_group *next );
struct ast_command     * ast_command_create( int type, void *data );
struct ast_function    * ast_function_create( int fline, int eline, struct ast_word *name, struct ast_group *body );
struct ast_conditional * ast_conditional_create( int iline, int tline, int eline, int end_line, struct expr *expr, struct ast_group *positive, struct ast_group *negative );
struct ast_try         * ast_try_create( int try_line, int catch_line, int end_line, struct ast_try_limit *time_limit,  struct ast_try_limit *loop_limit,  struct ast_try_limit *every_limit, struct ast_group *body, struct ast_group *catch_block );
struct ast_try_limit   * ast_try_limit_create( struct expr *expr, int units );
struct ast_whileloop   * ast_whileloop_create( int while_line, int do_line, int end_line, struct expr *expr, struct ast_group *body );
struct ast_forloop     * ast_forloop_create( int type, int for_line, int end_line, struct ast_word *name, struct expr *list, struct ast_group *body );
struct ast_shift       * ast_shift_create( int line, struct expr *expr );
struct ast_return      * ast_return_create( int line, struct expr *expr );
struct ast_assign      * ast_assign_create( int line, struct ast_word *name, struct expr *expr );
struct ast_simple      * ast_simple_create( int line, struct ast_word *words, struct ast_redirect *redirects );
struct ast_redirect    * ast_redirect_create( int kind, int source, struct ast_word *target, int mode );
struct ast_word        * ast_word_create( int line, const char *text );

#endif

