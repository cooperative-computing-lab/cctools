
%expect 1

/*
The "expect" directive indicates that this grammar has
a known ambiguity which is resolved by the default
precedence in bison.

The ambiguity is as follows:  A single function call can
be confused with two expressions separated by a space.
This conflict does not apear in singleton expressions,
but only in space-separated lists, used by forany and forall.
For example, "foo ( baz )" could be interpreted as a
call to function foo, or the literal expression
foo followed by baz.

I belive this is an acceptable ambiguity, because the
default is the common case and can be overridden when
necessary.  For example,

	forany x in foo(baz)
		...
	end

...indicates that the function foo should be called and the
resulting list be used for the branches of the forany.
To force the expression-list interpretation, use this instead:

	forany x in (foo) (baz)
		...
	end
*/

%token TOKEN_DELIMITER
%token TOKEN_LEFT_ARROW
%token TOKEN_DOUBLE_LEFT_ARROW
%token TOKEN_LONG_LEFT_ARROW
%token TOKEN_LONG_DOUBLE_LEFT_ARROW
%token TOKEN_RIGHT_ARROW
%token TOKEN_DOUBLE_RIGHT_ARROW
%token TOKEN_SQUIGGLY_RIGHT_ARROW
%token TOKEN_NUMBERED_SQUIGGLY_RIGHT_ARROW
%token TOKEN_DOUBLE_SQUIGGLY_RIGHT_ARROW
%token TOKEN_NUMBERED_DOUBLE_SQUIGGLY_RIGHT_ARROW
%token TOKEN_LONG_RIGHT_ARROW
%token TOKEN_LONG_DOUBLE_RIGHT_ARROW
%token TOKEN_LONG_SQUIGGLY_RIGHT_ARROW
%token TOKEN_LONG_DOUBLE_SQUIGGLY_RIGHT_ARROW
%token TOKEN_FUNCTION
%token TOKEN_ASSIGN
%token TOKEN_IF
%token TOKEN_ELSE
%token TOKEN_END
%token TOKEN_TRY
%token TOKEN_EVERY
%token TOKEN_CATCH
%token TOKEN_IN
%token TOKEN_FOR
%token TOKEN_FORANY
%token TOKEN_FORALL
%token TOKEN_TIMES
%token TOKEN_SECONDS
%token TOKEN_MINUTES
%token TOKEN_HOURS
%token TOKEN_DAYS
%token TOKEN_ATOM
%token TOKEN_SHIFT
%token TOKEN_WHILE
%token TOKEN_VAR
%token TOKEN_RETURN
%token TOKEN_COMMA

%token TOKEN_ADD
%token TOKEN_SUB
%token TOKEN_MUL
%token TOKEN_DIV
%token TOKEN_MOD
%token TOKEN_POW
%token TOKEN_EQ
%token TOKEN_NE
%token TOKEN_EQL
%token TOKEN_NEQL
%token TOKEN_LT
%token TOKEN_GT
%token TOKEN_LE
%token TOKEN_GE
%token TOKEN_AND
%token TOKEN_OR
%token TOKEN_QUESTION
%token TOKEN_COLON
%token TOKEN_LPAREN
%token TOKEN_RPAREN
%token TOKEN_NOT
%token TOKEN_TO
%token TOKEN_STEP

%token TOKEN_EXISTS
%token TOKEN_ISR
%token TOKEN_ISW
%token TOKEN_ISX

%token TOKEN_ISBLOCK
%token TOKEN_ISCHAR
%token TOKEN_ISDIR
%token TOKEN_ISFILE
%token TOKEN_ISLINK
%token TOKEN_ISPIPE
%token TOKEN_ISSOCK

%type <group> program group
%type <command> command
%type <function> function
%type <try> try try_limit
%type <try_limit> time_limit loop_limit opt_every_limit
%type <forloop> forloop
%type <whileloop> whileloop
%type <conditional> conditional
%type <shift> shift_line
%type <rtn> return_line
%type <assign> assign_line
%type <simple> simple_line simple
%type <redirect> redirect
%type <number> time_units fortype
%type <word> word wordequals
%type <expr> expr expr_list opt_expr_comma_list expr_comma_list
%type <token> TOKEN_FUNCTION TOKEN_END TOKEN_DELIMITER TOKEN_WHILE TOKEN_FOR TOKEN_IN TOKEN_IF TOKEN_ELSE TOKEN_TRY TOKEN_CATCH TOKEN_ASSIGN TOKEN_ATOM TOKEN_OR TOKEN_AND TOKEN_EQ TOKEN_NE TOKEN_EQL TOKEN_NEQL TOKEN_LT TOKEN_LE TOKEN_GT TOKEN_GE TOKEN_ADD TOKEN_SUB TOKEN_MUL TOKEN_DIV TOKEN_MOD TOKEN_NOT TOKEN_EXISTS TOKEN_ISR TOKEN_ISW TOKEN_ISX TOKEN_ISBLOCK TOKEN_ISCHAR TOKEN_ISDIR TOKEN_ISFILE TOKEN_ISLINK TOKEN_ISPIPE TOKEN_ISSOCK TOKEN_POW TOKEN_LPAREN TOKEN_RPAREN TOKEN_TO TOKEN_SHIFT TOKEN_LEFT_ARROW TOKEN_RIGHT_ARROW TOKEN_DOUBLE_RIGHT_ARROW TOKEN_NUMBERED_SQUIGGLY_RIGHT_ARROW TOKEN_NUMBERED_DOUBLE_SQUIGGLY_RIGHT_ARROW TOKEN_SQUIGGLY_RIGHT_ARROW TOKEN_LONG_LEFT_ARROW TOKEN_LONG_RIGHT_ARROW TOKEN_LONG_DOUBLE_RIGHT_ARROW TOKEN_LONG_SQUIGGLY_RIGHT_ARROW TOKEN_LONG_DOUBLE_SQUIGGLY_RIGHT_ARROW TOKEN_DOUBLE_SQUIGGLY_RIGHT_ARROW

%nonassoc TOKEN_TO
%nonassoc TOKEN_STEP
%left     TOKEN_OR
%left     TOKEN_AND
%nonassoc TOKEN_EQ TOKEN_NE TOKEN_EQL TOKEN_NEQL TOKEN_LT TOKEN_LE TOKEN_GT TOKEN_GE
%left     TOKEN_ADD TOKEN_SUB
%left     TOKEN_MUL TOKEN_DIV TOKEN_MOD
%right    TOKEN_NOT TOKEN_EXISTS TOKEN_ISR TOKEN_ISW TOKEN_ISX TOKEN_ISBLOCK TOKEN_ISCHAR TOKEN_ISDIR TOKEN_ISFILE TOKEN_ISLINK TOKEN_ISPIPE TOKEN_ISSOCK
%left     TOKEN_POW
%nonassoc PREC_EXPR
%nonassoc PREC_LITERAL
%nonassoc PREC_FCALL

%{

#include "ast.h"
#include "parser.h"
#include "ftsh_error.h"
#include "xmalloc.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define YYDEBUG 1

extern char * yytext;
extern FILE * yyin;

extern int yyerror( char *str );
extern int yylex();

static struct ast_group * parser_result=0;

%}

%%

program
	: group
		{ parser_result = $1; }
	;

group
	: command group
		{ $$ = ast_group_create( $1, $2 ); }
	| command
		{ $$ = ast_group_create( $1, 0 ); }
	;

command
	: function
		{ $$ = ast_command_create( AST_COMMAND_FUNCTION, $1 ); }
	| try
		{ $$ = ast_command_create( AST_COMMAND_TRY, $1 ); }
	| forloop
		{ $$ = ast_command_create( AST_COMMAND_FORLOOP, $1 ); }
	| whileloop
		{ $$ = ast_command_create( AST_COMMAND_WHILELOOP, $1 ); }
	| conditional
		{ $$ = ast_command_create( AST_COMMAND_CONDITIONAL, $1 ); }
	| shift_line
		{ $$ = ast_command_create( AST_COMMAND_SHIFT, $1 ); }
	| assign_line
		{ $$ = ast_command_create( AST_COMMAND_ASSIGN, $1 ); }
	| return_line
		{ $$ = ast_command_create( AST_COMMAND_RETURN, $1 ); }
	| simple_line
		{ $$ = ast_command_create( AST_COMMAND_SIMPLE, $1 ); }
	| TOKEN_DELIMITER
		{ $$ = ast_command_create( AST_COMMAND_EMPTY, 0 ); }
	;

function
	: TOKEN_FUNCTION word TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{ $$ = ast_function_create( $1.line, $5.line, $2, $4 ); }
	;

/*
Ok, this is ugly, but the only way to get around the reality of LALR(1).
*/

try
	: TOKEN_TRY try_limit TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{
			$$ = $2;
			$$->try_line=$1.line;
			$$->catch_line=$5.line;
			$$->end_line=$5.line;
			$$->body=$4;
		}
	| TOKEN_TRY try_limit TOKEN_DELIMITER group TOKEN_CATCH TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{
			$$ = $2;
			$$->try_line=$1.line;
			$$->catch_line=$5.line;
			$$->end_line=$8.line;
			$$->body=$4;
			$$->catch_block=$7;
		}
	;

try_limit
	: opt_every_limit
		{ $$ = ast_try_create(0,0,0,0,0,$1,0,0); }
	| opt_conj time_limit opt_every_limit
		{ $$ = ast_try_create(0,0,0,$2,0,$3,0,0); }
	| opt_conj loop_limit opt_every_limit
		{ $$ = ast_try_create(0,0,0,0,$2,$3,0,0); }
	| opt_conj time_limit opt_conj loop_limit opt_every_limit
		{ $$ = ast_try_create(0,0,0,$2,$4,$5,0,0); }
	| opt_conj loop_limit opt_conj time_limit opt_every_limit
		{ $$ = ast_try_create(0,0,0,$4,$2,$5,0,0); }
	;

opt_conj
	: TOKEN_FOR
		{ }
	| TOKEN_OR
		{ }
	|
		{ }
	;

time_limit
	: expr time_units
		{ $$ = ast_try_limit_create( $1, $2 ); }
	;

loop_limit
	: expr TOKEN_TIMES
		{ $$ = ast_try_limit_create( $1, 0 ); }
	;

opt_every_limit
	: /* nothing */
		{ $$ = 0; }
	| TOKEN_EVERY expr time_units
		{ $$ = ast_try_limit_create( $2, $3 ); }
	;
	
time_units
	: TOKEN_SECONDS
		{ $$ = 1; }
	| TOKEN_MINUTES
		{ $$ = 60; }
	| TOKEN_HOURS
		{ $$ = 60*60; }
	| TOKEN_DAYS
		{ $$ = 24*60*60; }
	;

forloop
	: fortype word TOKEN_IN expr_list TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{ $$ = ast_forloop_create( $1, $3.line, $7.line, $2, $4, $6 ); }
	;

fortype
	: TOKEN_FOR
		{ $$ = AST_FOR; }
	| TOKEN_FORANY
		{ $$ = AST_FORANY; }
	| TOKEN_FORALL
		{ $$ = AST_FORALL; }
	;

whileloop
	: TOKEN_WHILE expr TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{ $$ = ast_whileloop_create( $1.line, $3.line, $5.line, $2, $4 ); }
	;

conditional
	: TOKEN_IF expr TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{ $$ = ast_conditional_create( $1.line, $3.line, $5.line, $5.line, $2, $4, 0 ); }
	| TOKEN_IF expr TOKEN_DELIMITER group TOKEN_ELSE TOKEN_DELIMITER group TOKEN_END TOKEN_DELIMITER
		{ $$ = ast_conditional_create( $1.line, $3.line, $8.line, $8.line, $2, $4, $7 ); }
	| TOKEN_IF expr TOKEN_DELIMITER group TOKEN_ELSE conditional
		{
			struct ast_group *g = ast_group_create( ast_command_create( AST_COMMAND_CONDITIONAL, $6 ), 0 );
			$$ = ast_conditional_create( $1.line, $3.line, $5.line, $5.line, $2, $4, g );
		}
	;

shift_line
	: TOKEN_SHIFT expr TOKEN_DELIMITER
		{ $$ = ast_shift_create($1.line,$2); }
	| TOKEN_SHIFT TOKEN_DELIMITER
		{ $$ = ast_shift_create($1.line,0); }
	;

assign_line
	: wordequals expr TOKEN_DELIMITER
		{ $$ = ast_assign_create( $3.line, $1, $2 ); }
	| wordequals TOKEN_DELIMITER
		{ $$ = ast_assign_create( $2.line, $1, 0 ); }
	;

return_line
	: TOKEN_RETURN expr TOKEN_DELIMITER
		{ $$ = ast_return_create( $3.line, $2 ); }
	;

simple_line
	: simple TOKEN_DELIMITER
		{ $$ = $1; $1->line = $2.line; }
	;

simple
	: simple word
		{
			struct ast_word *w;
			for( w=$1->words; w->next; w=w->next ) {}
			w->next = $2;
			$$ = $1;
		}
	| simple redirect
		{
			struct ast_redirect *r;
			if( $1->redirects ) {
				for( r=$1->redirects; r->next; r=r->next ) {}
				r->next = $2;
			} else {
				$1->redirects = $2;
			}
			$$ = $1;
		}
	| word
		{ $$ = ast_simple_create( 0, $1, 0 ); }
	;

redirect
	: TOKEN_LEFT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_FILE, $1.firstint, $2, AST_REDIRECT_INPUT ); }
	| TOKEN_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_FILE, $1.firstint, $2, AST_REDIRECT_OUTPUT ); }
	| TOKEN_DOUBLE_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_FILE, $1.firstint, $2, AST_REDIRECT_APPEND ); }
	| TOKEN_NUMBERED_SQUIGGLY_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_FD, $1.firstint, $2, AST_REDIRECT_OUTPUT ); }
	| TOKEN_NUMBERED_DOUBLE_SQUIGGLY_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_FD, $1.firstint, $2, AST_REDIRECT_APPEND ); }
	| TOKEN_SQUIGGLY_RIGHT_ARROW word
		{
		$$ = ast_redirect_create( AST_REDIRECT_FILE, 1, $2, AST_REDIRECT_OUTPUT );
		$$->next = ast_redirect_create( AST_REDIRECT_FD, 2, ast_word_create($1.line,"1"), AST_REDIRECT_APPEND );
		}
	| TOKEN_DOUBLE_SQUIGGLY_RIGHT_ARROW word
		{
		$$ = ast_redirect_create( AST_REDIRECT_FILE, 1, $2, AST_REDIRECT_APPEND );
		$$->next = ast_redirect_create( AST_REDIRECT_FD, 2, ast_word_create($1.line,"1"), AST_REDIRECT_APPEND );
		}
	| TOKEN_LONG_LEFT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_BUFFER, $1.firstint, $2, AST_REDIRECT_INPUT ); }
	| TOKEN_LONG_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_BUFFER, $1.firstint, $2, AST_REDIRECT_OUTPUT ); }
	| TOKEN_LONG_DOUBLE_RIGHT_ARROW word
		{ $$ = ast_redirect_create( AST_REDIRECT_BUFFER, $1.firstint, $2, AST_REDIRECT_APPEND ); }
	| TOKEN_LONG_SQUIGGLY_RIGHT_ARROW word
		{
		$$ = ast_redirect_create( AST_REDIRECT_BUFFER, 1, $2, AST_REDIRECT_OUTPUT );
		$$->next = ast_redirect_create( AST_REDIRECT_FD, 2, ast_word_create($1.line,"1"), AST_REDIRECT_APPEND );
		}
	| TOKEN_LONG_DOUBLE_SQUIGGLY_RIGHT_ARROW word
		{
		$$ = ast_redirect_create( AST_REDIRECT_BUFFER, 1, $2, AST_REDIRECT_APPEND );
		$$->next = ast_redirect_create( AST_REDIRECT_FD, 2, ast_word_create($1.line,"1"), AST_REDIRECT_APPEND );
		}
	;

opt_expr_comma_list
	: /* nothing */
		{ $$ = 0; }
	| expr_comma_list
		{ $$ = $1; }
	;

expr_comma_list
	: expr
		{ $$ = $1; }
	| expr TOKEN_COMMA expr_comma_list
		{ $$ = $1; $1->next=$3; }
	;

expr_list
	: expr
		{ $$ = $1; }
	| expr expr_list
		{ $$ = $1; $1->next=$2; }
	;

expr
	: expr TOKEN_TO expr %prec TOKEN_TO
		{ $$ = expr_create($2.line,EXPR_TO,0,$1,$3,0); }
	| expr TOKEN_TO expr TOKEN_STEP expr %prec TOKEN_STEP
		{ $$ = expr_create($2.line,EXPR_TO,0,$1,$3,$5); }
	| expr TOKEN_OR expr
		{ $$ = expr_create($2.line,EXPR_OR,0,$1,$3,0); }
	| expr TOKEN_AND expr
		{ $$ = expr_create($2.line,EXPR_AND,0,$1,$3,0); }
	| expr TOKEN_EQ expr
		{ $$ = expr_create($2.line,EXPR_EQ,0,$1,$3,0); }
	| expr TOKEN_NE expr
		{ $$ = expr_create($2.line,EXPR_NE,0,$1,$3,0); }
	| expr TOKEN_EQL expr
		{ $$ = expr_create($2.line,EXPR_EQL,0,$1,$3,0); }
	| expr TOKEN_NEQL expr
		{ $$ = expr_create($2.line,EXPR_NEQL,0,$1,$3,0); }
	| expr TOKEN_LT expr
		{ $$ = expr_create($2.line,EXPR_LT,0,$1,$3,0); }
	| expr TOKEN_LE expr
		{ $$ = expr_create($2.line,EXPR_LE,0,$1,$3,0); }
	| expr TOKEN_GT expr
		{ $$ = expr_create($2.line,EXPR_GT,0,$1,$3,0); }
	| expr TOKEN_GE expr
		{ $$ = expr_create($2.line,EXPR_GE,0,$1,$3,0); }
	| expr TOKEN_ADD expr
		{ $$ = expr_create($2.line,EXPR_ADD,0,$1,$3,0); }
	| expr TOKEN_SUB expr
		{ $$ = expr_create($2.line,EXPR_SUB,0,$1,$3,0); }
	| expr TOKEN_MUL expr
		{ $$ = expr_create($2.line,EXPR_MUL,0,$1,$3,0); }
	| expr TOKEN_DIV expr
		{ $$ = expr_create($2.line,EXPR_DIV,0,$1,$3,0); }
	| expr TOKEN_MOD expr
		{ $$ = expr_create($2.line,EXPR_MOD,0,$1,$3,0); }
	| TOKEN_NOT expr
		{ $$ = expr_create( $1.line, EXPR_NOT, 0, $2, 0, 0); }
	| TOKEN_EXISTS expr
		{ $$ = expr_create( $1.line, EXPR_EXISTS, 0, $2, 0, 0); }
	| TOKEN_ISR expr
		{ $$ = expr_create( $1.line, EXPR_ISR, 0, $2, 0, 0); }
	| TOKEN_ISW expr
		{ $$ = expr_create( $1.line, EXPR_ISW, 0, $2, 0, 0); }
	| TOKEN_ISX expr
		{ $$ = expr_create( $1.line, EXPR_ISX, 0, $2, 0, 0); }
	| TOKEN_ISBLOCK expr
		{ $$ = expr_create( $1.line, EXPR_ISBLOCK, 0, $2, 0, 0); }
	| TOKEN_ISCHAR expr
		{ $$ = expr_create( $1.line, EXPR_ISCHAR, 0, $2, 0, 0); }
	| TOKEN_ISDIR expr
		{ $$ = expr_create( $1.line, EXPR_ISDIR, 0, $2, 0, 0); }
	| TOKEN_ISFILE expr
		{ $$ = expr_create( $1.line, EXPR_ISFILE, 0, $2, 0, 0); }
	| TOKEN_ISLINK expr
		{ $$ = expr_create( $1.line, EXPR_ISLINK, 0, $2, 0, 0); }
	| TOKEN_ISPIPE expr
		{ $$ = expr_create( $1.line, EXPR_ISPIPE, 0, $2, 0, 0); }
	| TOKEN_ISSOCK expr
		{ $$ = expr_create( $1.line, EXPR_ISSOCK, 0, $2, 0, 0); }
	| expr TOKEN_POW expr
		{ $$ = expr_create( $2.line, EXPR_POW, 0, $1, $3, 0 ); }
	| TOKEN_LPAREN expr TOKEN_RPAREN %prec PREC_EXPR
		{ $$ = expr_create($1.line,EXPR_EXPR,0,$2,0,0); }
	| word %prec PREC_LITERAL
		{ $$ = expr_create($1->line,EXPR_LITERAL,$1,0,0,0); }
	| word TOKEN_LPAREN opt_expr_comma_list TOKEN_RPAREN %prec PREC_FCALL
		{ $$ = expr_create($2.line,EXPR_FCALL,$1,$3,0,0); }
	;

wordequals
	: TOKEN_ASSIGN
		{ $$ = ast_word_create( $1.line, yytext ); }
	;

word
	: TOKEN_ATOM
		{ $$ = ast_word_create( $1.line, yytext ); }
	;

%%


struct ast_group * parse_file( FILE *f, int do_debug )
{
	yydebug = do_debug;
	yyin = f;
	parser_result = 0;
	yyparse();
	return parser_result;
}

extern int current_line;

int yyerror( char *string )
{
	ftsh_fatal(current_line,"parse error near here",current_line);
	return 0;
}
