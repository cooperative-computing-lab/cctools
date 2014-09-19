/*
  Copyright (C) 2013- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

/* Makeflow lexer. Converts a makeflow file into a series of tokens so
   that the parser can easily reconstruct the DAG.

   The lexer is implemented with as a hierarchy of functions. The entry
   points are:

   bk = lexer_init_book(...);
   t  = lexer_next_token(bk);     // The next token in the series.

   When the end-of-file is reached, t == NULL. Each token has a type,
   t->type, which is an element of enum token_t, and a value,
   t->lexeme, which is a pointer to char. A token is defined with
   struct token. The token type use and meanings are:

   SYNTAX:  A keyword particular to makeflow, for example the keyword 'export'.
   NEWLINE: A newline, either explicitely terminating a line, or after
   discarding a comment. NEWLINE tokens signal the end of
   lists of other tokens, such as commands, file lists, or
  variable substituions. New-line characters preceeded by \
   loose their special meaning.
   VARIABLE: A variable assignment of the form NAME=VALUE, NAME+=VALUE,
   NAME-=VALUE, NAME!=VALUE. NAME is any string consisting of
   alphanumeric characters and underscores. VALUE is an
   expandable string (see below). The type of assignment is
   recorded in t->lexeme. + appends, - sets if NAME is not
   already set, and !  executes in the shell and assigns the
   value printed to stdout.
   SUBSTITUTION: A variable substitution signaled by $. t->lexeme
   records the variable name, which can be specified as $name or
   $(name). All variable substitutions are done in place, that is, the
   parser never sees them.
   LITERAL: A literal string value, used as a variable name, a
   filename, or a command argument.
   COMMAND: Signals the command line of a rule, described as a list of
   tokens. A command line always starts with a tab character
   in the makeflow file. The end of the list is signaled with
   a NEWLINE token. The parser is resposible for assembling
   the command line for execution.
   COMMAND_MOD_END: LITERALs between COMMAND and COMMAND_MOD_END are
   interpreted as a command modifiers, such as LOCAL or MAKEFLOW.
   SPACE: White space that separates arguments in a command line.
   IO_REDIRECT: Indicates input or output redirection in a command
   line. One of "<" or ">".
   FILES: A list of literals, describing input or output files. Ends with a NEWLINE token.
   COLON: Signals the separation between input and output files.
   REMOTE_NAME: Signals the characters "->" to indicate remote renaming. 

  
   The function lexer_next_token(bk) calls lexer_next_line, which
   depending on some lookahead, calls lexer_read_command,
   lexer_read_file_list, or lexer_read_syntax. In turn, each of these
   functions call, respectively, lexer_read_command_argument,
   lexer_read_file, and lexer_read_export and lexer_read_variable,
   until a NEWLINE token is found.

   The lowest level function is lexer_next_char, which reads the stream
   char by char. For efficiency, the file chunks are read
   alternativetely into two buffers as needed. The current buffer
   position is kept at bk->lexeme_end;

   As tokens are recognized, the function lexer_add_to_lexeme
   accumulates the current value of the token. The function
   lexer_pack_token creates a new token, resetting the values of
   bk->lexeme, and bk->lexeme_end. A token is inserted
   in the token queue with lexer_push_token.
*/

#include "dag.h"

enum token_t
{
	SYNTAX,
	NEWLINE,
	VARIABLE,
	SUBSTITUTION, 
	LITERAL,
	SPACE,

	COMMAND,
	COMMAND_MOD_END,
	IO_REDIRECT,

	FILES,
	COLON,
	REMOTE_RENAME,

	ROOT,
};

enum
{
	STRING,
	STREAM
};

struct token 
{
	enum token_t type;
	char        *lexeme;
	int          option;

	long int     line_number;
	long int     column_number;
};

/* type: is either STREAM or CHAR */
struct lexer_book *lexer_init_book(int type, void *data, int line_number, int column_number);
struct lexer_book *lexer_init_substitution_book(struct lexer_book *bk, struct token *subs_name); 

struct token *lexer_next_token(struct lexer_book *bk);
struct token *lexer_peek_next_token(struct lexer_book *bk);

void lexer_report_error(struct lexer_book *bk, char *message, ...);
char *lexer_print_token(struct token *t);
void lexer_print_queue(struct lexer_book *bk);

int lexer_push_token(struct lexer_book *bk, struct token *t);
int lexer_preppend_token(struct lexer_book *bk, struct token *t);


void lexer_free_book(struct lexer_book *bk);
void lexer_free_token(struct token *t);

