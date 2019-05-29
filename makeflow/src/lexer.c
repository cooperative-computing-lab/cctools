/*
  Copyright (C) 2013- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>

#include "int_sizes.h"
#include "stringtools.h"
#include "get_line.h"
#include "list.h"
#include "hash_table.h"
#include "debug.h"
#include "xxmalloc.h"
#include "buffer.h"

#include "dag.h"
#include "lexer.h"

#define CHAR_EOF 26		// ASCII for EOF

#define LITERAL_LIMITS  "\\\"'$#\n\t \032"
#define SYNTAX_LIMITS  LITERAL_LIMITS  ",.-(){},[]<>=+!?/"
#define FILENAME_LIMITS LITERAL_LIMITS  ":-"

#define WHITE_SPACE          " \t"
#define BUFFER_CHUNK_SIZE 1048576	// One megabyte

#define MAX_SUBSTITUTION_DEPTH 32

#ifdef LEXER_TEST
extern int verbose_parsing;
#endif

struct token *lexer_pack_token(struct lexer *lx, enum token_t type)
{
	struct token *t = malloc(sizeof(struct token));

	t->type = type;
	t->line_number   = lx->line_number;
	t->column_number = lx->column_number;

	t->lexeme = calloc(lx->lexeme_size + 1, sizeof(char));
	memcpy(t->lexeme, lx->lexeme, lx->lexeme_size);
	*(t->lexeme + lx->lexeme_size) = '\0';

	lx->lexeme_size = 0;

	return t;
}

char *lexer_print_token(struct token *t)
{
	char str[1024];

	int n = sizeof(str);

	switch (t->type) {
	case TOKEN_SYNTAX:
		string_nformat(str, n, "SYNTAX:  %s\n", t->lexeme);
		break;
	case TOKEN_NEWLINE:
		string_nformat(str, n, "NEWLINE\n");
		break;
	case TOKEN_SPACE:
		string_nformat(str, n, "SPACE\n");
		break;
	case TOKEN_FILES:
		string_nformat(str, n, "FILES:  %s\n", t->lexeme);
		break;
	case TOKEN_VARIABLE:
		string_nformat(str, n, "VARIABLE: %s\n", t->lexeme);
		break;
	case TOKEN_COLON:
		string_nformat(str, n, "COLON\n");
		break;
	case TOKEN_REMOTE_RENAME:
		string_nformat(str, n, "REMOTE_RENAME: %s\n", t->lexeme);
		break;
	case TOKEN_LITERAL:
		string_nformat(str, n, "LITERAL: %s\n", t->lexeme);
		break;
	case TOKEN_SUBSTITUTION:
		string_nformat(str, n, "SUBSTITUTION: %s\n", t->lexeme);
		break;
	case TOKEN_COMMAND:
		string_nformat(str, n, "COMMAND: %s\n", t->lexeme);
		break;
	case TOKEN_COMMAND_MOD_END:
		string_nformat(str, n, "COMMAND_MOD_END: %s\n", t->lexeme);
		break;
	case TOKEN_IO_REDIRECT:
		string_nformat(str, n, "IO_REDIRECT: %s\n", t->lexeme);
		break;
	case TOKEN_DIRECTIVE:
		string_nformat(str, n, "DIRECTIVE\n");
		break;
	default:
		string_nformat(str, n, "unknown: %s\n", t->lexeme);
		break;
	}

	return xxstrdup(str);
}

int lexer_push_token(struct lexer *lx, struct token *t)
{
	list_push_tail(lx->token_queue, t);
	return list_size(lx->token_queue);
}

int lexer_preppend_token(struct lexer *lx, struct token *t)
{
	list_push_head(lx->token_queue, t);
	return list_size(lx->token_queue);
}

void lexer_roll_back_one(struct lexer *lx)
{
	int c = *lx->lexeme_end;

	if(c == '\n') {

		lx->line_number--;
		lx->column_number = (uintptr_t) list_pop_head(lx->column_numbers);
	} else if(c == CHAR_EOF) {
		lx->eof = 0;
		lx->column_number--;
	} else {
		lx->column_number--;
	}

	if(lx->lexeme_end == lx->buffer)
		lx->lexeme_end = (lx->buffer + 2 * BUFFER_CHUNK_SIZE);

	lx->lexeme_end--;

	if(*lx->lexeme_end == '\0')
		lx->lexeme_end--;
}

void lexer_roll_back(struct lexer *lx, int offset)
{
	while(offset > 0) {
		offset--;
		lexer_roll_back_one(lx);
	}
}

void lexer_add_to_lexeme(struct lexer *lx, char c)
{
	if(lx->lexeme_size == lx->lexeme_max) {
		char *tmp = realloc(lx->lexeme, lx->lexeme_max + BUFFER_CHUNK_SIZE);
		if(!tmp) {
			fatal("Could not allocate memory for next token.\n");
		}
		lx->lexeme = tmp;
		lx->lexeme_max += BUFFER_CHUNK_SIZE;
	}

	*(lx->lexeme + lx->lexeme_size) = c;
	lx->lexeme_size++;
}

int lexer_special_to_code(char c)
{
	switch (c) {
	case 'a':		/* Bell */
		return 7;
		break;
	case 'b':		/* Backspace */
		return 8;
		break;
	case 'f':		/* Form feed */
		return 12;
		break;
	case 'n':		/* New line */
		return 10;
		break;
	case 'r':		/* Carriage return */
		return 13;
		break;
	case 't':		/* Horizontal tab */
		return 9;
		break;
	case 'v':		/* Vertical tab */
		return 11;
		break;
	default:
		return 0;
		break;
	}
}

int lexer_special_escape(char c)
{
	switch (c) {
	case 'a':		/* Bell */
	case 'b':		/* Backspace */
	case 'f':		/* Form feed */
	case 'n':		/* New line */
	case 'r':		/* Carriage return */
	case 't':		/* Horizontal tab */
	case 'v':		/* Vertical tab */
		return 1;
		break;
	default:
		return 0;
		break;
	}
}

void lexer_report_error(struct lexer *lx, char *message, ...)
{
	va_list ap;
	va_start(ap, message);

	char message_filled[BUFFER_CHUNK_SIZE];

	vsprintf(message_filled, message, ap);
	//fprintf(stderr, "%s line: %ld column: %ld\n", message_filled, lx->line_number, lx->column_number);
	//abort();
	fatal("%s line: %d column: %d\n", message_filled, lx->line_number, lx->column_number);

	va_end(ap);

}

//Useful for debugging:
void lexer_print_queue(struct lexer *lx)
{
	struct token *t;

	debug(D_MAKEFLOW_LEXER, "Queue: ");

	list_first_item(lx->token_queue);
	while((t = list_next_item(lx->token_queue)))
		debug(D_MAKEFLOW_LEXER, "%s", lexer_print_token(t));
	list_first_item(lx->token_queue);

	debug(D_MAKEFLOW_LEXER, "End queue.");
}

void lexer_load_chunk(struct lexer *lx)
{

	if(lx->chunk_last_loaded == 2 && lx->lexeme_end == lx->buffer)
		lx->chunk_last_loaded = 1;
	else if(lx->chunk_last_loaded == 1 && lx->lexeme_end != lx->buffer)
		lx->chunk_last_loaded = 2;
	else
		return;

	int bread = fread(lx->lexeme_end, sizeof(char), BUFFER_CHUNK_SIZE - 1, lx->stream);

	*(lx->buffer + BUFFER_CHUNK_SIZE - 1) = '\0';
	*(lx->buffer + 2 * BUFFER_CHUNK_SIZE - 1) = '\0';

	if(lx->lexeme_end >= lx->buffer + 2 * BUFFER_CHUNK_SIZE)
		fatal("End of token is out of bounds.\n");

	if(bread < BUFFER_CHUNK_SIZE - 1)
		*(lx->lexeme_end + bread) = CHAR_EOF;

}

void lexer_load_string(struct lexer *lx, char *s)
{
	int len = strlen(s);

	lx->chunk_last_loaded = 1;

	strcpy(lx->buffer, s);
	*(lx->buffer + len) = CHAR_EOF;

	*(lx->buffer + 2 * BUFFER_CHUNK_SIZE - 1) = '\0';

	if(lx->lexeme_end >= lx->buffer + 2 * BUFFER_CHUNK_SIZE)
		fatal("End of token is out of bounds.\n");

}


char lexer_next_char(struct lexer *lx)
{
	if(*lx->lexeme_end == CHAR_EOF) {
		return CHAR_EOF;
	}

	/* If at the end of chunk, load the next chunk. */
	if(((lx->lexeme_end + 1) == (lx->buffer + BUFFER_CHUNK_SIZE - 1)) || ((lx->lexeme_end + 1) == (lx->buffer + 2 * BUFFER_CHUNK_SIZE - 1))) {
		if(lx->lexeme_max == BUFFER_CHUNK_SIZE - 1)
			lexer_report_error(lx, "Input buffer is full. Runaway token?");	//BUG: This is really a recoverable error, increase the buffer size.
		/* Wrap around the file chunks */
		else if(lx->lexeme_end == lx->buffer + 2 * BUFFER_CHUNK_SIZE - 2)
			lx->lexeme_end = lx->buffer;
		/* Position at the beginning of next chunk */
		else
			lx->lexeme_end += 2;

		lexer_load_chunk(lx);
	} else
		lx->lexeme_end++;

	char c = *lx->lexeme_end;

	if(c == '\n') {
		lx->line_number++;
		list_push_head(lx->column_numbers, (uint64_t *) lx->column_number);
		lx->column_number = 1;
	} else {
		lx->column_number++;
	}

	if(c == CHAR_EOF) {
		lx->eof = 1;
	}

	return c;
}

int lexer_next_peek(struct lexer *lx)
{
	/* Read next chunk if necessary */
	int c = lexer_next_char(lx);
	lexer_roll_back(lx, 1);

	return c;
}

int lexer_peek_remote_rename_syntax(struct lexer *lx)
{
	if(lexer_next_peek(lx) != '-')
		return 0;

	lexer_next_char(lx);

	int is_gt = (lexer_next_peek(lx) == '>');
	lexer_roll_back(lx, 1);

	return is_gt;
}


/* Read characters until a character in char_set is found. (exclusive) */
/* Returns the count of characters that we would have to roll-back to
   undo the read. */
int lexer_read_until(struct lexer *lx, const char *char_set )
{
	int count = 0;
	char c;

	do {
		c = lexer_next_peek(lx);
		if(strchr(char_set, c)) {
			return count;
		}

		if(c != CHAR_EOF)
			lexer_add_to_lexeme(lx, c);

		lexer_next_char(lx);
		count++;

	} while(c != CHAR_EOF);

	lx->eof = 1;

	return count;
}

/* A comment starts with # and ends with a newline, or end-of-file */
void lexer_discard_comments(struct lexer *lx)
{
	if(lexer_next_peek(lx) != '#')
		lexer_report_error(lx, "Expecting a comment.");

	char c;
	do {
		c = lexer_next_char(lx);
	} while(c != '\n' && c != CHAR_EOF);
}

/* As lexer_read_until, but elements of char_set preceded by \ are
   ignored as stops, with \n replaced with spaces. */
int lexer_read_escaped_until(struct lexer *lx, char *char_set)
{
	char *char_set_slash = string_format("\\%s", char_set);

	int count = 0;

	do {
		count += lexer_read_until(lx, char_set_slash);

		if(!lx->eof && lexer_next_peek(lx) == '\\') {
			lexer_next_char(lx);	/* Jump the slash */
			char c = lexer_next_char(lx);
			count += 2;

			if(lexer_next_peek(lx) != CHAR_EOF) {
				if(c == '\n') {
					lexer_add_to_lexeme(lx, ' ');
				} else {
					lexer_add_to_lexeme(lx, c);
				}
			}
		} else
			break;

	} while(!lx->eof);

	free(char_set_slash);

	if(lx->eof && !strchr(char_set, CHAR_EOF))
		lexer_report_error(lx, "Missing %s\n", char_set);

	return count;
}

int lexer_read_literal_unquoted(struct lexer * lx)
{
	return lexer_read_escaped_until(lx, LITERAL_LIMITS);
}

/* Read everything between single quotes */
int lexer_read_literal_quoted(struct lexer * lx)
{
	int c = lexer_next_peek(lx);

	if(c != '\'')
		lexer_report_error(lx, "Missing opening quote.\n");

	lexer_add_to_lexeme(lx, lexer_next_char(lx));	/* Add first ' */

	int count = lexer_read_escaped_until(lx, "'");

	lexer_add_to_lexeme(lx, lexer_next_char(lx));	/* Add second ' */

	return count;
}

int lexer_read_literal(struct lexer * lx)
{
	int c = lexer_next_peek(lx);

	if(c == '\'')
		return lexer_read_literal_quoted(lx);
	else
		return lexer_read_literal_unquoted(lx);
}

/* We read a string that can have $ substitutions. We read literals
   between special symbols, such as $ " ' etc., interpreting them as
   we go. end_marker indicates if the expandable should end with a
   newline (as for variable assignment), or with double quote ".*/

struct token *lexer_read_literal_in_expandable_until(struct lexer *lx, char end_marker)
{
	const char end_markers[8] = { end_marker, '$', '\\', '"', '\'', '#', CHAR_EOF ,0};

	int count = 0;
	do {
		count += lexer_read_until(lx, end_markers);

		if(lx->eof)
			break;

		char c = lexer_next_peek(lx);
		if(c == '\\') {
			lexer_next_char(lx);	/* Jump the slash */
			char n = lexer_next_char(lx);
			count += 2;

			if(lexer_special_escape(n)) {
					lexer_add_to_lexeme(lx, lexer_special_to_code(n));
			} else if(n == '\n') {
				lexer_add_to_lexeme(lx, ' ');
			} else {
				lexer_add_to_lexeme(lx, n);
			}
		} else if(c == '#') {
			if(end_marker == '\n') {
				lexer_discard_comments(lx);
				break;
			} else {
				// # comment is quoted, so we simply insert it.
				count++;
				lexer_add_to_lexeme(lx, lexer_next_char(lx));
			}
		} else
			break;
	} while(!lx->eof);

	if(lx->eof && strchr(")\"'", end_marker))
		lexer_report_error(lx, "Missing closing %c.\n", end_marker);

	return lexer_pack_token(lx, TOKEN_LITERAL);
}

/* Read a filename, adding '-' to names when - is not followed by
   >. The 'recursive' comes because the function calls itself when
   completing a name when it added a -. */
int lexer_read_filename_recursive(struct lexer *lx)
{
	int count = lexer_read_escaped_until(lx, FILENAME_LIMITS);

	if(count < 1)
		return count;

	if(lexer_next_peek(lx) == '-' && !lexer_peek_remote_rename_syntax(lx)) {
		lexer_add_to_lexeme(lx, '-');
		lexer_next_char(lx);
		count++;
		count += lexer_read_filename_recursive(lx);
	}

	return count;
}

struct token *lexer_read_filename(struct lexer *lx)
{
	int count = lexer_read_filename_recursive(lx);

	if(count < 1)
		lexer_report_error(lx, "Expecting a filename.");

	return lexer_pack_token(lx, TOKEN_LITERAL);
}


struct token *lexer_read_syntax_name(struct lexer *lx)
{
	int count;

	char c = lexer_next_peek(lx);

	count = lexer_read_until(lx, SYNTAX_LIMITS);

	if(count < 1 && c != '.')
		lexer_report_error(lx, "Expecting a keyword or a variable name.");

	if(c == '.')
		lexer_read_until(lx, LITERAL_LIMITS);

	return lexer_pack_token(lx, TOKEN_LITERAL);
}

struct token *lexer_read_substitution(struct lexer *lx)
{
	char closer = 0;                  //closer is either 0 (no closer), ) or }.
	char c = lexer_next_peek(lx);

	if(c != '$')
		lexer_report_error(lx, "Expecting $ for variable substitution.");

	lexer_next_char(lx);	/* Jump $ */

	if(lexer_next_peek(lx) == '(') {
		lexer_next_char(lx);	/* Jump ( */
		closer = ')';
	} else if(lexer_next_peek(lx) == '{') {
		lexer_next_char(lx);	/* Jump { */
		closer = '}';
	}

	struct token *name = lexer_read_syntax_name(lx);
	name->type = TOKEN_SUBSTITUTION;

	if(closer) {
		if(lexer_next_peek(lx) == closer)
			lexer_next_char(lx);	/* Jump ) */
		else
			lexer_report_error(lx, "Expecting %c for closing variable substitution.", closer);
	}

	return name;
}

int lexer_discard_white_space(struct lexer *lx)
{
	int count = 0;
	while(strchr(WHITE_SPACE, lexer_next_peek(lx))) {
		lexer_next_char(lx);
		count++;
	}

	return count;
}

/* Consolidates a sequence of white space into a single SPACE token */
struct token *lexer_read_white_space(struct lexer *lx)
{
	int count = lexer_discard_white_space(lx);

	while(strchr(WHITE_SPACE, lexer_next_peek(lx))) {
		count++;
		lexer_next_char(lx);
	}

	if(count > 0) {
		lexer_add_to_lexeme(lx, ' ');
		return lexer_pack_token(lx, TOKEN_SPACE);
	} else
		lexer_report_error(lx, "Expecting white space.");

	return NULL;
}

//opened tracks whether it is the opening (opened = 0) or closing (opened = 1) double quote we encounter.
struct list *lexer_read_expandable_recursive(struct lexer *lx, char end_marker, int opened)
{
	lexer_discard_white_space(lx);

	struct list *tokens = list_create();

	while(!lx->eof) {
		int c = lexer_next_peek(lx);

		if(c == '$') {
			list_push_tail(tokens, lexer_read_substitution(lx));
		}

		if(c == '\'') {
			lexer_read_literal(lx);
			list_push_tail(tokens, lexer_pack_token(lx, TOKEN_LITERAL));
		} else if(c == '"' && opened == 0) {
				lexer_add_to_lexeme(lx, lexer_next_char(lx));
				list_push_tail(tokens, lexer_pack_token(lx, TOKEN_LITERAL));     // Add first "
				tokens = list_splice(tokens, lexer_read_expandable_recursive(lx, '"', 1));
				lexer_add_to_lexeme(lx, '"');
				list_push_tail(tokens, lexer_pack_token(lx, TOKEN_LITERAL));     // Add closing "
				if(end_marker == '"')
					return tokens;
		} else if(c == '#' && end_marker != '"') {
			lexer_discard_comments(lx);
		} else if(c == end_marker) {
			lexer_next_char(lx);	/* Jump end_marker */
			return tokens;
		} else {
			list_push_tail(tokens, lexer_read_literal_in_expandable_until(lx, end_marker));
		}
	}

	lexer_report_error(lx, "Found EOF before end marker: %c.\n", end_marker);

	return NULL;
}

struct token *lexer_concat_expandable(struct lexer *lx, struct list *tokens)
{
	struct token *t;

	struct buffer b;
	buffer_init(&b);

	char *substitution;

	list_first_item(tokens);

	while((t = list_pop_head(tokens))) {
		switch(t->type) {
		case TOKEN_SUBSTITUTION:
			substitution = dag_variable_lookup_string(t->lexeme, lx->environment);
			if(!substitution)
				fatal("Variable %s has not yet been defined at line % " PRId64 ".\n", t->lexeme, lx->line_number);
			buffer_printf(&b, "%s", substitution);
			free(substitution);
			break;
		case TOKEN_LITERAL:
			if(strcmp(t->lexeme, "") != 0)           // Skip empty strings.
				buffer_printf(&b, "%s", t->lexeme);
			break;
		default:
			lexer_report_error(lx, "Error in expansion, got: %s.\n", lexer_print_token(t));
			break;
		}

		lexer_free_token(t);
	}

	t = lexer_pack_token(lx, TOKEN_LITERAL);

	/* free lexeme allocated, as the buffer did the accumulation */
	free(t->lexeme);

	t->lexeme = xxstrdup(buffer_tostring(&b));
	buffer_free(&b);

	return t;
}

struct token *lexer_read_expandable(struct lexer *lx, char end_marker)
{
	struct list *tokens = lexer_read_expandable_recursive(lx, end_marker, 0);

	struct token *t = lexer_concat_expandable(lx, tokens);

	list_delete(tokens);

	return t;
}


int lexer_append_tokens(struct lexer *lx, struct list *tokens)
{
	struct token *t;
	int count = 0;

	list_first_item(tokens);
	while((t = list_pop_head(tokens))) {
		lexer_push_token(lx, t);
		count++;
	}

	return count;
}

struct list *lexer_expand_substitution(struct lexer *lx, struct token *t,
							  struct list * (*list_reader)(struct lexer *))
{
	struct lexer *lx_s  = lexer_create_substitution(lx, t);
	struct list *tokens = list_reader(lx_s);

	lexer_delete(lx_s);

	return tokens;
}

struct token *lexer_read_file(struct lexer *lx)
{
	int c = lexer_next_peek(lx);

	switch (c) {
	case CHAR_EOF:
		lx->lexeme_end++;
		lx->eof = 1;
		if(lx->depth == 0)
			lexer_report_error(lx, "Found end of file while completing file list.\n");
		return NULL;
		break;
	case '\n':
		lexer_next_char(lx);	/* Jump \n */
		lexer_add_to_lexeme(lx, c);
		return lexer_pack_token(lx, TOKEN_NEWLINE);
		break;
	case '#':
		lexer_discard_comments(lx);
		lexer_add_to_lexeme(lx, '\n');
		return lexer_pack_token(lx, TOKEN_NEWLINE);
	case ':':
		lexer_next_char(lx);	/* Jump : */
		return lexer_pack_token(lx, TOKEN_COLON);
		break;
	case ' ':
	case '\t':
		/* Discard white-space and add space token. */
		lexer_discard_white_space(lx);
		return lexer_pack_token(lx, TOKEN_SPACE);
		break;
	case '$':
		return lexer_read_substitution(lx);
		break;
	case '\'':
		lexer_add_to_lexeme(lx, '\'');
		lexer_read_literal_quoted(lx);
		lexer_add_to_lexeme(lx, '\'');
		return lexer_pack_token(lx, TOKEN_LITERAL);
		break;
	case '-':
		if(lexer_peek_remote_rename_syntax(lx)) {
			lexer_next_char(lx);	/* Jump -> */
			lexer_next_char(lx);
			return lexer_pack_token(lx, TOKEN_REMOTE_RENAME);
		}
		/* Else fall through */
	default:
		return lexer_read_filename(lx);
		break;
	}
}

struct list *lexer_read_file_list_aux(struct lexer *lx)
{
	struct list *tokens = list_create();

	lexer_discard_white_space(lx);

	while(1) {
		struct token *t = lexer_read_file(lx);
		if(!t) break;

		//Do substitution recursively
		if(t->type == TOKEN_SUBSTITUTION) {
			tokens = list_splice(tokens, lexer_expand_substitution(lx, t, lexer_read_file_list_aux));
			lexer_free_token(t);
			continue;
		} else {
			list_push_tail(tokens, t);
			if(t->type==TOKEN_NEWLINE) break;
		}
	}

	return tokens;
}

void lexer_concatenate_consecutive_literals(struct list *tokens)
{
	struct list *tmp = list_create();
	struct token *t, *prev  = NULL;

	list_first_item(tokens);
	while((t = list_pop_head(tokens))) {
		if(t->type != TOKEN_LITERAL) {
			list_push_tail(tmp, t);
			continue;
		}

		prev = list_pop_tail(tmp);

		if(!prev) {
			list_push_tail(tmp, t);
			continue;
		}

		if(prev->type != TOKEN_LITERAL) {
			list_push_tail(tmp, prev);
			list_push_tail(tmp, t);
			continue;
		}

		char *merge = string_format("%s%s", prev->lexeme, t->lexeme);
		lexer_free_token(t);
		free(prev->lexeme);
		prev->lexeme = merge;

		list_push_tail(tmp, prev);
	}

	/* Copy to tokens, drop spaces. */
	list_first_item(tmp);
	while((t = list_pop_head(tmp)))
		if(t->type != TOKEN_SPACE) {
			list_push_tail(tokens, t);
		} else {
			lexer_free_token(t);
		}

	list_delete(tmp);
}

int lexer_read_file_list(struct lexer *lx)
{
	/* Add file list start marker */
	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_FILES));

	struct list *tokens = lexer_read_file_list_aux(lx);

	lexer_concatenate_consecutive_literals(tokens);

	int count = lexer_append_tokens(lx, tokens);

	if(count < 1)
		lexer_report_error(lx, "Rule files specification is empty.\n");

	list_delete(tokens);

	return count;
}

struct token *lexer_read_command_argument(struct lexer *lx)
{
	int c = lexer_next_peek(lx);

	switch (c) {
	case CHAR_EOF:
		/* Found end of file while completing command */
		lx->lexeme_end++;
		lx->eof = 1;

		if(lx->depth == 0)
			lexer_report_error(lx, "Found end of file while completing command.\n");
		return NULL;
		break;
	case '\n':
		lexer_next_char(lx);	/* Jump \n */
		lexer_add_to_lexeme(lx, c);
		return lexer_pack_token(lx, TOKEN_NEWLINE);
		break;
	case '#':
		lexer_discard_comments(lx);
		lexer_add_to_lexeme(lx, '\n');
		return lexer_pack_token(lx, TOKEN_NEWLINE);
	case ' ':
	case '\t':
		return lexer_read_white_space(lx);
		break;
	case '$':
		return lexer_read_substitution(lx);
		break;
	case '"':
		return lexer_read_expandable(lx, '"');
		break;
	case '<':
	case '>':
		lexer_next_char(lx);	/* Jump <, > */
		lexer_add_to_lexeme(lx, c);
		return lexer_pack_token(lx, TOKEN_IO_REDIRECT);
		break;
	case '\'':
		lexer_add_to_lexeme(lx, '\'');
		lexer_read_literal(lx);
		lexer_add_to_lexeme(lx, '\'');
		return lexer_pack_token(lx, TOKEN_LITERAL);
		break;
	default:
		lexer_read_literal(lx);
		return lexer_pack_token(lx, TOKEN_LITERAL);
		break;
	}
}

/*
  With command parsing we do not push tokens *
  immediatly to the main token queue. This is because a command may be
  modified by things such as LOCAL, and we want to be able to use these
  modifiers in variable substitutions (e.g., MODIF=LOCAL, or
  MODIF=""). Under the hard rule that the parser should not see any
  substitutions, we first construct a queue of command tokens, examine
  it for modifiers, and then append the command tokens queue to the
  main tokens queue.
*/

struct list *lexer_read_command_aux(struct lexer *lx)
{
	int spaces_deleted = lexer_discard_white_space(lx);

	struct list *tokens = list_create();

	//Preserve space in substitutions.
	if(spaces_deleted && lx->depth > 0) {
		list_push_tail(tokens, lexer_pack_token(lx, TOKEN_SPACE));
	}

	/* Read all command tokens. Note that we read from lx, but put in lx_c. */
	while(1) {
		struct token *t = lexer_read_command_argument(lx);
		if(!t)
			break;

		if(t->type == TOKEN_SUBSTITUTION) {
			tokens = list_splice(tokens, lexer_expand_substitution(lx, t, lexer_read_command_aux));
			lexer_free_token(t);
			continue;
		} else {
			list_push_tail(tokens, t);
			if(t->type==TOKEN_NEWLINE) break;
		}
	}

	return tokens;
}

int lexer_read_command(struct lexer *lx)
{
	struct list *tokens = lexer_read_command_aux(lx);

	struct token *t;

	if(list_size(tokens) < 2) {
		/* If the only token in the list is a NEWLINE, then this is an empty line. */
		t = list_pop_head(tokens);
		lexer_free_token(t);
		list_delete(tokens);

		return 1;
	}

	/* Add command start marker.*/
	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_COMMAND));


	/* Merge command tokens into main queue. */
	/* First merge command modifiers, if any. */
	list_first_item(tokens);
	while((t = list_peek_head(tokens))) {
		if(t->type == TOKEN_LITERAL &&
		   ((strcmp(t->lexeme, "LOCAL")    == 0) ||
			(strcmp(t->lexeme, "MAKEFLOW") == 0)    )) {
			t = list_pop_head(tokens);
			lexer_push_token(lx, t);
		} else if(t->type == TOKEN_SPACE) {
			//Discard spaces between modifiers.
			t = list_pop_head(tokens);
			lexer_free_token(t);
		} else {
			break;
		}
	}

	/* Mark end of modifiers. */
	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_COMMAND_MOD_END));

	/* Now merge tha actual command tokens */

	/* Gives the number of actual command tokens, not taking into account command modifiers. */
	int count = 0;

	while((t = list_pop_head(tokens))) {
		count++;
		lexer_push_token(lx, t);
	}

	list_delete(tokens);

	if(count < 1)
		lexer_report_error(lx, "Command is empty.\n");

	return count;
}

int lexer_read_variable(struct lexer *lx, struct token *name)
{
	lexer_discard_white_space(lx);

	if(lexer_next_peek(lx) == '=') {
		lexer_next_char(lx);
		lexer_add_to_lexeme(lx, '=');
	} else {
		int c = lexer_next_char(lx);
		if(lexer_next_peek(lx) != '=')
			lexer_report_error(lx, "Missing = in variable definition.");
		lexer_add_to_lexeme(lx, c);
		lexer_next_char(lx);	/* Jump = */
	}

	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_VARIABLE));
	lexer_push_token(lx, name);

	lexer_discard_white_space(lx);

	//Read variable value
	lexer_push_token(lx, lexer_read_expandable(lx, '\n'));
	lexer_roll_back(lx, 1);	//Recover '\n'

	lexer_discard_white_space(lx);

	if(lexer_next_char(lx) != '\n')
		lexer_report_error(lx, "Missing newline at end of variable definition.");

	return 1;
}

int lexer_read_directive(struct lexer *lx, struct token *name)
{
	lexer_discard_white_space(lx);

	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_DIRECTIVE));
	lexer_push_token(lx, name);

	int c;

	lexer_discard_white_space(lx);

	while((c = lexer_next_peek(lx)) != '\n') {
		if(c == '#') {
			lexer_discard_comments(lx);
			lexer_roll_back(lx, 1);	//Recover the newline
			break;
		}

		int count = lexer_read_literal(lx);

		if(count < 1) {
			lexer_report_error(lx, "Literal value (alphanumeric or single-quote string) missing.");
		}

		struct token *t = lexer_pack_token(lx, TOKEN_LITERAL);
		lexer_push_token(lx, t);
		lexer_discard_white_space(lx);
	}

	lexer_add_to_lexeme(lx, lexer_next_char(lx));	//Drop the newline
	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_NEWLINE));

	return 1;
}

int lexer_read_variable_list(struct lexer * lx)
{
	int c;

	while((c = lexer_next_peek(lx)) != '\n') {
		lexer_discard_white_space(lx);
		if(c == '#') {
			lexer_discard_comments(lx);
			lexer_roll_back(lx, 1);	//Recover the newline
			break;
		}

		lexer_push_token(lx, lexer_read_syntax_name(lx));
	}

	lexer_add_to_lexeme(lx, lexer_next_char(lx));	//Drop the newline
	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_NEWLINE));

	return 1;
}

int lexer_unquoted_look_ahead_count(struct lexer *lx, char *char_set)
{
	char c = -1;
	int count = 0;

	int double_quote = 0;
	int single_quote = 0;

	do {
		c = lexer_next_char(lx);
		count++;

		if(double_quote || single_quote) {
			if(c == '"' && double_quote)
				double_quote = 0;
			else if(c == '\'' && single_quote)
				single_quote = 0;
		} else if(strchr(char_set, c)) {
			break;
		} else if(c == '\\') {
			lexer_next_char(lx);
			count++;
		} else if(c == '"') {
			double_quote = 1;
		} else if(c == '\'') {
			single_quote = 1;
		}

	} while(c != '\n' && c != CHAR_EOF);

	lexer_roll_back(lx, count);

	if(c == CHAR_EOF) {
		if(strchr(char_set, CHAR_EOF))
			return count;
		else
			return -1;
	} else if(c == '\n') {
		if(strchr(char_set, '\n'))
			return count;
		else
			return -1;
	} else
		return count;

}

int lexer_read_syntax_export(struct lexer *lx, struct token *name)
{
	lexer_discard_white_space(lx);

	//name->lexeme is "export"
	name->type = TOKEN_SYNTAX;
	lexer_push_token(lx, name);

	if(lexer_unquoted_look_ahead_count(lx, "=") > -1)
		lexer_read_variable(lx, lexer_read_syntax_name(lx));
	else
		lexer_read_variable_list(lx);

	lexer_push_token(lx, lexer_pack_token(lx, TOKEN_NEWLINE));

	return 1;
}

int lexer_read_syntax_or_variable(struct lexer * lx)
{
	lexer_discard_white_space(lx);

	char c = lexer_next_peek(lx);
	struct token *name = lexer_read_syntax_name(lx);

	if(strcmp("export", name->lexeme) == 0)
		return lexer_read_syntax_export(lx, name);
	else if(lexer_unquoted_look_ahead_count(lx, "=") > -1)
		return lexer_read_variable(lx, name);
	else if(c == '.')
		return lexer_read_directive(lx, name);
	else {
		lexer_roll_back(lx, strlen(name->lexeme));
		lexer_report_error(lx, "Unrecognized keyword: %s.", name->lexeme);
	}

	return 1;
}

int lexer_read_line(struct lexer * lx)
{
	char c = lexer_next_peek(lx);

	int colon, equal;

	switch (c) {
	case CHAR_EOF:
		/* Found end of file */
		return lexer_next_char(lx);
		break;
	case '#':
		lexer_discard_comments(lx);
		return 1;
		break;
	case '\t':
	case  ' ': /* A command starting with a space.. for backwards compatibility. */
		return lexer_read_command(lx);
		break;
	case '\n':
		/* Ignore empty lines and try again */
		lexer_next_char(lx);
		return lexer_read_line(lx);
		break;
	case '@':
		/* Jump @ */
		lexer_next_char(lx);
		return lexer_read_syntax_or_variable(lx);
		break;
	default:
		/* Either makeflow keyword (e.g. export), a file list, or variable assignment */
		lexer_discard_white_space(lx);

		colon = lexer_unquoted_look_ahead_count(lx, ":");
		equal = lexer_unquoted_look_ahead_count(lx, "=");

		/* If there is a colon and it appears before any existing
		 * equal sign read the line as a file list. */
		if((colon != -1) && (equal == -1 || colon < equal)) {
			lexer_read_file_list(lx);
		}
		else {
			lexer_read_syntax_or_variable(lx);
		}

		return 1;
		break;
	}
}

/* type: is either STREAM or CHAR */

struct lexer *lexer_create(int type, void *data, int line_number, int column_number)
{
	struct lexer *lx = malloc(sizeof(struct lexer));

	lx->line_number = line_number;
	lx->column_number = column_number;
	lx->column_numbers = list_create();

	lx->stream = NULL;
	lx->buffer = NULL;
	lx->eof = 0;

	lx->depth = 0;

	lx->lexeme = calloc(BUFFER_CHUNK_SIZE, sizeof(char));
	lx->lexeme_size = 0;
	lx->lexeme_max = BUFFER_CHUNK_SIZE;

	lx->token_queue = list_create();

	lx->buffer = calloc(2 * BUFFER_CHUNK_SIZE, sizeof(char));
	if(!lx->buffer)
		fatal("Could not allocate memory for input buffer.\n");

	lx->lexeme_end = (lx->buffer + 2 * BUFFER_CHUNK_SIZE - 2);

	if(type == STREAM) {
		lx->stream = (FILE *) data;

		lx->chunk_last_loaded = 2;	// Bootstrap load_chunk to load chunk 1.
		lexer_load_chunk(lx);
	} else {
		lexer_load_string(lx, (char *) data);
	}

	return lx;
}

struct lexer *lexer_create_substitution(struct lexer *lx, struct token *t)
{
	char *substitution = NULL;

	if(lx->environment) {
		substitution = dag_variable_lookup_string(t->lexeme, lx->environment);
	}

	struct lexer *lx_s;

	if(!substitution) {
		fatal("Variable %s has not yet been defined at line % " PRId64 ".\n", t->lexeme, lx->line_number);
		substitution = xxstrdup("");
	}

	lx_s = lexer_create(STRING, substitution, lx->line_number, lx->column_number);
	lx_s->depth = lx->depth + 1;

	if(lx_s->depth > MAX_SUBSTITUTION_DEPTH)
		lexer_report_error(lx, "More than %d recursive subsitutions attempted.\n", MAX_SUBSTITUTION_DEPTH);

	free(substitution);

	return lx_s;
}

void lexer_delete(struct lexer *lx)
{

	list_delete(lx->column_numbers);

	free(lx->lexeme);

	list_delete(lx->token_queue);

	free(lx->buffer);

	free(lx);
}

void lexer_free_token(struct token *t)
{
	free(t->lexeme);
	free(t);
}

struct token *lexer_peek_next_token(struct lexer *lx)
{
	if(list_size(lx->token_queue) == 0) {
		if(lx->eof)
			return NULL;

		lexer_read_line(lx);
		return lexer_peek_next_token(lx);
	}

	struct token *head = list_peek_head(lx->token_queue);

	return head;
}

struct token *lexer_next_token(struct lexer *lx)
{
	struct token *head = lexer_peek_next_token(lx);

	if(head)
	{
		if(lx->depth == 0) {
			char *str = lexer_print_token(head);
			debug(D_MAKEFLOW_LEXER, "%s", str);
			free(str);
		}
		list_pop_head(lx->token_queue);
	}

	return head;
}

#ifdef LEXER_TEST
int main(int argc, char **argv)
{
	FILE *ff = fopen("../example/example.makeflow", "r");

	struct lexer *lx = lexer_init_book(STREAM, ff, 1, 1);

	//debug_config(argv[0]);
	debug_config("lexer-test");
	debug_flags_set("all");

	verbose_parsing = 1;

	struct token *t;
	while((t = lexer_next_token(lx)) != NULL)
		print_token(stderr, t);


	return 0;
}
#endif


/* vim: set noexpandtab tabstop=4: */
