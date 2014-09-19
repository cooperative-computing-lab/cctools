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
int verbose_parsing = 1;
#endif

struct token *lexer_pack_token(struct lexer_book *bk, enum token_t type)
{
	struct token *t = malloc(sizeof(struct token));

	t->type = type;
	t->line_number = bk->line_number;
	t->column_number = bk->column_number;

	t->lexeme = calloc(bk->lexeme_size + 1, sizeof(char));
	memcpy(t->lexeme, bk->lexeme, bk->lexeme_size);
	*(t->lexeme + bk->lexeme_size) = '\0';

	bk->lexeme_size = 0;

	return t;
}

char *lexer_print_token(struct token *t)
{
	char str[1024]; 

	switch (t->type) {
	case SYNTAX:
		snprintf(str, 1024, "SYNTAX:  %s\n", t->lexeme);
		break;
	case NEWLINE:
		snprintf(str, 1024, "NEWLINE\n");
		break;
	case SPACE:
		snprintf(str, 1024, "SPACE\n");
		break;
	case FILES:
		snprintf(str, 1024, "FILES:  %s\n", t->lexeme);
		break;
	case VARIABLE:
		snprintf(str, 1024, "VARIABLE: %s\n", t->lexeme);
		break;
	case COLON:
		snprintf(str, 1024, "COLON\n");
		break;
	case REMOTE_RENAME:
		snprintf(str, 1024, "REMOTE_RENAME: %s\n", t->lexeme);
		break;
	case LITERAL:
		snprintf(str, 1024, "LITERAL: %s\n", t->lexeme);
		break;
	case SUBSTITUTION:
		snprintf(str, 1024, "SUBSTITUTION: %s\n", t->lexeme);
		break;
	case COMMAND:
		snprintf(str, 1024, "COMMAND: %s\n", t->lexeme);
		break;
	case COMMAND_MOD_END:
		snprintf(str, 1024, "COMMAND_MOD_END: %s\n", t->lexeme);
		break;
	case IO_REDIRECT:
		snprintf(str, 1024, "IO_REDIRECT: %s\n", t->lexeme);
		break;
	default:
		snprintf(str, 1024, "unknown: %s\n", t->lexeme);
		break;
	}
	
	return xxstrdup(str);
}

int lexer_push_token(struct lexer_book *bk, struct token *t)
{
	list_push_tail(bk->token_queue, t);
	return list_size(bk->token_queue);
}

int lexer_preppend_token(struct lexer_book *bk, struct token *t)
{
	list_push_head(bk->token_queue, t);
	return list_size(bk->token_queue);
}

void lexer_roll_back_one(struct lexer_book *bk)
{
	int c = *bk->lexeme_end;

	if(c == '\n') {

		bk->line_number--;
		bk->column_number = (uintptr_t) list_pop_head(bk->column_numbers);
	} else if(c == CHAR_EOF) {
		bk->eof = 0;
		bk->column_number--;
	} else {
		bk->column_number--;
	}

	if(bk->lexeme_end == bk->buffer)
		bk->lexeme_end = (bk->buffer + 2 * BUFFER_CHUNK_SIZE);

	bk->lexeme_end--;

	if(*bk->lexeme_end == '\0')
		bk->lexeme_end--;
}

void lexer_roll_back(struct lexer_book *bk, int offset)
{
	while(offset > 0) {
		offset--;
		lexer_roll_back_one(bk);
	}
}

void lexer_add_to_lexeme(struct lexer_book *bk, char c)
{
	if(bk->lexeme_size == bk->lexeme_max) {
		char *tmp = realloc(bk->lexeme, bk->lexeme_max + BUFFER_CHUNK_SIZE);
		if(!tmp) {
			fatal("Could not allocate memory for next token.\n");
		}
		bk->lexeme = tmp;
		bk->lexeme_max += BUFFER_CHUNK_SIZE;
	}

	*(bk->lexeme + bk->lexeme_size) = c;
	bk->lexeme_size++;
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

void lexer_report_error(struct lexer_book *bk, char *message, ...)
{
	va_list ap;
	va_start(ap, message);

	char message_filled[BUFFER_CHUNK_SIZE];

	vsprintf(message_filled, message, ap);
	//fprintf(stderr, "%s line: %ld column: %ld\n", message_filled, bk->line_number, bk->column_number);
	//abort();
	fatal("%s line: %d column: %d\n", message_filled, bk->line_number, bk->column_number);

	va_end(ap);

}

//Useful for debugging:
void lexer_print_queue(struct lexer_book *bk)
{
	struct token *t;
	
	debug(D_MFLEX, "Queue: ");

	list_first_item(bk->token_queue);
	while((t = list_next_item(bk->token_queue)))
		debug(D_MFLEX, "%s", lexer_print_token(t));
	list_first_item(bk->token_queue);

	debug(D_MFLEX, "End queue.");
}

void lexer_load_chunk(struct lexer_book *bk)
{

	if(bk->chunk_last_loaded == 2 && bk->lexeme_end == bk->buffer)
		bk->chunk_last_loaded = 1;
	else if(bk->chunk_last_loaded == 1 && bk->lexeme_end != bk->buffer)
		bk->chunk_last_loaded = 2;
	else
		return;

	int bread = fread(bk->lexeme_end, sizeof(char), BUFFER_CHUNK_SIZE - 1, bk->stream);

	*(bk->buffer + BUFFER_CHUNK_SIZE - 1) = '\0';
	*(bk->buffer + 2 * BUFFER_CHUNK_SIZE - 1) = '\0';

	if(bk->lexeme_end >= bk->buffer + 2 * BUFFER_CHUNK_SIZE)
		fatal("End of token is out of bounds.\n");

	if(bread < BUFFER_CHUNK_SIZE - 1)
		*(bk->lexeme_end + bread) = CHAR_EOF;

}

void lexer_load_string(struct lexer_book *bk, char *s)
{
	int len = strlen(s);

	bk->chunk_last_loaded = 1;

	strcpy(bk->buffer, s);
	*(bk->buffer + len) = CHAR_EOF;

	*(bk->buffer + 2 * BUFFER_CHUNK_SIZE - 1) = '\0';

	if(bk->lexeme_end >= bk->buffer + 2 * BUFFER_CHUNK_SIZE)
		fatal("End of token is out of bounds.\n");

}


char lexer_next_char(struct lexer_book *bk)
{
	if(*bk->lexeme_end == CHAR_EOF) {
		return CHAR_EOF;
	}

	/* If at the end of chunk, load the next chunk. */
	if(((bk->lexeme_end + 1) == (bk->buffer + BUFFER_CHUNK_SIZE - 1)) || ((bk->lexeme_end + 1) == (bk->buffer + 2 * BUFFER_CHUNK_SIZE - 1))) {
		if(bk->lexeme_max == BUFFER_CHUNK_SIZE - 1)
			lexer_report_error(bk, "Input buffer is full. Runaway token?");	//BUG: This is really a recoverable error, increase the buffer size.
		/* Wrap around the file chunks */
		else if(bk->lexeme_end == bk->buffer + 2 * BUFFER_CHUNK_SIZE - 2)
			bk->lexeme_end = bk->buffer;
		/* Position at the beginning of next chunk */
		else
			bk->lexeme_end += 2;

		lexer_load_chunk(bk);
	} else
		bk->lexeme_end++;

	char c = *bk->lexeme_end;

	if(c == '\n') {
		bk->line_number++;
		list_push_head(bk->column_numbers, (uint64_t *) bk->column_number);
		bk->column_number = 1;
	} else {
		bk->column_number++;
	}

	if(c == CHAR_EOF) {
		bk->eof = 1;
	}

	return c;
}

int lexer_next_peek(struct lexer_book *bk)
{
	/* Read next chunk if necessary */
	int c = lexer_next_char(bk);
	lexer_roll_back(bk, 1);

	return c;
}

int lexer_peek_remote_rename_syntax(struct lexer_book *bk)
{
	if(lexer_next_peek(bk) != '-')
		return 0;

	lexer_next_char(bk);

	int is_gt = (lexer_next_peek(bk) == '>');
	lexer_roll_back(bk, 1);

	return is_gt;
}


/* Read characters until a character in char_set is found. (exclusive) */
/* Returns the count of characters that we would have to roll-back to
   undo the read. */
int lexer_read_until(struct lexer_book *bk, char *char_set)
{
	int count = 0;
	char c;

	do {
		c = lexer_next_peek(bk);
		if(strchr(char_set, c)) {
			return count;
		}

		if(c != CHAR_EOF)
			lexer_add_to_lexeme(bk, c);

		lexer_next_char(bk);
		count++;

	} while(c != CHAR_EOF);

	bk->eof = 1;

	return count;
}

/* A comment starts with # and ends with a newline, or end-of-file */
void lexer_discard_comments(struct lexer_book *bk)
{
	if(lexer_next_peek(bk) != '#')
		lexer_report_error(bk, "Expecting a comment.");

	char c;
	do {
		c = lexer_next_char(bk);
	} while(c != '\n' && c != CHAR_EOF);
}

/* As lexer_read_until, but elements of char_set preceded by \ are
   ignored as stops, with \n replaced with spaces. */
int lexer_read_escaped_until(struct lexer_book *bk, char *char_set)
{
	char *char_set_slash = string_format("\\%s", char_set);

	int count = 0;

	do {
		count += lexer_read_until(bk, char_set_slash);

		if(!bk->eof && lexer_next_peek(bk) == '\\') {
			lexer_next_char(bk);	/* Jump the slash */
			char c = lexer_next_char(bk);
			count += 2;

			if(lexer_next_peek(bk) != CHAR_EOF) {
				if(c == '\n') {
					lexer_add_to_lexeme(bk, ' ');
				} else {
					lexer_add_to_lexeme(bk, c);
				}
			}
		} else
			break;

	} while(!bk->eof);

	free(char_set_slash);

	if(bk->eof && !strchr(char_set, CHAR_EOF))
		lexer_report_error(bk, "Missing %s\n", char_set);

	return count;
}

int lexer_read_literal_unquoted(struct lexer_book * bk)
{
	return lexer_read_escaped_until(bk, LITERAL_LIMITS);
}

/* Read everything between single quotes */
int lexer_read_literal_quoted(struct lexer_book * bk)
{
	int c = lexer_next_peek(bk);

	if(c != '\'')
		lexer_report_error(bk, "Missing opening quote.\n");

	lexer_add_to_lexeme(bk, lexer_next_char(bk));	/* Add first ' */

	int count = lexer_read_escaped_until(bk, "'");

	lexer_add_to_lexeme(bk, lexer_next_char(bk));	/* Add second ' */

	return count;
}

int lexer_read_literal(struct lexer_book * bk)
{
	int c = lexer_next_peek(bk);

	if(c == '\'')
		return lexer_read_literal_quoted(bk);
	else
		return lexer_read_literal_unquoted(bk);
}

/* We read a string that can have $ substitutions. We read literals
   between special symbols, such as $ " ' etc., interpreting them as
   we go. end_marker indicates if the expandable should end with a
   newline (as for variable assignment), or with double quote ".*/

struct token *lexer_read_literal_in_expandable_until(struct lexer_book *bk, char end_marker)
{
	char end_markers[7] = { end_marker, '$', '\\', '"', '\'', '#', CHAR_EOF };

	int count = 0;
	do {
		count += lexer_read_until(bk, end_markers);

		if(bk->eof)
			break;

		char c = lexer_next_peek(bk);
		if(c == '\\') {
			lexer_next_char(bk);	/* Jump the slash */
			char n = lexer_next_char(bk);
			count += 2;

			if(lexer_special_escape(n)) {
					lexer_add_to_lexeme(bk, lexer_special_to_code(n));
			} else if(n == '\n') {
				lexer_add_to_lexeme(bk, ' ');
			} else {
				lexer_add_to_lexeme(bk, n);
			}
		} else if(c == '#') {
			if(end_marker == '\n') {
				lexer_discard_comments(bk);
				break;
			}
		} else
			break;
	} while(!bk->eof);

	if(bk->eof && strchr(")\"'", end_marker))
		lexer_report_error(bk, "Missing closing %c.\n", end_marker);

	return lexer_pack_token(bk, LITERAL);
}

/* Read a filename, adding '-' to names when - is not followed by
   >. The 'recursive' comes because the function calls itself when
   completing a name when it added a -. */
int lexer_read_filename_recursive(struct lexer_book *bk)
{
	int count = lexer_read_escaped_until(bk, FILENAME_LIMITS);

	if(count < 1)
		return count;

	if(lexer_next_peek(bk) == '-' && !lexer_peek_remote_rename_syntax(bk)) {
		lexer_add_to_lexeme(bk, '-');
		count++;
		count += lexer_read_filename_recursive(bk);
	}

	return count;
}

struct token *lexer_read_filename(struct lexer_book *bk)
{
	int count = lexer_read_filename_recursive(bk);

	if(count < 1)
		lexer_report_error(bk, "Expecting a filename.");

	return lexer_pack_token(bk, LITERAL);
}


struct token *lexer_read_syntax_name(struct lexer_book *bk)
{
	int count;
	count = lexer_read_until(bk, SYNTAX_LIMITS);

	if(count < 1)
		lexer_report_error(bk, "Expecting a keyword or a variable name.");

	return lexer_pack_token(bk, LITERAL);
}

struct token *lexer_read_substitution(struct lexer_book *bk)
{
	char closer = 0;                  //closer is either 0 (no closer), ) or }.
	char c = lexer_next_peek(bk);

	if(c != '$')
		lexer_report_error(bk, "Expecting $ for variable substitution.");

	lexer_next_char(bk);	/* Jump $ */

	if(lexer_next_peek(bk) == '(') {
		lexer_next_char(bk);	/* Jump ( */
		closer = ')';
	} else if(lexer_next_peek(bk) == '{') {
		lexer_next_char(bk);	/* Jump { */
		closer = '}';
	}

	struct token *name = lexer_read_syntax_name(bk);
	name->type = SUBSTITUTION;

	if(closer) {
		if(lexer_next_peek(bk) == closer)
			lexer_next_char(bk);	/* Jump ) */
		else
			lexer_report_error(bk, "Expecting %c for closing variable substitution.", closer);
	}

	return name;
}

int lexer_discard_white_space(struct lexer_book *bk)
{
	int count = 0;
	while(strchr(WHITE_SPACE, lexer_next_peek(bk))) {
		lexer_next_char(bk);
		count++;
	}
	
	return count;
}

/* Consolidates a sequence of white space into a single SPACE token */
struct token *lexer_read_white_space(struct lexer_book *bk)
{
	int count = lexer_discard_white_space(bk);

	while(strchr(WHITE_SPACE, lexer_next_peek(bk))) {
		count++;
		lexer_next_char(bk);
	}

	if(count > 0) {
		lexer_add_to_lexeme(bk, ' ');
		return lexer_pack_token(bk, SPACE);
	} else
		lexer_report_error(bk, "Expecting white space.");

	return NULL;
}

//opened tracks whether it is the opening (opened = 0) or closing (opened = 1) double quote we encounter.
struct list *lexer_read_expandable_recursive(struct lexer_book *bk, char end_marker, int opened)
{
	lexer_discard_white_space(bk);
	
	struct list *tokens = list_create();

	while(!bk->eof) {
		int c = lexer_next_peek(bk);
		
		if(c == '$') {
			list_push_tail(tokens, lexer_read_substitution(bk));
		} 

		if(c == '\'') {
			lexer_read_literal(bk);
			list_push_tail(tokens, lexer_pack_token(bk, LITERAL));
		} else if(c == '"' && opened == 0) {
				lexer_add_to_lexeme(bk, lexer_next_char(bk));
				list_push_tail(tokens, lexer_pack_token(bk, LITERAL));     // Add first "
				tokens = list_splice(tokens, lexer_read_expandable_recursive(bk, '"', 1));
				lexer_add_to_lexeme(bk, '"');
				list_push_tail(tokens, lexer_pack_token(bk, LITERAL));     // Add closing "
				if(end_marker == '"')
					return tokens;
		} else if(c == '#' && end_marker != '"') {
			lexer_discard_comments(bk);
		} else if(c == end_marker) {
			lexer_next_char(bk);	/* Jump end_marker */
			return tokens;
		} else {
			list_push_tail(tokens, lexer_read_literal_in_expandable_until(bk, end_marker));
		}
	}

	lexer_report_error(bk, "Found EOF before end marker: %c.\n", end_marker);
	
	return NULL;
}

struct token *lexer_concat_expandable(struct lexer_book *bk, struct list *tokens)
{
	struct token *t;

	struct buffer b;
	buffer_init(&b);

	char *substitution;

	list_first_item(tokens);
	
	while((t = list_pop_head(tokens))) {
		switch(t->type) {
		case SUBSTITUTION:
			substitution = dag_lookup_str(t->lexeme, bk->environment);
			if(!substitution)
				fatal("Variable %s has not yet been defined at line % " PRId64 ".\n", t->lexeme, bk->line_number);
			buffer_printf(&b, "%s", substitution);
			free(substitution);
			break;
		case LITERAL:
			if(strcmp(t->lexeme, "") != 0)           // Skip empty strings.
				buffer_printf(&b, "%s", t->lexeme);
			break;
		default:
			lexer_report_error(bk, "Error in expansion, got: %s.\n", lexer_print_token(t));
			break;
		}

		lexer_free_token(t);
	}
	
	t = lexer_pack_token(bk, LITERAL);
	t->lexeme = xxstrdup(buffer_tostring(&b, NULL));
	buffer_free(&b);
	
	return t;
}

struct token *lexer_read_expandable(struct lexer_book *bk, char end_marker)
{
	struct list *tokens = lexer_read_expandable_recursive(bk, end_marker, 0);
	
	struct token *t = lexer_concat_expandable(bk, tokens); 
	
	list_delete(tokens);

	return t;
}


int lexer_append_tokens(struct lexer_book *bk, struct list *tokens)
{
	struct token *t;
	int count = 0;

	list_first_item(tokens);
	while((t = list_pop_head(tokens))) {
		lexer_push_token(bk, t);
		count++;
	}

	return count;
}

struct list *lexer_expand_substitution(struct lexer_book *bk, struct token *t, 
							  struct list * (*list_reader)(struct lexer_book *))
{
	struct lexer_book *bk_s = lexer_init_substitution_book(bk, t);
	struct list *tokens     = list_reader(bk_s);
	
	lexer_free_book(bk_s);

	return tokens;
}

struct token *lexer_read_file(struct lexer_book *bk)
{
	int c = lexer_next_peek(bk);

	switch (c) {
	case CHAR_EOF:
		bk->lexeme_end++;
		bk->eof = 1;
		if(bk->depth == 0)
			lexer_report_error(bk, "Found end of file while completing file list.\n");
		return NULL;
		break;
	case '\n':
		lexer_next_char(bk);	/* Jump \n */
		lexer_add_to_lexeme(bk, c);
		return lexer_pack_token(bk, NEWLINE);
		break;
	case '#':
		lexer_discard_comments(bk);
		lexer_add_to_lexeme(bk, '\n');
		return lexer_pack_token(bk, NEWLINE);
	case ':':
		lexer_next_char(bk);	/* Jump : */
		return lexer_pack_token(bk, COLON);
		break;
	case ' ':
	case '\t':
		/* Discard white-space and add space token. */
		lexer_discard_white_space(bk);
		return lexer_pack_token(bk, SPACE);
		break;
	case '$':
		return lexer_read_substitution(bk);
		break;
	case '\'':
		lexer_add_to_lexeme(bk, '\'');
		lexer_read_literal_quoted(bk);
		lexer_add_to_lexeme(bk, '\'');
		return lexer_pack_token(bk, LITERAL);
		break;
	case '-':
		if(lexer_peek_remote_rename_syntax(bk)) {
			lexer_next_char(bk);	/* Jump -> */
			lexer_next_char(bk);
			return lexer_pack_token(bk, REMOTE_RENAME);
		}
		/* Else fall through */
	default:
		return lexer_read_filename(bk);
		break;
	}
}

struct list *lexer_read_file_list_aux(struct lexer_book *bk)
{
	struct list *tokens = list_create();

	lexer_discard_white_space(bk);

	struct token *t;
	do {
		t = lexer_read_file(bk);
		if(!t)
			break;

		//Do substitution recursively
		if(t->type == SUBSTITUTION) {
			tokens = list_splice(tokens, lexer_expand_substitution(bk, t, lexer_read_file_list_aux));
			lexer_free_token(t);
		} else { 
			list_push_tail(tokens, t);
		}
	} while(t->type != NEWLINE);

	return tokens;
}

void lexer_concatenate_consecutive_literals(struct list *tokens)
{
	struct list *tmp = list_create();
	struct token *t, *prev  = NULL;
	
	list_first_item(tokens);
	while((t = list_pop_head(tokens))) {
		if(t->type != LITERAL) {
			list_push_tail(tmp, t);
			continue;
		}
		
		prev = list_pop_tail(tmp);
		
		if(!prev) {
			list_push_tail(tmp, t);
			continue;
		}
		
		if(prev->type != LITERAL) {
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
		if(t->type != SPACE) {
			list_push_tail(tokens, t);
		} else {
			lexer_free_token(t);
		}
	
	list_delete(tmp);
}

int lexer_read_file_list(struct lexer_book *bk)
{
	/* Add file list start marker */
	lexer_push_token(bk, lexer_pack_token(bk, FILES));
	
	struct list *tokens = lexer_read_file_list_aux(bk);
	
	lexer_concatenate_consecutive_literals(tokens);

	int count = lexer_append_tokens(bk, tokens);

	if(count < 1)
		lexer_report_error(bk, "Rule files specification is empty.\n");
	
	list_delete(tokens);
	
	return count;
}

struct token *lexer_read_command_argument(struct lexer_book *bk)
{
	int c = lexer_next_peek(bk);

	switch (c) {
	case CHAR_EOF:
		/* Found end of file while completing command */
		bk->lexeme_end++;
		bk->eof = 1;

		if(bk->depth == 0)
			lexer_report_error(bk, "Found end of file while completing command.\n");
		return NULL;
		break;
	case '\n':
		lexer_next_char(bk);	/* Jump \n */
		lexer_add_to_lexeme(bk, c);
		return lexer_pack_token(bk, NEWLINE);
		break;
	case '#':
		lexer_discard_comments(bk);
		lexer_add_to_lexeme(bk, '\n');
		return lexer_pack_token(bk, NEWLINE);
	case ' ':
	case '\t':
		return lexer_read_white_space(bk);
		break;
	case '$':
		return lexer_read_substitution(bk);
		break;
	case '"':
		return lexer_read_expandable(bk, '"');
		break;
	case '<':
	case '>':
		lexer_next_char(bk);	/* Jump <, > */
		lexer_add_to_lexeme(bk, c);
		return lexer_pack_token(bk, IO_REDIRECT);
		break;
	case '\'':
		lexer_add_to_lexeme(bk, '\'');
		lexer_read_literal(bk);
		lexer_add_to_lexeme(bk, '\'');
		return lexer_pack_token(bk, LITERAL);
		break;
	default:
		lexer_read_literal(bk);
		return lexer_pack_token(bk, LITERAL);
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

struct list *lexer_read_command_aux(struct lexer_book *bk)
{
	int spaces_deleted = lexer_discard_white_space(bk);
	
	struct list *tokens = list_create(); 

	//Preserve space in substitutions.
	if(spaces_deleted && bk->depth > 0) {
		list_push_tail(tokens, lexer_pack_token(bk, SPACE)); 
	}

	/* Read all command tokens. Note that we read from bk, but put in bk_c. */
	struct token *t;

	do {
		t = lexer_read_command_argument(bk);
		if(!t)
			break;

		if(t->type == SUBSTITUTION) {
			tokens = list_splice(tokens, lexer_expand_substitution(bk, t, lexer_read_command_aux));
			lexer_free_token(t);
		} else { 
			list_push_tail(tokens, t);
		}
	} while(t->type != NEWLINE);
	
	return tokens;
}

int lexer_read_command(struct lexer_book *bk)
{
	struct list *tokens = lexer_read_command_aux(bk);
	
	/* Add command start marker.*/
	lexer_push_token(bk, lexer_pack_token(bk, COMMAND));

	struct token *t;

	/* Merge command tokens into main queue. */
	/* First merge command modifiers, if any. */
	list_first_item(tokens);
	while((t = list_peek_head(tokens))) {
		if(t->type == LITERAL &&
		   ((strcmp(t->lexeme, "LOCAL")    == 0) || 
			(strcmp(t->lexeme, "MAKEFLOW") == 0)    )) {
			t = list_pop_head(tokens);
			lexer_push_token(bk, t);
		} else if(t->type == SPACE) {
			//Discard spaces between modifiers.
			t = list_pop_head(tokens);
			lexer_free_token(t);
		} else {
			break;
		}
	}

	/* Mark end of modifiers. */
	lexer_push_token(bk, lexer_pack_token(bk, COMMAND_MOD_END));
	
	/* Now merge tha actual command tokens */

	/* Gives the number of actual command tokens, not taking into account command modifiers. */
	int count = 0;

	while((t = list_pop_head(tokens))) {
		count++;
		lexer_push_token(bk, t);
	}
	
	list_delete(tokens);
	
	if(count < 1)
		lexer_report_error(bk, "Command is empty.\n");
	
	return count;
}

int lexer_read_variable(struct lexer_book *bk, struct token *name)
{
	lexer_discard_white_space(bk);

	if(lexer_next_peek(bk) == '=') {
		lexer_next_char(bk);
		lexer_add_to_lexeme(bk, '=');
	} else {
		int c = lexer_next_char(bk);
		if(lexer_next_peek(bk) != '=')
			lexer_report_error(bk, "Missing = in variable definition.");
		lexer_add_to_lexeme(bk, c);
		lexer_next_char(bk);	/* Jump = */
	}

	lexer_push_token(bk, lexer_pack_token(bk, VARIABLE));
	lexer_push_token(bk, name);

	lexer_discard_white_space(bk);

	//Read variable value
	lexer_push_token(bk, lexer_read_expandable(bk, '\n'));
	lexer_roll_back(bk, 1);	//Recover '\n'

	lexer_discard_white_space(bk);

	if(lexer_next_char(bk) != '\n')
		lexer_report_error(bk, "Missing newline at end of variable definition.");

	return 1;
}

int lexer_read_variable_list(struct lexer_book * bk)
{
	int c;

	while((c = lexer_next_peek(bk)) != '\n') {
		lexer_discard_white_space(bk);
		if(c == '#') {
			lexer_discard_comments(bk);
			lexer_roll_back(bk, 1);	//Recover the newline
			break;
		}

		lexer_push_token(bk, lexer_read_syntax_name(bk));
	}

	lexer_add_to_lexeme(bk, lexer_next_char(bk));	//Drop the newline
	lexer_push_token(bk, lexer_pack_token(bk, NEWLINE));

	return 1;
}

int lexer_unquoted_look_ahead_count(struct lexer_book *bk, char *char_set)
{
	char c = -1;
	int count = 0;

	int double_quote = 0;
	int single_quote = 0;

	do {
		c = lexer_next_char(bk);
		count++;

		if(double_quote || single_quote) {
			if(c == '"' && double_quote)
				double_quote = 0;
			else if(c == '\'' && single_quote)
				single_quote = 0;
		} else if(strchr(char_set, c)) {
			break;
		} else if(c == '\\') {
			lexer_next_char(bk);
			count++;
		} else if(c == '"') {
			double_quote = 1;
		} else if(c == '\'') {
			single_quote = 1;
		}

	} while(c != '\n' && c != CHAR_EOF);

	lexer_roll_back(bk, count);

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

int lexer_read_syntax_export(struct lexer_book *bk, struct token *name)
{
	lexer_discard_white_space(bk);

	//name->lexeme is "export"
	name->type = SYNTAX;
	lexer_push_token(bk, name);

	if(lexer_unquoted_look_ahead_count(bk, "=") > -1)
		lexer_read_variable(bk, lexer_read_syntax_name(bk));
	else
		lexer_read_variable_list(bk);

	lexer_push_token(bk, lexer_pack_token(bk, NEWLINE));

	return 1;
}

int lexer_read_syntax_or_variable(struct lexer_book * bk)
{
	lexer_discard_white_space(bk);
	struct token *name = lexer_read_syntax_name(bk);

	if(strcmp("export", name->lexeme) == 0)
		return lexer_read_syntax_export(bk, name);
	else if(lexer_unquoted_look_ahead_count(bk, "=") > -1)
		return lexer_read_variable(bk, name);
	else {
		lexer_roll_back(bk, strlen(name->lexeme));
		lexer_report_error(bk, "Unrecognized keyword: %s.", name->lexeme);
	}
	
	return 1;
}

int lexer_read_line(struct lexer_book * bk)
{
	char c = lexer_next_peek(bk);

	int colon, equal;

	switch (c) {
	case CHAR_EOF:
		/* Found end of file */
		return lexer_next_char(bk);
		break;
	case '#':
		lexer_discard_comments(bk);
		return 1;
		break;
	case '\t':
		return lexer_read_command(bk);
		break;
	case ' ':
		/* Eat whitespace and try again */
		lexer_discard_white_space(bk);
		return lexer_read_line(bk);
	case '\n':
		/* Ignore empty lines and try again */
		lexer_next_char(bk);
		return lexer_read_line(bk);
		break;
	case '@':
		/* Jump @ */
		lexer_next_char(bk);
		return lexer_read_syntax_or_variable(bk);
		break;
	default:
		/* Either makeflow keyword (e.g. export), a file list, or variable assignment */
		lexer_discard_white_space(bk);

		colon = lexer_unquoted_look_ahead_count(bk, ":");
		equal = lexer_unquoted_look_ahead_count(bk, "=");

		if((colon != -1) && (equal == -1 || colon < equal)) {
			lexer_read_file_list(bk);
		} else {
			lexer_read_syntax_or_variable(bk);
		}
		return 1;
		break;
	}
}

/* type: is either STREAM or CHAR */

struct lexer_book *lexer_init_book(int type, void *data, int line_number, int column_number)
{
	struct lexer_book *bk = malloc(sizeof(struct lexer_book));

	bk->line_number = line_number;
	bk->column_number = column_number;
	bk->column_numbers = list_create(0);

	bk->stream = NULL;
	bk->buffer = NULL;
	bk->eof = 0;
	
	bk->depth = 0;

	bk->lexeme = calloc(BUFFER_CHUNK_SIZE, sizeof(char));
	bk->lexeme_size = 0;
	bk->lexeme_max = BUFFER_CHUNK_SIZE;

	bk->token_queue = list_create(0);

	bk->buffer = calloc(2 * BUFFER_CHUNK_SIZE, sizeof(char));
	if(!bk->buffer)
		fatal("Could not allocate memory for input buffer.\n");

	bk->lexeme_end = (bk->buffer + 2 * BUFFER_CHUNK_SIZE - 2);

	if(type == STREAM) {
		bk->stream = (FILE *) data;

		bk->chunk_last_loaded = 2;	// Bootstrap load_chunk to load chunk 1.
		lexer_load_chunk(bk);
	} else {
		lexer_load_string(bk, (char *) data);
	}

	return bk;
}

struct lexer_book *lexer_init_substitution_book(struct lexer_book *bk, struct token *t)
{
	char *substitution = NULL;
	
	if(bk->environment) {
		substitution = dag_lookup_str(t->lexeme, bk->environment);
	}

	struct lexer_book *bk_s;

	if(!substitution) {
		fatal("Variable %s has not yet been defined at line % " PRId64 ".\n", t->lexeme, bk->line_number);
		substitution = xxstrdup("");
	}

	bk_s = lexer_init_book(STRING, substitution, bk->line_number, bk->column_number);
	bk_s->depth = bk->depth + 1;
	
	if(bk_s->depth > MAX_SUBSTITUTION_DEPTH)
		lexer_report_error(bk, "More than %d recursive subsitutions attempted.\n", MAX_SUBSTITUTION_DEPTH);

	free(substitution);

	return bk_s;
}

void lexer_free_book(struct lexer_book *bk)
{

	list_free(bk->column_numbers);

	free(bk->lexeme);

	list_free(bk->token_queue);

	free(bk->buffer);

	free(bk);
}

void lexer_free_token(struct token *t)
{
	free(t->lexeme);
	free(t);
}

struct token *lexer_peek_next_token(struct lexer_book *bk)
{
	if(list_size(bk->token_queue) == 0) {
		if(bk->eof)
			return NULL;

		lexer_read_line(bk);
		return lexer_peek_next_token(bk);
	}

	struct token *head = list_peek_head(bk->token_queue);
	
	return head;
}

struct token *lexer_next_token(struct lexer_book *bk)
{
	struct token *head = lexer_peek_next_token(bk);
	
	if(head)
	{
		if(bk->depth == 0)
			debug(D_MFLEX, "%s", lexer_print_token(head));

	    list_pop_head(bk->token_queue);
	}

	return head;
}

#ifdef LEXER_TEST
int main(int argc, char **argv)
{
	FILE *ff = fopen("../example/example.makeflow", "r");

	struct lexer_book *bk = lexer_init_book(STREAM, ff, 1, 1);

	//debug_config(argv[0]);
	debug_config("lexer-test");
	debug_flags_set("all");


	struct token *t;
	while((t = lexer_next_token(bk)) != NULL)
		print_token(stderr, t);


	return 0;
}
#endif


/* vim: set noexpandtab tabstop=4: */
