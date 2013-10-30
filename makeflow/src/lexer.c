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

#include "dag.h"
#include "lexer.h"

#define CHAR_EOF 26		// ASCII for EOF

#define LITERAL_LIMITS  "\\\"'$#:\n\t \032"
#define SYNTAX_LIMITS  LITERAL_LIMITS  ",.-(){},[]<>=+!?"
#define FILENAME_LIMITS LITERAL_LIMITS  "-"

#define WHITE_SPACE          " \t"
#define BUFFER_CHUNK_SIZE 1048576	// One megabyte

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

void print_token(FILE * stream, struct token *t)
{
	switch (t->type) {
	case SYNTAX:
		fprintf(stream, "syntax:  %d. %s\n", t->type, t->lexeme);
		break;
	case NEWLINE:
		fprintf(stream, "newline: %d. %s\n", t->type, t->lexeme);
		break;
	case SPACE:
		fprintf(stream, "space:   %d. %s\n", t->type, t->lexeme);
		break;
	case FILES:
		fprintf(stream, "files:  %d. %s\n", t->type, t->lexeme);
		break;
	case VARIABLE:
		fprintf(stream, "variabl: %d. %s\n", t->type, t->lexeme);
		break;
	case COLON:
		fprintf(stream, "colon:  %d. %s\n", t->type, t->lexeme);
		break;
	case REMOTE_RENAME:
		fprintf(stream, "rename: %d. %s\n", t->type, t->lexeme);
		break;
	case LITERAL:
		fprintf(stream, "literal: %d. %s\n", t->type, t->lexeme);
		break;
	case LEXPANDABLE:
		fprintf(stream, "expandL: %d. %s\n", t->type, t->lexeme);
		break;
	case REXPANDABLE:
		fprintf(stream, "expandR: %d. %s\n", t->type, t->lexeme);
		break;
	case SUBSTITUTION:
		fprintf(stream, "substit: %d. %s\n", t->type, t->lexeme);
		break;
	case COMMAND:
		fprintf(stream, "command: %d. %s\n", t->type, t->lexeme);
		break;
	case IO_REDIRECT:
		fprintf(stream, "redirec: %d. %s\n", t->type, t->lexeme);
		break;
	default:
		fprintf(stream, "unknown: %d. %s\n", t->type, t->lexeme);
	}
}

int lexer_push_token(struct lexer_book *bk, struct token *t)
{
	list_push_tail(bk->token_queue, t);
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
	fprintf(stderr, "%s line: %ld column: %ld\n", message_filled, bk->line_number, bk->column_number);
	abort();
	fatal("%s line: %d column: %d\n", message_filled, bk->line_number, bk->column_number);

	va_end(ap);

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
   ignored as stops. */
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

			if(lexer_next_peek(bk) != CHAR_EOF)
				lexer_add_to_lexeme(bk, c);
		} else
			break;

	} while(!bk->eof);

	free(char_set_slash);

	if(bk->eof && !strchr(char_set, CHAR_EOF))
		lexer_report_error(bk, "Missing %s\n", char_set);

	return count;
}

accept_t lexer_read_literal_unquoted(struct lexer_book * bk)
{
	int count = lexer_read_escaped_until(bk, LITERAL_LIMITS);

	if(count > 0)
		return YES;
	else
		return NO;

}

/* Read everything between single quotes */
accept_t lexer_read_literal_quoted(struct lexer_book * bk)
{
	int c = lexer_next_peek(bk);

	if(c != '\'')
		return NO;

	lexer_next_char(bk);	/* Jump first ' */

	int count = lexer_read_escaped_until(bk, "'");
	if(count >= 0) {
		lexer_next_char(bk);	/* Jump second ' */
		return YES;
	} else
		return NO;
}

accept_t lexer_read_literal(struct lexer_book * bk)
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

			if(lexer_special_escape(n))
				lexer_add_to_lexeme(bk, lexer_special_to_code(n));
			else
				lexer_add_to_lexeme(bk, n);
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
	int parenthesis = 0;
	char c = lexer_next_peek(bk);

	if(c != '$')
		lexer_report_error(bk, "Expecting $ for variable substitution.");

	lexer_next_char(bk);	/* Jump $ */

	if(lexer_next_peek(bk) == '(') {
		lexer_next_char(bk);	/* Jump ( */
		parenthesis = 1;
	}

	struct token *name = lexer_read_syntax_name(bk);
	name->type = SUBSTITUTION;

	if(parenthesis) {
		if(lexer_next_peek(bk) == ')')
			lexer_next_char(bk);	/* Jump ) */
		else
			lexer_report_error(bk, "Expecting ) for closing variable substitution.");
	}

	return name;
}

void lexer_discard_white_space(struct lexer_book *bk)
{
	while(strchr(WHITE_SPACE, lexer_next_peek(bk)))
		lexer_next_char(bk);
}

/* Consolidates a sequence of white space into a single SPACE token */
struct token *lexer_read_white_space(struct lexer_book *bk)
{
	int count = 0;

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

int lexer_read_expandable_recursive(struct lexer_book *bk, char end_marker)
{
	int count = 0;
	lexer_discard_white_space(bk);

	while(!bk->eof) {
		int c = lexer_next_peek(bk);

		if(c == '$') {
			count++;
			lexer_push_token(bk, lexer_read_substitution(bk));
		}
		if(c == '\'') {
			lexer_read_literal(bk);
			lexer_push_token(bk, lexer_pack_token(bk, LITERAL));
		} else if(c == end_marker) {
			lexer_next_char(bk);	/* Jump end_marker */
			return count;
		} else if(c == '"')
			count += lexer_read_expandable_recursive(bk, '"');
		else if(c == '#' && end_marker != '"') {
			lexer_discard_comments(bk);
			return count;
		} else {
			count++;
			lexer_push_token(bk, lexer_read_literal_in_expandable_until(bk, end_marker));
		}
	}
	/* Found eof before end_marker */
	abort();
}

struct token *lexer_read_expandable(struct lexer_book *bk, char end_marker)
{
	struct token *start = lexer_pack_token(bk, LEXPANDABLE);
	lexer_push_token(bk, start);
	lexer_read_expandable_recursive(bk, end_marker);
	lexer_push_token(bk, lexer_pack_token(bk, REXPANDABLE));

	return start;
}


struct token *lexer_read_file(struct lexer_book *bk)
{
	int c = lexer_next_peek(bk);

	switch (c) {
	case CHAR_EOF:
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
		/* Discard white-space and try again */
		lexer_discard_white_space(bk);
		return lexer_read_file(bk);
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

accept_t lexer_read_file_list(struct lexer_book *bk)
{
	int count = 0;

	lexer_discard_white_space(bk);

	struct token *t;
	do {
		t = lexer_read_file(bk);
		if(t->type == NEWLINE && count == 0) {
			return NO;
		} else if(t->type != NEWLINE && count == 0) {
			/* Add file list start marker */
			lexer_push_token(bk, lexer_pack_token(bk, FILES));
		}

		lexer_push_token(bk, t);
		count++;
	} while(t->type != NEWLINE);

	if(count > 1)
		return YES;
	else {
		return NO;
	}
}

struct token *lexer_read_command_argument(struct lexer_book *bk)
{
	int c = lexer_next_peek(bk);

	switch (c) {
	case CHAR_EOF:
		/* Found end of file while completing command */
		bk->lexeme_end++;
		bk->eof = 1;

		if(bk->stream)
			return lexer_pack_token(bk, NEWLINE);
		else
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

accept_t lexer_read_command(struct lexer_book *bk)
{
	if(lexer_next_peek(bk) != '\t')
		return NO;

	int count = 0;

	lexer_discard_white_space(bk);

	struct token *t;
	do {
		t = lexer_read_command_argument(bk);
		if(!t)
			break;
		if(t->type == NEWLINE && count == 0) {
			lexer_report_error(bk, "Missing command line.\n");
		} else if(t->type != NEWLINE && count == 0) {
			/* Add command start marker */
			lexer_push_token(bk, lexer_pack_token(bk, COMMAND));
		}
		lexer_push_token(bk, t);
		count++;
	} while(t->type != NEWLINE);

	if(count > 1)
		return YES;
	else {
		return NO;
	}
}

accept_t lexer_read_variable(struct lexer_book * bk, struct token * name)
{
	lexer_discard_white_space(bk);

	if(lexer_next_peek(bk) == '=') {
		lexer_next_char(bk);
		lexer_add_to_lexeme(bk, '=');
	} else {
		int c = lexer_next_char(bk);
		if(lexer_next_peek(bk) != '=')
			return NO;
		lexer_add_to_lexeme(bk, c);
		lexer_next_char(bk);	/* Jump = */
	}

	lexer_push_token(bk, lexer_pack_token(bk, VARIABLE));
	lexer_push_token(bk, name);

	lexer_discard_white_space(bk);

	lexer_read_expandable(bk, '\n');
	lexer_roll_back(bk, 1);	//Recover '\n'

	lexer_discard_white_space(bk);

	if(lexer_next_char(bk) != '\n')
		return NO;

	return YES;
}

accept_t lexer_read_variable_list(struct lexer_book * bk)
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

	return YES;
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

typedef accept_t(*read_syntax) (struct lexer_book * bk, struct token * name);

accept_t lexer_read_syntax_export(struct lexer_book *bk, struct token *name)
{
	lexer_discard_white_space(bk);

	name->type = SYNTAX;
	lexer_push_token(bk, name);

	if(lexer_unquoted_look_ahead_count(bk, "=") > -1)
		return lexer_read_variable(bk, lexer_read_syntax_name(bk));
	else
		return lexer_read_variable_list(bk);



	return YES;
}

accept_t lexer_read_syntax(struct lexer_book * bk)
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

	return NO;
}



accept_t lexer_read_line(struct lexer_book * bk)
{
	char c = lexer_next_peek(bk);

	int colon, equal;

	bk->substitution_mode = ROOT;

	switch (c) {
	case CHAR_EOF:
		/* Found end of file */
		lexer_next_char(bk);
		return YES;
		break;
	case '#':
		lexer_discard_comments(bk);
		return YES;
		break;
	case '\t':
		bk->substitution_mode = COMMAND;
		return lexer_read_command(bk);
		break;
	case ' ':
		/* Eat whitespace and try again */
		lexer_discard_white_space(bk);
		return lexer_read_line(bk);
		break;
	case '\n':
		/* Ignore empty lines and try again */
		lexer_next_char(bk);
		return lexer_read_line(bk);
		break;
	case '@':
		/* Jump @ */
		bk->substitution_mode = SYNTAX;
		lexer_next_char(bk);
		return lexer_read_syntax(bk);
		break;
	default:
		/* Either makeflow keyword (e.g. export), a file list, or variable assignment */
		lexer_discard_white_space(bk);

		colon = lexer_unquoted_look_ahead_count(bk, ":");
		equal = lexer_unquoted_look_ahead_count(bk, "=");

		fprintf(stderr, "%d %d %c\n", colon, equal, c);


		if((colon != -1) && (equal == -1 || colon < equal)) {
			bk->substitution_mode = FILES;
			return lexer_read_file_list(bk);
		} else {
			bk->substitution_mode = SYNTAX;
			return lexer_read_syntax(bk);
		}
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

void lexer_append_all_tokens(struct lexer_book *bk, struct lexer_book *bk_s)
{
	struct token *head_s;

	bk_s->substitution_mode = bk->substitution_mode;
	while(!bk_s->eof) {
		if(lexer_next_peek(bk_s) == CHAR_EOF) {
			/* Found end of string while completing command */
			bk_s->lexeme_end++;
			bk_s->eof = 1;
		} else {
			switch (bk_s->substitution_mode) {
			case CHAR_EOF:
			case COMMAND:
				head_s = lexer_read_command_argument(bk_s);
				break;
			case FILES:
				head_s = lexer_read_file(bk_s);
				break;
			case SYNTAX:
				lexer_read_expandable(bk_s, CHAR_EOF);
				head_s = lexer_pack_token(bk_s, LITERAL);
				break;
			default:
				lexer_read_line(bk_s);
				continue;
				break;
			}

			if(head_s)
				lexer_push_token(bk_s, head_s);
		}
	}

	while((head_s = list_pop_tail(bk_s->token_queue)) != NULL)
		list_push_head(bk->token_queue, head_s);
}

struct token *lexer_next_token(struct lexer_book *bk, struct dag_lookup_set *s)
{
	struct token *head;
	if(list_size(bk->token_queue) == 0) {
		if(bk->eof)
			return NULL;

		lexer_read_line(bk);
		return lexer_next_token(bk, s);
	}

	head = list_pop_head(bk->token_queue);

	if(head->type == SUBSTITUTION) {
		char *substitution = dag_lookup(head->lexeme, s);
		struct lexer_book *bk_s;

		if(!substitution) {
			debug(D_NOTICE, "Variable %s has not yet been defined at line %d.\n", head->lexeme, bk->line_number);
			return lexer_next_token(bk, s);
		}

		bk_s = lexer_init_book(STRING, dag_lookup(head->lexeme, s), bk->line_number, bk->column_number);

		lexer_append_all_tokens(bk, bk_s);

		lexer_free_book(bk_s);
		lexer_free_token(head);

		head = list_pop_head(bk->token_queue);
	}

	return head;
}

int main_test(int argc, char **argv)
{
	FILE *ff = fopen("../example/example.makeflow", "r");

	struct lexer_book *bk = lexer_init_book(STREAM, ff, 1, 1);

	debug_config(argv[0]);
	debug_flags_set("all");


	struct token *t;
	while((t = lexer_next_token(bk, NULL)) != NULL)
		print_token(stderr, t);


	return 0;
}

/* vim: set noexpandtab tabstop=4: */
