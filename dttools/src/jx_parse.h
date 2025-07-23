/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PARSE_H
#define JX_PARSE_H

/** @file jx_parse.h Parse JSON strings and files into JX expressions.
This module parses arbirary JSON expressions according to the
definition at <a href=http://www.json.org>json.org</a>,
with the following exceptions:
<ol>
<li> Atomic values are limited to 4KB in size.
<li> Bare identifiers are permitted, to enable expression evaluation.
</ol>
*/

#include "jx.h"
#include "link.h"

#include <stdbool.h>
#include <stdio.h>

struct jx_parser;

/* Sets module-wide flag for static parse mode */
void jx_parse_set_static_mode( bool mode );

/** Parse a JSON string to a JX expression.  @param str A null-terminated C string containing JSON data.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_string( const char *str );

/** Parse a JSON string to a JX expression.  @param str An unterminated string containing JSON data.  @param length of the string in bytes.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_string_and_length( const char *str, int length );

/** Parse a standard IO stream to a JX expression.  @param file A stream containing JSON data.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_stream( FILE *file );

/** Parse a file to a JX expression.  @param name The name of a file containing JSON data.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_file( const char *name );

/** Parse a network link to a JX expression. @param l A link pointer (opaque struct).  @param stoptime The absolute time at which to stop.   @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_link( struct link *l, time_t stoptime );

/** Parse a jx argument file from a commandline option.
 * The passed-in object is consumed.
 * @param jx_args A JX object to add args to.
 * @param args_file Name of the jx_args file that will be read in.
 * @returns An augmented JX object.
 * @returns NULL on failure.
 */
struct jx *jx_parse_cmd_args(struct jx *jx_args, char *args_file);

/** Parse a jx define statement from a commandline option.
 * @param jx_args A JX object to add args to.
 * @param define_stmt Command line value of from VAR=EXPR.
 * @returns 1 on success.
 * @returns 0 on failure.
 */
int jx_parse_cmd_define(struct jx *jx_args, char *define_stmt);

/** Create a JX parser object.  @return A parser object. */
struct jx_parser *jx_parser_create(bool strict_mode);

/** Attach parser to a file.  @param p A parser object.  @param file A standard IO stream. */
void jx_parser_read_stream( struct jx_parser *p, FILE *file );

/** Attach parser to a null-terminate string.  @param p A parser object.  @param str A JSON string to parse. */
void jx_parser_read_string( struct jx_parser *p, const char *str );

/** Attach parser to a raw string with known length.  @param p A parser object.  @param str A JSON string to parse. @param length The length of the string in bytes. */
void jx_parser_read_string_and_length( struct jx_parser *p, const char *str, int length );

/** Attach parser to a link.  @param p A parser object.  @param l A link object.  @param stoptime The absolute time at which to stop. */
void jx_parser_read_link( struct jx_parser *p, struct link *l, time_t stoptime );

/** Parse and return a single value. This function is useful for streaming multiple independent values from a single source. @param p A parser object @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parser_yield( struct jx_parser *p );

/** Parse a JX expression. Note that in the event of a parse error, this function can return a partial result, reflecting the text that was parseable. You must call @ref jx_parser_errors to determine if the parse was successul.  @param p A parser created by @ref jx_parser_create.  @return A JX expression, or null if nothing was parsed. */
struct jx * jx_parse( struct jx_parser *p );

/** Return number of parse errors.  @param p A parser object.  @return Number of parse errors encountered. */
int jx_parser_errors( struct jx_parser *p );

/** Return text of first parse error encountered. @param p A parser object. @return Error string, if available, null otherwise. */
const char *jx_parser_error_string( struct jx_parser *p );

/** Delete a parser.  @param p The parser to delete. */
void jx_parser_delete( struct jx_parser *p );

/* Private function used by jx_print to put parens in the right place. */
int jx_operator_precedence( jx_operator_t op );


#endif
