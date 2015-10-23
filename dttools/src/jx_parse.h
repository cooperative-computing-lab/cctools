/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PARSE_H
#define JX_PARSE_H

/** @file jx_parse.h Parse JSON strings and files into JX expressions.
This module parses arbirary JSON expressions according to the
definition at <a href=http://www.json.org>json.org</a> albeit
with some limitations:
<ol>
<li>
<li>
</ol>
*/

#include "jx.h"
#include <stdio.h>

struct jx_parser;

/** Parse a JSON string to a JX expression.  @param str A C string containing JSON data.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_string( const char *str );

/** Parse a standard IO stream to a JX expression.  @param file A file containing JSON data.  @return A JX expression which must be deleted with @ref jx_delete. If the parse fails or no JSON value is present, null is returned. */
struct jx * jx_parse_file( FILE *file );

/** Create a JX parser object.  @return A parser object. */
struct jx_parser * jx_parser_create();

/** Attach parser to a file.  @param p A parser object.  @param file A standard IO stream. */
void jx_parser_read_file( struct jx_parser *p, FILE *file );

/** Attach parser to a string.  @param p A parser object.  @param str A JSON string to parse. */
void jx_parser_read_string( struct jx_parser *p, const char *str );

/** Parse a JX expression.  @param p A parser created by @ref jx_parser_create.  @return A JX expression, or null on parse failure. */
struct jx * jx_parse( struct jx_parser *p );

/** Return number of parse errors.  @param p A parser object.  @return Number of parse errors encountered. */
int jx_parser_errors( struct jx_parser *p );

/** Delete a parser.  @param p The parser to delete. */
void               jx_parser_delete( struct jx_parser *p );

#endif
