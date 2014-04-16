/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_PARSER_H
#define DELTADB_PARSER_H

#include "deltadb_value.h"
#include "deltadb_expr.h"
#include "deltadb_scanner.h"

#include <stdio.h>

struct deltadb_expr  * deltadb_parse_string_as_expr( const char *s );
struct deltadb_value * deltadb_parse_string_as_value( const char *s );

struct deltadb_parser * deltadb_parser_create( struct deltadb_scanner *s );
struct deltadb_expr *   deltadb_parse_expr( struct deltadb_parser *p );
struct deltadb_value *  deltadb_parse_value( struct deltadb_parser *p );
const char *            deltadb_parser_error_string( struct deltadb_parser *p );
void                    deltadb_parser_delete( struct deltadb_parser *p );

#endif
