/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PARSE_H
#define JX_PARSE_H

#include "jx.h"
#include <stdio.h>

struct jx_parser;

struct jx * jx_parse_string( const char *str );
struct jx * jx_parse_file( FILE *file );
struct jx * jx_parse( struct jx_parser *p );

struct jx_parser * jx_parser_create();
void               jx_parser_read_file( struct jx_parser *p, FILE *file );
void               jx_parser_read_string( struct jx_parser *p, const char *str );
int                jx_parser_errors( struct jx_parser *p );
void               jx_parser_delete( struct jx_parser *p );

#endif
