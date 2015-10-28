/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PRINT_H
#define JX_PRINT_H

/** @file jx_print.h Print JX expressions to strings, files, and buffers. */

#include "jx.h"
#include "buffer.h"
#include <stdio.h>

/** Convert a JX expression into a string.  @param j A JX expression. @return A C string representing the expression in JSON form.  The string must be deleted with free(). */

char * jx_print_string( struct jx *j );

/** Print a JX expression to a file.  @param j A JX expression.  @param file A standard IO stream. */

void jx_print_file( struct jx *j, FILE *file );

/** Print a JX expression to a buffer. @param j A JX expression. @param buffer The buffer for output. @see buffer.h */

void jx_print_buffer( struct jx *j, buffer_t *buffer);

/** Print a C string in JSON format (with escape codes) into a buffer.  @param s A C string.  @param buffer The buffer for output.  @see buffer.h */
void jx_escape_string( const char *s, buffer_t *b );

#endif
