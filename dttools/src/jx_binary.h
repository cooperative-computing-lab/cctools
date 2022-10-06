/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_BINARY_H
#define JX_BINARY_H

/** @file jx_binary.h Binary format encode/decode for JX expressions.
These routines allow for JX expressions to be read/written from disk
in a custom binary format that is more efficient than traditional
parsing of ASCII data.  It does not conform to an external standard,
and so should only be used for efficient internal storage.
**/

#include <stdio.h>
#include "jx.h"

/** Write a JX expression to a file in binary form.
@param stream The stdio stream to write to.
@param j The expression to write.
@return True on success, false on failure.
*/

int jx_binary_write( FILE *stream, struct jx *j );

/** Read a JX expression from a file in binary form.
@param stream The stdio stream to read from.
@return A JX expression, or null on failure.
*/

struct jx * jx_binary_read( FILE *stream );

#endif
