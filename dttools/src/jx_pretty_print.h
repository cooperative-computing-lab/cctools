/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PPRINT_H
#define JX_PPRINT_H

/** @file jx_print.h Print JX expressions to strings, files, and buffers. */

#include "jx.h"
#include "buffer.h"
#include "link.h"
#include <stdio.h>


/** Print a JX expression to a standard I/O stream.  @param j A JX expression.  @param file A standard IO stream. */

void jx_pretty_print_stream( struct jx *j, FILE *file );

#endif
