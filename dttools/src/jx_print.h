/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PRINT_H
#define JX_PRINT_H

#include "jx.h"
#include "buffer.h"
#include <stdio.h>

void jx_print_buffer( struct jx *j, buffer_t *buffer);
void jx_print_file( struct jx *j, FILE *file );
char * jx_print_string( struct jx *j );


#endif
