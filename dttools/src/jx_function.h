/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_FUNCTION_H
#define JX_FUNCTION_H

#include "jx.h"

int jx_function_parse_args(struct jx *array, int argc, ...);
struct jx *jx_function_range( struct jx_operator *o, struct jx *context );
struct jx *jx_function_foreach( struct jx_operator *o, struct jx *context );
struct jx *jx_function_str( struct jx_operator *o, struct jx *context );

#endif
