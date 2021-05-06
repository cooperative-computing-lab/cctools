/*
Copyright (C) 2021- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_SUB_H
#define JX_SUB_H

#include "jx.h"

/** @file jx_sub.h Implements context substitution of JX expressions.
*/

/** Substitute symbols from context.
Traverses the expression, searching for symbols 
Unbounds symbols by looking for matches in the context object.
@param j The expression whose symbols will be substituted.
@param context An object in which values will be found.
@return A newly created result expression, which must be deleted with @ref jx_delete.
If the expression is invalid in some way, an object of type @ref JX_NULL is returned.
*/
struct jx * jx_sub( struct jx *j, struct jx *context );


#endif
