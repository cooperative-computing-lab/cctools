/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_EVAL_H
#define JX_EVAL_H

#include "jx.h"

/** @file jx_eval.h Implements evaluation of JX expressions.
*/

/** Evaluate an expression.
Traverses the expression, evaluates all operators and evalutes
unbound symbols by looking for matches in the context object.
@param j The expression to evaluate.
@param context An object in which values will be found.
@return A newly created result expression, which must be deleted with @ref jx_delete.
If the expression is invalid in some way, an object of type @ref JX_NULL is returned.
*/
struct jx * jx_eval( struct jx *j, struct jx *context );

/** Evaluate an expression with embedded definitions.
Same as @ref jx_eval, except first looks for a "defines"
clause and combines that with the context.  Allows an
expression to have its own bound values, for convenience.
@param j The expression to evaluate, which may contain a "defines" clause.
@param context An object in which values will be found.
@return A newly created result expression, which must be deleted with @ref jx_delete.
If the expression is invalid in some way, an object of type @ref JX_NULL is returned.
*/
struct jx * jx_eval_with_defines( struct jx *j, struct jx* context );

/** Enable external functions.
A small number of JX functions make use of "external" context,
For safety, these functions are not enabled unless the user first
calls jx_eval_enable_external(1).
@param enable If non-zero, enable external functions, otherwise disable.
*/

void jx_eval_enable_external( int enable );

#endif
