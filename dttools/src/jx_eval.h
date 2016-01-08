/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_EVAL_H
#define JX_EVAL_H

#include "jx.h"

/** Evaluation function.  To use @ref jx_evaluate, the caller must
define a function of type @ref jx_eval_func_t which accepts a symbol
name and returns a JX value.
*/
typedef struct jx * (*jx_eval_func_t) ( const char *ident );

/** Evaluate an expression.  Traverses the expression recursively, and
for each value of type @ref JX_SYMBOL, invokes the evaluator function
to replace it with a constant value.  @param j The expression to evaluate.  @param evaluator The evaluating function.  @return A newly created result expression, which must be deleted with @ref jx_delete.
*/
struct jx * jx_evaluate( struct jx *j, jx_eval_func_t evaluator );

#endif
