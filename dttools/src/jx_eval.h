/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_EVAL_H
#define JX_EVAL_H

#include "jx.h"

/** Evaluate an expression.
Traverses the expression, evaluates all operators and evalutes
unbound symbols by looking for matches in the context object.
@param j The expression to evaluate.
@param context An object in which values will be found.
@return A newly created result expression, which must be deleted with @ref jx_delete.
If the expression is invalid in some way, an object of type @ref JX_NULL is returned.
*/
struct jx * jx_eval( struct jx *j, struct jx *context );

typedef enum {
	JX_EVAL_MODE_ERROR,
	JX_EVAL_MODE_ENV,
	JX_EVAL_MODE_PARTIAL,
	JX_EVAL_MODE_DEFAULT
} jx_eval_mode_t;

struct jx * jx_eval_1( struct jx *j, struct jx *context, jx_eval_mode_t mode, struct jx *default_value );

#endif
