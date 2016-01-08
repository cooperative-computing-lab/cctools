/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"

struct jx_pair * jx_pair_evaluate( struct jx_pair *pair, jx_eval_func_t func )
{
	return jx_pair(
		jx_evaluate(pair->key,func),
		jx_evaluate(pair->value,func),
		jx_pair_evaluate(pair->next,func)
	);
}

struct jx_item * jx_item_evaluate( struct jx_item *item, jx_eval_func_t func )
{
	return jx_item(
		jx_evaluate(item->value,func),
		jx_item_evaluate(item->next,func)
	);
}

struct jx * jx_evaluate( struct jx *j, jx_eval_func_t func )
{
	switch(j->type) {
		case JX_SYMBOL:
			return func(j->u.symbol_name);
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return jx_copy(j);
		case JX_ARRAY:
			return jx_array(jx_item_evaluate(j->u.items,func));
		case JX_OBJECT:
			return jx_object(jx_pair_evaluate(j->u.pairs,func));
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
