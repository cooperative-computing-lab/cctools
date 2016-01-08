/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"

static struct jx_pair * jx_pair_evaluate( struct jx_pair *pair, struct jx *context )
{
	return jx_pair(
		jx_evaluate(pair->key,context),
		jx_evaluate(pair->value,context),
		jx_pair_evaluate(pair->next,context)
	);
}

static struct jx_item * jx_item_evaluate( struct jx_item *item, struct jx *context )
{
	return jx_item(
		jx_evaluate(item->value,context),
		jx_item_evaluate(item->next,context)
	);
}

struct jx * jx_evaluate( struct jx *j, struct jx *context )
{
	switch(j->type) {
		case JX_SYMBOL:
			return jx_lookup(context,j->u.symbol_name);
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return jx_copy(j);
		case JX_ARRAY:
			return jx_array(jx_item_evaluate(j->u.items,context));
		case JX_OBJECT:
			return jx_object(jx_pair_evaluate(j->u.pairs,context));
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
