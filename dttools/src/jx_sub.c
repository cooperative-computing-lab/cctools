/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_sub.h"
#include "jx_function.h"

#include <assert.h>
#include <string.h>
#include <stdlib.h>


static struct jx * jx_sub_call( struct jx *func, struct jx *args, struct jx *ctx) {
	assert(func);
	assert(args);
	assert(jx_istype(args, JX_ARRAY));
	assert(jx_istype(func, JX_SYMBOL));

	struct jx *left  = jx_copy(func);
	struct jx *right = jx_function_sub(func->u.symbol_name, args, ctx);

	return jx_operator(JX_OP_CALL, left, right);
}

static struct jx * jx_sub_operator( struct jx_operator *o, struct jx *context )
{
	if(!o) return 0;

	struct jx *left  = NULL;
	struct jx *right = NULL;

	if (o->type == JX_OP_CALL) return jx_sub_call(o->left, o->right, context);

	left = jx_sub(o->left, context);
	if (jx_istype(left, JX_ERROR)) {
		return left;
	}

	right = jx_sub(o->right, context);
	if (jx_istype(right, JX_ERROR)) {
		jx_delete(left);
		return right;
	}

	return jx_operator(o->type, left, right);
}

static struct jx_item *jx_sub_list_comprehension( struct jx *body, struct jx_comprehension *comp, struct jx *context) {
	assert(body);
	assert(comp);

	// for item
	struct jx *value = NULL;

	// for comp
	struct jx *elements = NULL;
	struct jx *condition = NULL;
	struct jx_comprehension *next = NULL;

	elements = jx_sub(comp->elements, context);

	if (jx_istype(elements, JX_ERROR)) {
		return jx_item(elements, NULL);
	}

	// use local var without affecting context, this way we avoid collisions with pairs already in context
	// set variable to null value (base case), so jx_sub() knows to not fill in symbols
	struct jx *ctx = jx_copy(context);
	jx_insert(ctx, jx_string(comp->variable), jx_null());

	if (comp->condition) {
		condition = jx_sub(comp->condition, ctx);

		if (jx_istype(condition, JX_ERROR)) {
			jx_delete(ctx);
			jx_delete(elements);
			return jx_item(condition, NULL);
		}
	}

	if (comp->next) {
		struct jx_item *item = jx_sub_list_comprehension(body, comp->next, ctx);

		if (!item) {
			jx_delete(ctx);
			jx_delete(elements);
			jx_delete(condition);
			return NULL;
		}

		next = item->comp;
		value = item->value;

		free(item);

	} else {
		// final comp -> do sub on body
		value = jx_sub(body, ctx);

		if (jx_istype(value, JX_ERROR)) {
			jx_delete(ctx);
			jx_delete(elements);
			jx_delete(condition);
			return jx_item(value, NULL);
		}
	}

	jx_delete(ctx);

	struct jx_item *result = jx_item(value, NULL);
	result->comp = jx_comprehension(comp->variable, elements, condition, next);

	return result;
}

static struct jx_pair *jx_sub_dict_comprehension( struct jx *key, struct jx *value, struct jx_comprehension *comp, struct jx *context) {
	assert(key);
	assert(value);
	assert(comp);

	// for item
	struct jx *new_key = NULL;
	struct jx *new_value = NULL;

	// for comp
	struct jx *elements = NULL;
	struct jx *condition = NULL;
	struct jx_comprehension *next = NULL;

	elements = jx_sub(comp->elements, context);

	if (jx_istype(elements, JX_ERROR)) {
		return jx_pair(elements, NULL, NULL);
	}

	// use local var without affecting context, this way we avoid collisions with pairs already in context
	// set variable to null value (base case), so jx_sub() knows to not fill in symbols
	struct jx *ctx = jx_copy(context);
	jx_insert(ctx, jx_string(comp->variable), jx_null());

	if (comp->condition) {
		condition = jx_sub(comp->condition, ctx);

		if (jx_istype(condition, JX_ERROR)) {
			jx_delete(ctx);
			jx_delete(elements);
			return jx_pair(condition, NULL, NULL);
		}
	}

	if (comp->next) {
		struct jx_pair *pair = jx_sub_dict_comprehension(key, value, comp->next, ctx);

		if (!pair) {
			jx_delete(ctx);
			jx_delete(elements);
			jx_delete(condition);
			return NULL;
		}

		next = pair->comp;
		new_key = pair->key;
		new_value = pair->value;

		free(pair);

	} else {
		// final comp -> do sub on body
		new_key = jx_sub(key, ctx);
		new_value = jx_sub(value, ctx);

		if (jx_istype(new_key, JX_ERROR) || jx_istype(new_value, JX_ERROR)) {
			jx_delete(ctx);
			jx_delete(elements);
			jx_delete(condition);
			jx_delete(new_key);
			jx_delete(new_value);
			return jx_pair(jx_error(jx_format(
				"on line %d, invalid pair in dict comprehension", key->line)
			), NULL, NULL);
		}
	}

	jx_delete(ctx);

	struct jx_pair *result = jx_pair(new_key, new_value, NULL);
	result->comp = jx_comprehension(comp->variable, elements, condition, next);

	return result;
}

static struct jx_pair *jx_sub_pair( struct jx_pair *pair, struct jx *context) {
	if (!pair) return 0;

	if (pair->comp) {
		struct jx_pair *result = jx_sub_dict_comprehension(pair->key, pair->value, pair->comp, context);
		if (result) {
			struct jx_pair *p = result;
			while (p->next) p = p->next;
			p->next = jx_sub_pair(pair->next, context);
			return result;
		} else {
			return jx_sub_pair(pair->next, context);
		}
	} else {
		return jx_pair(
			jx_sub(pair->key, context),
			jx_sub(pair->value, context),
			jx_sub_pair(pair->next, context));
	}
}

static struct jx_item *jx_sub_item( struct jx_item *item, struct jx *context) {
	if (!item) return NULL;

	if (item->comp) {
		struct jx_item *result = jx_sub_list_comprehension(item->value, item->comp, context);
		if (result) {
			struct jx_item *i = result;
			while (i->next) i = i->next;
			i->next = jx_sub_item(item->next, context);
			return result;
		} else {
			return jx_sub_item(item->next, context);
		}
	} else {
		return jx_item(jx_sub(item->value, context), jx_sub_item(item->next, context));
	}
}

struct jx * jx_sub( struct jx *j, struct jx *context )
{
	struct jx *result = NULL;
	if (!j) return NULL;

	if (context && !jx_istype(context, JX_OBJECT)) {
		return jx_error(jx_string("context must be an object"));
	}

	switch(j->type) {
		case JX_SYMBOL: {
			struct jx *t = jx_lookup(context, j->u.symbol_name);
			if (t) {
				if (jx_istype(t, JX_NULL)) {
					// for local variables in list comprehensions (don't overwrite)
					result = jx_copy(j);
				} else {
					result = jx_sub(t, context);
				}
				break;
			} else {
				return jx_error(jx_format(
					"on line %d, %s: undefined symbol",
					j->line,
					j->u.symbol_name
				));
			}
		}
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_ERROR:
		case JX_NULL:
			result = jx_copy(j);
			break;
		case JX_ARRAY:
			result = jx_array(jx_sub_item(j->u.items, context));
			break;
		case JX_OBJECT:
			result = jx_object(jx_sub_pair(j->u.pairs, context));
			break;
		case JX_OPERATOR:
			result = jx_sub_operator(&j->u.oper, context);
			break;
	}

	return result;
}

/*vim: set noexpandtab tabstop=8: */
