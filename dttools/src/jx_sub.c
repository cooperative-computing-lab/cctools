/*
Copyright (C) 2021- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_sub.h"

#include <assert.h>
#include <string.h>


#include <stdio.h>
#include "jx_pretty_print.h"


static struct jx * jx_sub_call(struct jx *func, struct jx *args, struct jx *ctx) {
	assert(func);
	assert(args);
	assert(jx_istype(args, JX_ARRAY));
	assert(jx_istype(func, JX_SYMBOL));

    struct jx *left  = jx_copy(func);
    struct jx *right = NULL;

	if (!strcmp(func->u.symbol_name, "range")    ||
	    !strcmp(func->u.symbol_name, "format")   ||
	    !strcmp(func->u.symbol_name, "join")     ||
	    !strcmp(func->u.symbol_name, "ceil")     ||
	    !strcmp(func->u.symbol_name, "floor")    ||
	    !strcmp(func->u.symbol_name, "basename") ||
	    !strcmp(func->u.symbol_name, "dirname")  ||
	    !strcmp(func->u.symbol_name, "listdir")  ||
	    !strcmp(func->u.symbol_name, "escape")   ||
	    !strcmp(func->u.symbol_name, "template") ||
	    !strcmp(func->u.symbol_name, "len")      ||
	    !strcmp(func->u.symbol_name, "fetch")    ||
	    !strcmp(func->u.symbol_name, "schema")   ||
	    !strcmp(func->u.symbol_name, "like")     ||
	    !strcmp(func->u.symbol_name, "keys")     ||
	    !strcmp(func->u.symbol_name, "values")   ||
	    !strcmp(func->u.symbol_name, "items")) {
        
        right = jx_sub(args, ctx);

	/* select() and project() differ from the other functions in
	 * that they need deferred eval for specific arguments.
	 * We therefore just give them a copy of the args, and let
	 * them do the eval themselves.
	 *
	 * When adding new functions, you probably don't want to do this
	 */
	} else if (!strcmp(func->u.symbol_name, "select") ||
	           !strcmp(func->u.symbol_name, "project")) {

        // only sub objlist
        printf("getting val\n");
        struct jx *val = jx_array_index(args, 0);
        printf("getting objlist\n");
        struct jx *objlist = jx_array_index(args, 1);

        struct jx *new_val = jx_copy(val);
        printf("subbing objlist\n");
        struct jx *new_objlist = jx_sub(objlist, ctx);

        // add back to array
        struct jx *right = jx_array(0);
        jx_array_append(right, new_val);
        jx_array_append(right, new_objlist);

        printf("new objlist: ");
        jx_pretty_print_stream(right, stdout);

	} else {
        jx_delete(left);
        jx_delete(right);

		return jx_error(jx_format(
			"on line %d, unknown function: %s",
			func->line,
			func->u.symbol_name
		));
	}
    
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

static struct jx_item *jx_sub_comprehension(struct jx *body, struct jx_comprehension *comp, struct jx *context) {
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

    // overwrite var with local copy if need be
    struct jx *ctx = jx_copy(context);
    jx_insert(ctx, jx_string(comp->variable), jx_object(0));

    if (comp->condition) {
        condition = jx_sub(comp->condition, ctx);

        if (jx_istype(condition, JX_ERROR)) {
            jx_delete(ctx);
            jx_delete(elements);
            return jx_item(condition, NULL);
        }
    }

    if (comp->next) {
        struct jx_item *item_with_next = jx_sub_comprehension(body, comp->next, ctx);

        if (!item_with_next) {
            jx_delete(ctx);
            jx_delete(elements);
            jx_delete(condition);
            return NULL;
        }

        next = item_with_next->comp;

        // only use `item_with_next` for next value (hacky)
        free(item_with_next->value);
        free(item_with_next);

    } else {
        // time to sub body
        struct jx *value = jx_sub(body, ctx);

        if (jx_istype(value, JX_ERROR)) {
            jx_delete(ctx);
            jx_delete(elements);
            jx_delete(condition);
            return jx_item(value, NULL);
        }
    }


	jx_delete(ctx);

    struct jx_item *result = jx_item(value, 0);
    result->comp = jx_comprehension(comp->variable, elements, condition, next);

	return result;
}

static struct jx_pair *jx_sub_pair(struct jx_pair *pair, struct jx *context) {
	if (!pair) return 0;

	return jx_pair(
		jx_sub(pair->key, context),
		jx_sub(pair->value, context),
		jx_sub_pair(pair->next, context));
}

static struct jx_item *jx_sub_item(struct jx_item *item, struct jx *context) {
	if (!item) return NULL;

	if (item->comp) {
		return jx_sub_comprehension(item->value, item->comp, context);
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
				result = jx_sub(t, context);
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

/*vim: set noexpandtab tabstop=4: */
