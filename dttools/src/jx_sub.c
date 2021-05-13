/*
Copyright (C) 2021- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_sub.h"
#include "jx_function.h"

#include <assert.h>
#include <string.h>


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

static struct jx_item *jx_sub_comprehension( struct jx *body, struct jx_comprehension *comp, struct jx *context) {
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
        struct jx_item *item = jx_sub_comprehension(body, comp->next, ctx);

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

    struct jx_item *result = jx_item(value, 0);
    result->comp = jx_comprehension(comp->variable, elements, condition, next);

    return result;
}

static struct jx_pair *jx_sub_pair( struct jx_pair *pair, struct jx *context) {
    if (!pair) return 0;

    return jx_pair(
        jx_sub(pair->key, context),
        jx_sub(pair->value, context),
        jx_sub_pair(pair->next, context));
}

static struct jx_item *jx_sub_item( struct jx_item *item, struct jx *context) {
    if (!item) return NULL;

    struct jx_item *new_item = NULL;

    if (item->comp) {
        new_item = jx_sub_comprehension(item->value, item->comp, context);
        new_item->next = jx_sub_item(item->next, context);
    } else {
        new_item = jx_item(jx_sub(item->value, context), jx_sub_item(item->next, context));
    }

    return new_item;
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

/*vim: set noexpandtab tabstop=4: */
