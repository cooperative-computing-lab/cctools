/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"
#include "debug.h"
#include "jx_function.h"
#include "jx_print.h"

#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>

// FAILOP(int code, jx_operator *op, struct jx *left, struct jx *right, const char *message)
// left, right, and message are evaluated exactly once
#define FAILOP(code, op, left, right, message) do { \
	struct jx *ebidfgds = jx_object(NULL); \
	jx_insert_integer(ebidfgds, "code", code); \
	jx_insert(ebidfgds, jx_string("operator"), jx_operator(op->type, left, right)); \
	if (op->line) jx_insert_integer(ebidfgds, "line", op->line); \
	jx_insert_string(ebidfgds, "message", message); \
	jx_insert_string(ebidfgds, "name", jx_error_name(code)); \
	jx_insert_string(ebidfgds, "source", "jx_eval"); \
	return jx_error(ebidfgds); \
} while (false)

// FAILARR(struct jx *array, struct jx *index, const char *message)
#define FAILARR(array, index, message) do { \
	struct jx *ekjhgsae = jx_object(NULL); \
	jx_insert_integer(ekjhgsae, "code", 4); \
	jx_insert(ekjhgsae, jx_string("array"), jx_copy(array)); \
	jx_insert(ekjhgsae, jx_string("index"), jx_copy(index)); \
	if (index && index->line) jx_insert_integer(ekjhgsae, "line", index->line); \
	jx_insert_string(ekjhgsae, "message", message); \
	jx_insert_string(ekjhgsae, "name", jx_error_name(4)); \
	jx_insert_string(ekjhgsae, "source", "jx_eval"); \
	return jx_error(ekjhgsae); \
} while (false)

// FAILOBJ(struct jx *obj, struct jx *key, const char *message)
#define FAILOBJ(obj, key, message) do { \
	struct jx *edgibijs = jx_object(NULL); \
	jx_insert_integer(edgibijs, "code", 3); \
	jx_insert(edgibijs, jx_string("object"), jx_copy(obj)); \
	jx_insert(edgibijs, jx_string("key"), jx_copy(key)); \
	if (key && key->line) jx_insert_integer(edgibijs, "line", key->line); \
	jx_insert_string(edgibijs, "message", message); \
	jx_insert_string(edgibijs, "name", jx_error_name(3)); \
	jx_insert_string(edgibijs, "source", "jx_eval"); \
	return jx_error(edgibijs); \
} while (false)

static struct jx *jx_check_errors(struct jx *j);

static struct jx *jx_eval_null(struct jx_operator *op, struct jx *left, struct jx *right) {
	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(1);
		case JX_OP_NE:
			return jx_boolean(0);
		default: FAILOP(1, op, jx_null(), jx_null(), "unsupported operator on null");
	}
}

static struct jx *jx_eval_boolean(struct jx_operator *op, struct jx *left, struct jx *right) {
	int a = left ? left->u.boolean_value : 0;
	int b = right ? right->u.boolean_value : 0;

	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(a==b);
		case JX_OP_NE:
			return jx_boolean(a!=b);
		case JX_OP_AND:
			return jx_boolean(a&&b);
		case JX_OP_OR:
			return jx_boolean(a||b);
		case JX_OP_NOT:
			return jx_boolean(!b);
		default: FAILOP(1, op, jx_copy(left), jx_copy(right), "unsupported operator on boolean");
	}
}

static struct jx *jx_eval_integer(struct jx_operator *op, struct jx *left, struct jx *right) {
	jx_int_t a = left ? left->u.integer_value : 0;
	jx_int_t b = right ? right->u.integer_value : 0;

	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(a==b);
		case JX_OP_NE:
			return jx_boolean(a!=b);
		case JX_OP_LT:
			return jx_boolean(a<b);
		case JX_OP_LE:
			return jx_boolean(a<=b);
		case JX_OP_GT:
			return jx_boolean(a>b);
		case JX_OP_GE:
			return jx_boolean(a>=b);
		case JX_OP_ADD:
			return jx_integer(a+b);
		case JX_OP_SUB:
			return jx_integer(a-b);
		case JX_OP_MUL:
			return jx_integer(a*b);
		case JX_OP_DIV:
			if(b==0) FAILOP(5, op, jx_copy(left), jx_copy(right), "division by zero");
			return jx_integer(a/b);
		case JX_OP_MOD:
			if(b==0) FAILOP(5, op, jx_copy(left), jx_copy(right), "division by zero");
			return jx_integer(a%b);
		default: FAILOP(1, op, jx_copy(left), jx_copy(right), "unsupported operator on integer");
	}
}

static struct jx *jx_eval_double(struct jx_operator *op, struct jx *left, struct jx *right) {
	double a = left ? left->u.double_value : 0;
	double b = right ? right->u.double_value : 0;

	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(a==b);
		case JX_OP_NE:
			return jx_boolean(a!=b);
		case JX_OP_LT:
			return jx_boolean(a<b);
		case JX_OP_LE:
			return jx_boolean(a<=b);
		case JX_OP_GT:
			return jx_boolean(a>b);
		case JX_OP_GE:
			return jx_boolean(a>=b);
		case JX_OP_ADD:
			return jx_double(a+b);
		case JX_OP_SUB:
			return jx_double(a-b);
		case JX_OP_MUL:
			return jx_double(a*b);
		case JX_OP_DIV:
			if(b==0) FAILOP(5, op, jx_copy(left), jx_copy(right), "division by zero");
			return jx_double(a/b);
		case JX_OP_MOD:
			if(b==0) FAILOP(5, op, jx_copy(left), jx_copy(right), "division by zero");
			return jx_double((jx_int_t)a%(jx_int_t)b);
		default: FAILOP(1, op, jx_copy(left), jx_copy(right), "unsupported operator on double");
	}
}

static struct jx *jx_eval_string(struct jx_operator *op, struct jx *left, struct jx *right) {
	const char *a = left ? left->u.string_value : "";
	const char *b = right ? right->u.string_value : "";

	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(0==strcmp(a,b));
		case JX_OP_NE:
			return jx_boolean(0!=strcmp(a,b));
		case JX_OP_LT:
			return jx_boolean(strcmp(a,b)<0);
		case JX_OP_LE:
			return jx_boolean(strcmp(a,b)<=0);
		case JX_OP_GT:
			return jx_boolean(strcmp(a,b)>0);
		case JX_OP_GE:
			return jx_boolean(strcmp(a,b)>=0);
		case JX_OP_ADD:
			return jx_format("%s%s",a,b);
		default: FAILOP(1, op, jx_copy(left), jx_copy(right), "unsupported operator on string");
	}
}

static struct jx *jx_eval_array(struct jx_operator *op, struct jx *left, struct jx *right) {
	assert(op);
	if (!(left && right)) FAILOP(1, op, jx_copy(left), jx_copy(right), "missing arguments to array operator");

	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(jx_equals(left, right));
		case JX_OP_NE:
			return jx_boolean(!jx_equals(left, right));
		case JX_OP_ADD:
			return jx_check_errors(jx_array_concat(jx_copy(left), jx_copy(right), NULL));
		default: FAILOP(1, op, jx_copy(left), jx_copy(right), "unsupported operator on array");
	}
}

static struct jx *jx_eval_call(
	struct jx *func, struct jx *args, struct jx *ctx) {
	assert(func);
	assert(func->type == JX_FUNCTION);
	assert(args);
	assert(args->type == JX_ARRAY);

	switch (func->u.func.builtin) {
		case JX_BUILTIN_RANGE: return jx_function_range(args);
		case JX_BUILTIN_FORMAT: return jx_function_format(args);
		case JX_BUILTIN_JOIN: return jx_function_join(args);
		case JX_BUILTIN_CEIL: return jx_function_ceil(args);
		case JX_BUILTIN_FLOOR: return jx_function_floor(args);
		case JX_BUILTIN_BASENAME: return jx_function_basename(args);
		case JX_BUILTIN_DIRNAME: return jx_function_dirname(args);
		case JX_BUILTIN_ESCAPE: return jx_function_escape(args);
		case JX_BUILTIN_LAMBDA: {
			assert(func->u.func.params);

			ctx = jx_copy(ctx);
			if (!ctx) ctx = jx_object(NULL);
			assert(ctx->type == JX_OBJECT);

			struct jx_item *p = func->u.func.params;
			struct jx_item *a = args->u.items;
			while (p->value) {
				assert(p->value->type == JX_SYMBOL);
				if (a) {
					jx_insert(ctx,
						jx_string(p->value->u
								  .symbol_name),
						jx_copy(a->value));
					a = a->next;
				} else {
					jx_insert(ctx,
						jx_string(p->value->u
								  .symbol_name),
						jx_null());
				}
				p = p->next;
			}

			struct jx *j = jx_eval(func->u.func.body, ctx);
			jx_delete(ctx);
			return j;
		}
	}
	// invalid function, so bail out
	abort();
}

static struct jx *jx_eval_slice(struct jx *array, struct jx *slice) {
	assert(array);
	assert(slice);
	assert(slice->type == JX_OPERATOR);
	assert(slice->u.oper.type == JX_OP_SLICE);
	struct jx *left = slice->u.oper.left;
	struct jx *right = slice->u.oper.right;

	if (array->type != JX_ARRAY) {
		int code = 2;
		struct jx *err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("operator"), jx_operator(JX_OP_LOOKUP, jx_copy(array), jx_copy(slice)));
		if (array->line) jx_insert_integer(err, "line", array->line);
		jx_insert_string(err, "message", "only arrays support slicing");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}
	if (left && left->type != JX_INTEGER) FAILOP(2, (&slice->u.oper), jx_copy(left), jx_copy(right),
		"slice indices must be integers");
	if (right && right->type != JX_INTEGER) FAILOP(2, (&slice->u.oper), jx_copy(left), jx_copy(right),
		"slice indices must be integers");

	struct jx *result = jx_array(NULL);
	int len = jx_array_length(array);

	// this is all SUPER inefficient
	jx_int_t start = left ? left->u.integer_value : 0;
	jx_int_t end = right ? right->u.integer_value : len;
	if (start < 0) start += len;
	if (end < 0) end += len;

	for (jx_int_t i = start; i < end; ++i) {
		struct jx *j = jx_array_index(array, i);
		if (j) jx_array_append(result, jx_copy(j));
	}

	return result;
}

/*
Handle a lookup operator, which has two valid cases:
1 - left is an object, right is a string, return the named item in the object.
2 - left is an array, right is an integer, return the nth item in the array.
*/

static struct jx * jx_eval_lookup( struct jx *left, struct jx *right )
{
	struct jx *err;
	int code;
	if(left->type==JX_OBJECT && right->type==JX_STRING) {
		struct jx *r = jx_lookup(left,right->u.string_value);
		if(r) {
			return jx_copy(r);
		} else {
			FAILOBJ(left, right, "key not found");
		}
	} else if(left->type==JX_ARRAY && right->type==JX_INTEGER) {
		struct jx_item *item = left->u.items;
		int count = right->u.integer_value;

		if (count < 0) {
			count += jx_array_length(left);
			if (count < 0) FAILARR(left, right, "index out of range");
		}

		while (count > 0) {
			if (!item) FAILARR(left, right, "index out of range");
			item = item->next;
			count--;
		}

		if (item) {
			return jx_copy(item->value);
		} else {
			FAILARR(left, right, "index out of range");
		}
	} else {
		code = 1;
		err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("operator"), jx_operator(JX_OP_LOOKUP, jx_copy(left), jx_copy(right)));
		if (right && right->line) jx_insert_integer(err, "line", right->line);
		jx_insert_string(err, "message", "invalid type for lookup");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);

	}
}

/*
Type conversion rules:
Generally, operators are not meant to be applied to unequal types.
NULL is the result of an operator on two incompatible expressions.
Exception: integers are promoted to doubles as needed.
Exception: string+x or x+string for atomic types results in converting x to string and concatenating.
Exception: When x and y are incompatible types, x==y returns FALSE and x!=y returns TRUE.
Exception: The lookup operation can be "object[string]" or "array[integer]"
*/

static struct jx * jx_eval_operator( struct jx_operator *o, struct jx *context )
{
	if(!o) return 0;

	struct jx *left = jx_eval(o->left,context);
	struct jx *right = jx_eval(o->right,context);
	struct jx *result;

	if (jx_istype(left, JX_ERROR)) {
		result = left;
		left = NULL;
		goto DONE;
	}
	if (jx_istype(right, JX_ERROR)) {
		result = right;
		right = NULL;
		goto DONE;
	}

	if (o->type == JX_OP_SLICE) return jx_operator(JX_OP_SLICE, left, right);

	if((left && right) && (left->type!=right->type) ) {
		if( left->type==JX_INTEGER && right->type==JX_DOUBLE) {
			struct jx *n = jx_double(left->u.integer_value);
			jx_delete(left);
			left = n;
		} else if( left->type==JX_DOUBLE && right->type==JX_INTEGER) {
			struct jx *n = jx_double(right->u.integer_value);
			jx_delete(right);
			right = n;
		} else if(o->type==JX_OP_EQ) {
			jx_delete(left);
			jx_delete(right);
			return jx_boolean(0);
		} else if(o->type==JX_OP_NE) {
			jx_delete(left);
			jx_delete(right);
			return jx_boolean(1);
		} else if(o->type==JX_OP_LOOKUP) {
			struct jx *r;
			if (right->type == JX_OPERATOR && right->u.oper.type == JX_OP_SLICE) {
				r = jx_eval_slice(left, right);
			} else {
				r = jx_eval_lookup(left, right);
			}
			jx_delete(left);
			jx_delete(right);
			return r;
		} else if (o->type == JX_OP_CALL) {
			struct jx *r = jx_eval_call(left, right, context);
			jx_delete(left);
			jx_delete(right);
			return r;
		} else if(o->type==JX_OP_ADD && jx_istype(left,JX_STRING) && jx_isatomic(right) ) {

			char *str = jx_print_string(right);
			jx_delete(right);
			right = jx_string(str);
			free(str);
			/* fall through */

		} else if(o->type==JX_OP_ADD && jx_istype(right,JX_STRING) && jx_isatomic(left) ) {
			char *str = jx_print_string(left);
			jx_delete(left);
			left = jx_string(str);
			free(str);
			/* fall through */
 			
		} else {
			FAILOP(2, o, left, right, "mismatched types for operator");
		}
	}

	switch(right->type) {
		case JX_NULL:
			result = jx_eval_null(o, left, right);
			break;
		case JX_BOOLEAN:
			result = jx_eval_boolean(o, left, right);
			break;
		case JX_INTEGER:
			result = jx_eval_integer(o, left, right);
			break;
		case JX_DOUBLE:
			result = jx_eval_double(o, left, right);
			break;
		case JX_STRING:
			result = jx_eval_string(o, left, right);
			break;
		case JX_ARRAY:
			result = jx_eval_array(o, left, right);
			break;
		default: FAILOP(1, o, left, right, "rvalue does not support operators");
	}

DONE:
	jx_delete(left);
	jx_delete(right);

	return result;
}

static struct jx_item *jx_eval_comprehension(struct jx *body, struct jx_comprehension *comp, struct jx *context) {
	assert(body);
	assert(comp);

	struct jx *list = jx_eval(comp->elements, context);
	if (jx_istype(list, JX_ERROR)) return jx_item(list, NULL);
	if (!jx_istype(list, JX_ARRAY)) {
		struct jx *err = jx_object(NULL);
		jx_insert_integer(err, "code", 2);
		jx_insert(err, jx_string("list"), list);
		if (comp->line) jx_insert_integer(err, "line", comp->line);
		jx_insert_string(
			err, "message", "list comprehension takes an array");
		jx_insert_string(err, "name", jx_error_name(2));
		jx_insert_string(err, "source", "jx_eval");
		return jx_item(jx_error(err), NULL);
	}

	struct jx_item *result = NULL;
	struct jx_item *tail = NULL;

	struct jx *j = NULL;
	void *i = NULL;
	while ((j = jx_iterate_array(list, &i))) {
		struct jx *ctx = jx_copy(context);
		jx_insert(ctx, jx_string(comp->variable), jx_copy(j));
		if (comp->condition) {
			struct jx *cond = jx_eval(comp->condition, ctx);
			if (jx_istype(cond, JX_ERROR)) {
				jx_delete(ctx);
				jx_delete(list);
				jx_item_delete(result);
				return jx_item(cond, NULL);
			}
			if (!jx_istype(cond, JX_BOOLEAN)) {
				jx_delete(ctx);
				jx_delete(list);
				jx_item_delete(result);
				struct jx *err = jx_object(NULL);
				jx_insert_integer(err, "code", 2);
				jx_insert(err, jx_string("condition"), cond);
				if (cond->line)
					jx_insert_integer(err, "line", cond->line);
				jx_insert_string(err, "message",
					"list comprehension condition takes a boolean");
				jx_insert_string(err, "name", jx_error_name(2));
				jx_insert_string(err, "source", "jx_eval");
				return jx_item(jx_error(err), NULL);
			}
			int ok = cond->u.boolean_value;
			jx_delete(cond);
			if (!ok) {
				jx_delete(ctx);
				continue;
			}
		}

		if (comp->next) {
			struct jx_item *val = jx_eval_comprehension(body, comp->next, ctx);
			jx_delete(ctx);
			if (result) {
				tail->next = val;
			} else {
				result = tail = val;
			}
			// this is going to go over the list LOTS of times
			// in the various recursive calls
			while (tail && tail->next) tail = tail->next;

		} else {
			struct jx *val = jx_eval(body, ctx);
			jx_delete(ctx);
			if (!val) {
				jx_delete(list);
				jx_item_delete(result);
				return NULL;
			}
			if (result) {
				tail->next = jx_item(val, NULL);
				tail = tail->next;
			} else {
				result = tail = jx_item(val, NULL);
			}
		}
	}

	jx_delete(list);
	return result;
}

static struct jx_pair *jx_eval_pair(struct jx_pair *pair, struct jx *context) {
	if (!pair) return 0;

	return jx_pair(
		jx_eval(pair->key, context),
		jx_eval(pair->value, context),
		jx_eval_pair(pair->next, context));
}

static struct jx_item *jx_eval_item(struct jx_item *item, struct jx *context) {
	if (!item) return NULL;

	if (item->comp) {
		struct jx_item *result = jx_eval_comprehension(item->value, item->comp, context);
		if (result) {
			struct jx_item *i = result;
			while (i->next) i = i->next;
			i->next = jx_eval_item(item->next, context);
			return result;
		} else {
			return jx_eval_item(item->next, context);
		}
	} else {
		return jx_item(jx_eval(item->value, context),
			jx_eval_item(item->next, context));
	}
}

static struct jx *jx_check_errors(struct jx *j)
{
	struct jx *err = NULL;
	switch (j->type) {
		case JX_ARRAY:
			for(struct jx_item *i = j->u.items; i; i = i->next) {
				if(jx_istype(i->value, JX_ERROR)) {
					err = jx_copy(i->value);
					jx_delete(j);
					return err;
				}
			}
			return j;
		case JX_OBJECT:
			for(struct jx_pair *p = j->u.pairs; p; p = p->next) {
				if (jx_istype(p->key, JX_ERROR)) err = jx_copy(p->key);
				if (!err && jx_istype(p->value, JX_ERROR)) err = jx_copy(p->value);
				if (err) {
					jx_delete(j);
					return err;
				}
			}
			return j;
		default:
			return j;
	}
}

static void jx_eval_add_builtin(
	struct jx *ctx, const char *name, jx_builtin_t b) {
	if (!jx_lookup(ctx, name)) {
		jx_insert(
			ctx, jx_string(name), jx_function(name, b, NULL, NULL));
	}
}

struct jx * jx_eval( struct jx *j, struct jx *context )
{
	struct jx *result = NULL;
	if (!j) return NULL;
	if (context) {
		context = jx_copy(context);
	} else {
		context = jx_object(NULL);
	}
	if (!jx_istype(context, JX_OBJECT)) {
		struct jx *err = jx_object(NULL);
		int code = 7;
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("context"), context);
		jx_insert_string(err, "message", "context must be an object");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}
	jx_eval_add_builtin(context, "range", JX_BUILTIN_RANGE);
	jx_eval_add_builtin(context, "format", JX_BUILTIN_FORMAT);
	jx_eval_add_builtin(context, "join", JX_BUILTIN_JOIN);
	jx_eval_add_builtin(context, "ceil", JX_BUILTIN_CEIL);
	jx_eval_add_builtin(context, "floor", JX_BUILTIN_FLOOR);
	jx_eval_add_builtin(context, "basename", JX_BUILTIN_BASENAME);
	jx_eval_add_builtin(context, "dirname", JX_BUILTIN_DIRNAME);
	jx_eval_add_builtin(context, "escape", JX_BUILTIN_ESCAPE);

	switch(j->type) {
		case JX_SYMBOL: {
			struct jx *t = jx_lookup(context, j->u.symbol_name);
			if (t) {
				result = jx_copy(t);
				break;
			} else {
				struct jx *err = jx_object(NULL);
				int code = 0;
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("symbol"), jx_copy(j));
				jx_insert(err, jx_string("context"), context);
				if (j->line)
					jx_insert_integer(err, "line", j->line);
				jx_insert_string(
					err, "message", "undefined symbol");
				jx_insert_string(
					err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
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
			result = jx_check_errors(jx_array(jx_eval_item(j->u.items, context)));
			break;
		case JX_OBJECT:
			result = jx_check_errors(jx_object(jx_eval_pair(j->u.pairs, context)));
			break;
		case JX_OPERATOR:
			result = jx_eval_operator(&j->u.oper, context);
			break;
		case JX_FUNCTION:
			// we should never eval a function body
			abort();
	}

	jx_delete(context);
	return result;
}
