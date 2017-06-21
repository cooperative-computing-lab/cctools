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
		case JX_OP_OR:
			return jx_integer(a|b);
		case JX_OP_AND:
			return jx_integer(a&b);
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
			break;
		case JX_OP_NE:
			return jx_boolean(a!=b);
			break;
		case JX_OP_LT:
			return jx_boolean(a<b);
			break;
		case JX_OP_LE:
			return jx_boolean(a<=b);
			break;
		case JX_OP_GT:
			return jx_boolean(a>b);
			break;
		case JX_OP_GE:
			return jx_boolean(a>=b);
			break;
		case JX_OP_ADD:
			return jx_double(a+b);
			break;
		case JX_OP_SUB:
			return jx_double(a-b);
			break;
		case JX_OP_MUL:
			return jx_double(a*b);
			break;
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

		if(count<0) FAILARR(left, right, "index must be positive");

		while(count>0) {
			if(!item) FAILARR(left, right, "index out of range");
			item = item->next;
			count--;
		}

		if(item) {
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
Exception: When x and y are incompatible types, x==y returns FALSE and x!=y returns TRUE.
Exception: integers are promoted to doubles as needed.
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
			struct jx *r = jx_eval_lookup(left,right);
			jx_delete(left);
			jx_delete(right);
			return r;
		} else if (o->type == JX_OP_CALL) {
			struct jx *r = jx_eval_call(left, right, context);
			jx_delete(left);
			jx_delete(right);
			return r;
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

static struct jx_pair * jx_eval_pair( struct jx_pair *pair, struct jx *context )
{
	if(!pair) return 0;

	return jx_pair(
		jx_eval(pair->key,context),
		jx_eval(pair->value,context),
		jx_eval_pair(pair->next,context)
	);
}

static struct jx_item * jx_eval_item( struct jx_item *item, struct jx *context )
{
	if(!item) return 0;

	return jx_item(
		jx_eval(item->value,context),
		jx_eval_item(item->next,context)
	);
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
				if(jx_istype(p->key, JX_ERROR)) err = jx_copy(p->key);
				if(jx_istype(p->value, JX_ERROR)) err = jx_copy(p->value);
				if(err) {
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
	if (!j) return NULL;
	if (!context) context = jx_object(NULL);
	if (!jx_istype(context, JX_OBJECT)) {
		struct jx *err = jx_object(NULL);
		int code = 7;
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("context"), jx_copy(context));
		jx_insert_string(err, "message", "context must be an object");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}
	jx_eval_add_builtin(context, "range", JX_BUILTIN_RANGE);
	jx_eval_add_builtin(context, "format", JX_BUILTIN_FORMAT);

	switch(j->type) {
		case JX_SYMBOL: {
			struct jx *result =
				jx_lookup(context, j->u.symbol_name);
			if (result) {
				return jx_copy(result);
			} else {
				struct jx *err = jx_object(NULL);
				int code = 0;
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("symbol"), jx_copy(j));
				jx_insert(err, jx_string("context"),
					jx_copy(context));
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
			return jx_copy(j);
		case JX_ARRAY:
			return jx_check_errors(jx_array(jx_eval_item(j->u.items,context)));
		case JX_OBJECT:
			return jx_check_errors(jx_object(jx_eval_pair(j->u.pairs,context)));
		case JX_OPERATOR:
			return jx_eval_operator(&j->u.oper,context);
		case JX_FUNCTION:
			// we should never eval a function body
			abort();
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
