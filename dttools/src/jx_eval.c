/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"
#include "jx_print.h"
#include "jx_function.h"
#include "debug.h"

#include <assert.h>
#include <string.h>

static struct jx *jx_check_errors(struct jx *j);

static struct jx *jx_eval_null(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;

	assert(op);
	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(1);
		case JX_OP_NE:
			return jx_boolean(0);
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_null(), jx_null()));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on null");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
}

static struct jx *jx_eval_boolean(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;
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
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on boolean");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
}

static struct jx *jx_eval_integer(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;
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
			if(b==0) {
				code = 5;
				err = jx_object(NULL);
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
				if (op->line) jx_insert_integer(err, "line", op->line);
				jx_insert_string(err, "message", "division by zero");
				jx_insert_string(err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
			}
			return jx_integer(a/b);
		case JX_OP_MOD:
			if(b==0) {
				code = 5;
				err = jx_object(NULL);
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
				if (op->line) jx_insert_integer(err, "line", op->line);
				jx_insert_string(err, "message", "division by zero");
				jx_insert_string(err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
			}
			return jx_integer(a%b);
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on integer");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
}

static struct jx *jx_eval_double(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;
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
			if(b==0) {
				code = 5;
				err = jx_object(NULL);
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
				if (op->line) jx_insert_integer(err, "line", op->line);
				jx_insert_string(err, "message", "division by zero");
				jx_insert_string(err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
			}
			return jx_double(a/b);
		case JX_OP_MOD:
			if(b==0) {
				code = 5;
				err = jx_object(NULL);
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
				if (op->line) jx_insert_integer(err, "line", op->line);
				jx_insert_string(err, "message", "division by zero");
				jx_insert_string(err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
			}
			return jx_double((jx_int_t)a%(jx_int_t)b);
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on double");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
}

static struct jx *jx_eval_string(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;
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
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on string");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
}

static struct jx *jx_eval_array(struct jx_operator *op, struct jx *left, struct jx *right) {
	struct jx *err;
	int code;
	assert(op);
	if (!(left && right)) {
		err = jx_object(NULL);
		code = 1;
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
		if (op->line) jx_insert_integer(err, "line", op->line);
		jx_insert_string(err, "message", "missing arguments to array operator");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}

	switch(op->type) {
		case JX_OP_EQ:
			return jx_boolean(jx_equals(left, right));
		case JX_OP_NE:
			return jx_boolean(!jx_equals(left, right));
		case JX_OP_ADD:
			return jx_check_errors(jx_array_concat(jx_copy(left), jx_copy(right), NULL));
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(op->type, jx_copy(left), jx_copy(right)));
			if (op->line) jx_insert_integer(err, "line", op->line);
			jx_insert_string(err, "message", "unsupported operator on array");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
	}
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
			code = 3;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("object"), jx_copy(left));
			jx_insert(err, jx_string("key"), jx_copy(right));
			if (right && right->line) jx_insert_integer(err, "line", right->line);
			jx_insert_string(err, "message", "key not found");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
		}
	} else if(left->type==JX_ARRAY && right->type==JX_INTEGER) {
		struct jx_item *item = left->u.items;
		int count = right->u.integer_value;

		if(count<0) {
			code = 4;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("array"), jx_copy(left));
			jx_insert(err, jx_string("index"), jx_copy(right));
			if (right && right->line) jx_insert_integer(err, "line", right->line);
			jx_insert_string(err, "message", "index must be positive");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
		}

		while(count>0) {
			if(!item) {
				code = 4;
				err = jx_object(NULL);
				jx_insert_integer(err, "code", code);
				jx_insert(err, jx_string("array"), jx_copy(left));
				jx_insert(err, jx_string("index"), jx_copy(right));
				if (right && right->line) jx_insert_integer(err, "line", right->line);
				jx_insert_string(err, "message", "index out of range");
				jx_insert_string(err, "name", jx_error_name(code));
				jx_insert_string(err, "source", "jx_eval");
				return jx_error(err);
			}
			item = item->next;
			count--;
		}

		if(item) {
			return jx_copy(item->value);
		} else {
			code = 4;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("array"), jx_copy(left));
			jx_insert(err, jx_string("index"), jx_copy(right));
			if (right && right->line) jx_insert_integer(err, "line", right->line);
			jx_insert_string(err, "message", "index out of range");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
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

static struct jx *jx_eval_function( struct jx_function *f, struct jx *context )
{
	if(!f) return NULL;
	switch(f->function) {
		case JX_FUNCTION_DBG:
			return jx_function_dbg(f, context);
		case JX_FUNCTION_RANGE:
			return jx_function_range(f, context);
		case JX_FUNCTION_FOREACH:
			return jx_function_foreach(f, context);
		case JX_FUNCTION_STR:
			return jx_function_str(f, context);
		case JX_FUNCTION_JOIN:
			return jx_function_join(f, context);
		case JX_FUNCTION_LET:
			return jx_function_let(f, context);
		case JX_FUNCTION_INVALID:
			return NULL;
	}
	return NULL;
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
	struct jx *err;
	struct jx *result;
	int code;

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
		} else {
			code = 2;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(o->type, left, right));
			if (o->line) jx_insert_integer(err, "line", o->line);
			jx_insert_string(err, "message", "mismatched types for operator");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			return jx_error(err);
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
		default:
			code = 1;
			err = jx_object(NULL);
			jx_insert_integer(err, "code", code);
			jx_insert(err, jx_string("operator"), jx_operator(o->type, jx_copy(left), jx_copy(right)));
			if (o->line) jx_insert_integer(err, "line", o->line);
			jx_insert_string(err, "message", "rvalue does not support operators");
			jx_insert_string(err, "name", jx_error_name(code));
			jx_insert_string(err, "source", "jx_eval");
			result = jx_error(err);
			break;
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

struct jx * jx_eval( struct jx *j, struct jx *context )
{
	if(!j) return 0;

	if (context && !jx_istype(context, JX_OBJECT)) {
		struct jx *err = jx_object(NULL);
		int code = 7;
		jx_insert_integer(err, "code", code);
		jx_insert(err, jx_string("context"), jx_copy(context));
		jx_insert_string(err, "message", "context must be an object");
		jx_insert_string(err, "name", jx_error_name(code));
		jx_insert_string(err, "source", "jx_eval");
		return jx_error(err);
	}

	switch(j->type) {
		case JX_SYMBOL:
			if(context) {
				struct jx *result = jx_lookup(context,j->u.symbol_name);
				if(result) {
					return jx_copy(result);
				} else {
					struct jx *err = jx_object(NULL);
					int code = 0;
					jx_insert_integer(err, "code", code);
					jx_insert(err, jx_string("symbol"), jx_copy(j));
					jx_insert(err, jx_string("context"), jx_copy(context));
					if (j->line) jx_insert_integer(err, "line", j->line);
					jx_insert_string(err, "message", "undefined symbol");
					jx_insert_string(err, "name", jx_error_name(code));
					jx_insert_string(err, "source", "jx_eval");
					return jx_error(err);
				}
			}
			return jx_null();
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
			return jx_eval_function(&j->u.func,context);
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
