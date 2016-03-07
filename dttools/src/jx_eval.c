/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"
#include "debug.h"

#include <string.h>

static struct jx * jx_eval_null( jx_operator_t op )
{
	switch(op) {
		case JX_OP_EQ:
			return jx_boolean(1);
		case JX_OP_NE: 
		case JX_OP_LT:
		case JX_OP_LE:
		case JX_OP_GT:
		case JX_OP_GE:
			return jx_boolean(0);
		default:
			return jx_null();
	}
}

static struct jx * jx_eval_boolean( jx_operator_t op, struct jx *left, struct jx *right )
{
	int a = left ? left->u.boolean_value : 0;
	int b = right ? right->u.boolean_value : 0;

	switch(op) {
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
			return jx_boolean(a|b);
		case JX_OP_MUL:
			return jx_boolean(a&b);
		case JX_OP_AND:
			return jx_boolean(a&&b);
		case JX_OP_OR:
			return jx_boolean(a||b);
		case JX_OP_NOT:
			return jx_boolean(!b);
		default:
			return jx_boolean(0);
	}
}

static struct jx * jx_eval_integer( jx_operator_t op, struct jx *left, struct jx *right )
{
	int a = left ? left->u.integer_value : 0;
	int b = right ? right->u.integer_value : 0;

	switch(op) {
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
			if(b==0) return jx_null();
			return jx_integer(a/b);
		case JX_OP_MOD:
			if(b==0) return jx_null();
			return jx_integer(a%b);
		default:
			return jx_null();
	}
}

static struct jx * jx_eval_double( jx_operator_t op, struct jx *left, struct jx *right )
{
	double a = left ? left->u.double_value : 0;
	double b = right ? right->u.double_value : 0;

	switch(op) {
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
			if(b==0) return jx_null();
			return jx_double(a/b);
		case JX_OP_MOD:
			if(b==0) return jx_null();
			return jx_double((int)a%(int)b);
		default:
			return jx_null();
	}
}

static struct jx * jx_eval_string( jx_operator_t op, struct jx *left, struct jx *right )
{
	const char *a = left->u.string_value;
	const char *b = right->u.string_value;

	switch(op) {
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
			return jx_null();

	}
}

/*
Type conversion rules:
Generally, operators are not meant to be applied to unequal types.
NULL is the result of an operator on two incompatible expressions.
Exception: When x and y are incompatible types, x==y returns FALSE and x!=y returns TRUE.
Exception: integers are promoted to doubles as needed.
*/

static struct jx * jx_eval_operator( struct jx_operator *o, struct jx *context )
{
	if(!o) return 0;
	
	struct jx *left = jx_eval(o->left,context);
	struct jx *right = jx_eval(o->right,context);

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
			return jx_boolean(0);
		} else if(o->type==JX_OP_NE) {
			return jx_boolean(1);
		} else {
			return jx_null();
		}
	}

	switch(right->type) {
		case JX_NULL:
			return jx_eval_null(o->type);
		case JX_BOOLEAN:
			return jx_eval_boolean(o->type,left,right);
		case JX_INTEGER:
			return jx_eval_integer(o->type,left,right);
		case JX_DOUBLE:
			return jx_eval_double(o->type,left,right);
		case JX_STRING:
			return jx_eval_string(o->type,left,right);
		default:
			return jx_null();
	}
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

struct jx * jx_eval( struct jx *j, struct jx *context )
{
	if(!j) return 0;
	
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
			return jx_array(jx_eval_item(j->u.items,context));
		case JX_OBJECT:
			return jx_object(jx_eval_pair(j->u.pairs,context));
		case JX_OPERATOR:
			return jx_eval_operator(&j->u.oper,context);
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
