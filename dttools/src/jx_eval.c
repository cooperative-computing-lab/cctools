/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_eval.h"
#include "debug.h"

#include <string.h>

struct jx * jx_eval_boolean( struct jx_operator *o, struct jx *left, struct jx *right )
{
	int a = left->u.boolean_value;
	int b = right->u.boolean_value;

	int r;

	switch(o->type) {
		case JX_OP_EQ:
			r = a==b;
			break;
		case JX_OP_NE:
			r = a!=b;
			break;
		case JX_OP_LT:
			r = a<b;
			break;
		case JX_OP_LE:
			r = a<=b;
			break;
		case JX_OP_GT:
			r = a>b;
			break;
		case JX_OP_GE:
			r = a>=b;
			break;
		case JX_OP_ADD:
			r = a|b;
			break;
		case JX_OP_MUL:
			r = a&b;
			break;
		default:
			r = 0;
			break;
	}

	return jx_boolean(r);
}

struct jx * jx_eval_integer( struct jx_operator *o, struct jx *left, struct jx *right )
{
	int a = left->u.integer_value;
	int b = right->u.integer_value;

	switch(o->type) {
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
			return jx_integer(a+b);
			break;
		case JX_OP_SUB:
			return jx_integer(a-b);
			break;
		case JX_OP_MUL:
			return jx_integer(a*b);
			break;
		case JX_OP_DIV:
			if(b==0) return jx_null();
			return jx_integer(a/b);
		case JX_OP_MOD:
			if(b==0) return jx_null();
			return jx_integer(a%b);
	}

	return jx_null();
}

struct jx * jx_eval_double( struct jx_operator *o, struct jx *left, struct jx *right )
{
	double a = left->u.double_value;
	double b = right->u.double_value;

	switch(o->type) {
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
	}

	return jx_null();
}

struct jx * jx_eval_string( struct jx_operator *o, struct jx *left, struct jx *right )
{
	const char *a = left->u.string_value;
	const char *b = right->u.string_value;

	switch(o->type) {
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
			break;
	}

	return jx_null();
}

static struct jx * jx_eval_operator( struct jx_operator *o, struct jx *context )
{
	struct jx *left = jx_eval(o->left,context);
	struct jx *right = jx_eval(o->right,context);

	/* need to do type conversions here */

	if(left->type!=right->type) {
		fatal("jx type conversions not yet implemented");
	}

	switch(left->type) {
		case JX_BOOLEAN:
			return jx_eval_boolean(o,left,right);
		case JX_INTEGER:
			return jx_eval_integer(o,left,right);
		case JX_DOUBLE:
			return jx_eval_double(o,left,right);
		case JX_STRING:
			return jx_eval_string(o,left,right);
		default:
			return jx_null();
	}
}

static struct jx_pair * jx_eval_pair( struct jx_pair *pair, struct jx *context )
{
	return jx_pair(
		jx_eval(pair->key,context),
		jx_eval(pair->value,context),
		jx_eval_pair(pair->next,context)
	);
}

static struct jx_item * jx_eval_item( struct jx_item *item, struct jx *context )
{
	return jx_item(
		jx_eval(item->value,context),
		jx_eval_item(item->next,context)
	);
}

struct jx * jx_eval( struct jx *j, struct jx *context )
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
			return jx_array(jx_eval_item(j->u.items,context));
		case JX_OBJECT:
			return jx_object(jx_eval_pair(j->u.pairs,context));
		case JX_OPERATOR:
			return jx_eval_operator(&j->u.oper,context);
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
