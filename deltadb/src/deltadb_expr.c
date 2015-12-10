/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"

#include <string.h>
#include <stdio.h>

struct deltadb_expr {
	char operator[32];
	char *param;
	char *val;
	struct deltadb_expr *next;
};

struct deltadb_expr * deltadb_expr_create( const char *str, struct deltadb_expr *next )
{
	struct deltadb_expr *e = malloc(sizeof(*e));
	e->param = strdup(str);
	char *delim = strpbrk(e->param, "<>=!");
	e->val = strpbrk(delim, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
	int operator_size = (int)(e->val-delim);
	strncpy(e->operator,delim,operator_size);
	e->operator[operator_size] = '\0';
	delim[0] = '\0';
	e->next = next;
	return e;
}

static int is_number(char const* p)
{
	char* end;
	strtod(p, &end);
	return !*end;
}

static int jx_is_number( struct jx * j )
{
	return j->type==JX_DOUBLE || j->type==JX_INTEGER;
}

static double jx_to_double( struct jx *j )
{
	if(j->type==JX_DOUBLE) return j->double_value;
	return j->integer_value;
}

static int expr_is_true( struct deltadb_expr *expr, struct jx *jvalue )
{
	char *operator = expr->operator;
	int cmp;

	/// XXX need to handle other combinations of values here

	if (is_number(expr->val) && jx_is_number(jvalue) ) {
		double in = jx_to_double(jvalue);
		double v = atof(expr->val);
		if (in<v) cmp = -1;
		else if (in==v) cmp = 0;
		else cmp = 1;
	} else {
		cmp = strcmp(jvalue->string_value,expr->val);
	}

	if(strcmp(operator,"=")==0) {
		if(cmp==0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"!=")==0) {
		if(cmp!=0)
			return 1;
		else return 0;
	} else if(strcmp(operator,">")==0) {
		if(cmp>0)
			return 1;
		else return 0;
	} else if(strcmp(operator,">=")==0) {
		if(cmp>=0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"<")==0) {
		if(cmp<0)
			return 1;
		else return 0;
	} else if(strcmp(operator,"<=")==0) {
		if(cmp<=0)
			return 1;
		else return 0;
	}
	return 0;
}

int deltadb_expr_matches( struct deltadb_expr *expr, struct jx *jobject )
{
	while(expr) {
		struct jx *jvalue = jx_lookup(jobject,expr->param);
		if(!jvalue) return 0;

		if(!expr_is_true(expr,jvalue)) return 0;

		expr = expr->next;
	}

	return 1;
}

