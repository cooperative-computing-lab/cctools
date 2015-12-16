/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_expr.h"

#include "stringtools.h"

#include <string.h>
#include <stdio.h>

struct deltadb_expr {
	char *operator;
	char *param;
	char *value;
	struct deltadb_expr *next;
};

struct deltadb_expr * deltadb_expr_create( const char *str, struct deltadb_expr *next )
{
	struct deltadb_expr *e = malloc(sizeof(*e));

	int size = strlen(str)+1;
	e->param = malloc(size);
	e->operator = malloc(size);
	e->value = malloc(size);

	int n = sscanf(str,"%[^<>=!]%[<>=!]%s",e->param,e->operator,e->value);
	if(n!=3) {
		deltadb_expr_delete(e);
		return 0;
	}

	return e;
}

void deltadb_expr_delete( struct deltadb_expr *e )
{
	if(!e) return;
	free(e->param);
	free(e->operator);
	free(e->value);
	free(e);
}

static int jx_is_number( struct jx * j )
{
	return j->type==JX_DOUBLE || j->type==JX_INTEGER;
}

static double jx_to_double( struct jx *j )
{
	if(j->type==JX_DOUBLE) return j->u.double_value;
	return j->u.integer_value;
}

static int expr_is_true( struct deltadb_expr *expr, struct jx *jvalue )
{
	char *operator = expr->operator;
	int cmp;
	double dvalue;

	/// XXX need to handle other combinations of values here

	if (string_is_float(expr->value,&dvalue) && jx_is_number(jvalue) ) {
		double in = jx_to_double(jvalue);
		if (in<dvalue) cmp = -1;
		else if (in==dvalue) cmp = 0;
		else cmp = 1;
	} else {
		cmp = strcmp(jvalue->u.string_value,expr->value);
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

