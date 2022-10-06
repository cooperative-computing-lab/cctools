/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_reduction.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "stringtools.h"
#include "jx_print.h"

struct deltadb_reduction *deltadb_reduction_create_type( deltadb_reduction_t type, struct jx *expr, deltadb_scope_t scope )
{
	struct deltadb_reduction *r;
	r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->type = type;
	r->scope = scope;
	r->expr = expr;
	r->temporal_table = hash_table_create(0,0);
	r->unique_table = hash_table_create(0,0);
	r->unique_value = jx_array(0);
	return r;
};

struct deltadb_reduction *deltadb_reduction_create( const char *name, struct jx *expr, deltadb_scope_t scope )
{
	deltadb_reduction_t type;

	if (strcmp(name,"COUNT")==0)		type = COUNT;
	else if (strcmp(name,"SUM")==0)		type = SUM;
	else if (strcmp(name,"FIRST")==0)	type = FIRST;
	else if (strcmp(name,"LAST")==0)	type = LAST;
	else if (strcmp(name,"MIN")==0)		type = MIN;
	else if (strcmp(name,"AVERAGE")==0)	type = AVERAGE;
	else if (strcmp(name,"MAX")==0)		type = MAX;
	else if (strcmp(name,"INC")==0)		type = INC;
	else if (strcmp(name,"UNIQUE")==0)      type = UNIQUE;
	else	return 0;

	return deltadb_reduction_create_type(type, expr, scope);
}

void deltadb_reduction_delete_temporal_table(struct hash_table *temporal_table) {
	if (!temporal_table) return;

	char *key;
	void *value;
	struct deltadb_reduction *r;

	hash_table_firstkey(temporal_table);
	while(hash_table_nextkey(temporal_table,&key,&value)) {
		r = (struct deltadb_reduction *) value;
		deltadb_reduction_delete(r);
	}

	hash_table_delete(temporal_table);
}

void deltadb_reduction_delete( struct deltadb_reduction *r )
{
	if(!r) return;

	deltadb_reduction_delete_temporal_table(r->temporal_table);
	jx_delete(r->unique_value);
	hash_table_delete(r->unique_table);
	jx_delete(r->expr);
	free(r);
}

void deltadb_reduction_reset( struct deltadb_reduction *r, deltadb_scope_t scope )
{
	if(r->scope!=scope) return;

	r->count = r->sum = r->first = r->last = r->min = r->max = 0;
	deltadb_reduction_delete_temporal_table(r->temporal_table);
	r->temporal_table = hash_table_create(0,0);
	jx_delete(r->unique_value);
	hash_table_delete(r->unique_table);
	r->unique_table = hash_table_create(0,0);
	r->unique_value = jx_array(0);
}

void deltadb_reduction_update( struct deltadb_reduction *r, const char *key, struct jx * value, deltadb_scope_t scope )
{
	if(r->scope!=scope) return;

	if (r->scope == DELTADB_SCOPE_TEMPORAL) {
		struct deltadb_reduction *base = r;
		r = hash_table_lookup(base->temporal_table, key);
		if (!r) {
			r = deltadb_reduction_create_type(base->type,jx_copy(base->expr),base->scope);
			hash_table_insert(base->temporal_table, key, r);
		}
	}

	/* UNIQUE: keep a value in a hash table, keyed by the string representation. */

	if(r->type==UNIQUE) {
		char *str = jx_print_string(value);
		if(!hash_table_lookup(r->unique_table,str)) {
			struct jx *value_copy = jx_copy(value);
			hash_table_insert(r->unique_table,str,value_copy);
			jx_array_append(r->unique_value,value_copy);
		}
		free(str);
		return;
	}

	/* Any other type: convert to a double and track the extrema. */

	double val = 0;

	if(value->type==JX_INTEGER) {
		val = value->u.integer_value;
	} else if(value->type==JX_DOUBLE) {
		val = value->u.double_value;
	} else {
		// treat non-numerics as 1, to facilitate operations like COUNT
		val = 1;
	}

	if(r->count==0) {
		r->min = r->max = r->first = val;
	} else {
		if (val < r->min) r->min = val;
		if (val > r->max) r->max = val;
	}

	r->sum += val;
	r->last = val;
	r->count++;
};

char * deltadb_reduction_string( struct deltadb_reduction *r )
{
	double value = 0;
	switch(r->type) {
		case UNIQUE:
			return jx_print_string(r->unique_value);
		case COUNT:
			value = r->count;
			break;
		case SUM:
			value = r->sum;
			break;
		case FIRST:
			value = r->first;
			break;
		case LAST:
			value = r->last;
			break;
		case MIN:
			value = r->min;
			break;
		case AVERAGE:
			value = r->count>0 ? r->sum/r->count : 0;
			break;
		case MAX:
			value = r->max;
			break;
		case INC:
			value = r->last-r->first;
			break;
	}

	return string_format("%lf",value);
}

/* vim: set noexpandtab tabstop=4: */
