/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next )
{
	struct jx_pair *pair = malloc(sizeof(*pair));
	pair->key = key;
	pair->value = value;
	pair->next = next;
	return pair;
}

struct jx_item * jx_item( struct jx *value, struct jx_item *next )
{
	struct jx_item *item = malloc(sizeof(*item));
	item->value = value;
	item->next = next;
	return item;
}

static struct jx * jx_create( jx_type_t type )
{
	struct jx *j = malloc(sizeof(*j));
	j->type = type;
	return j;
}

struct jx * jx_null()
{
	return jx_create(JX_NULL);
}

struct jx * jx_symbol( const char *symbol_name )
{
	struct jx *j = jx_create(JX_SYMBOL);
	j->symbol_name = strdup(symbol_name);
	return j;
}

struct jx * jx_string( const char *string_value )
{
	struct jx *j = jx_create(JX_STRING);
	j->string_value = strdup(string_value);
	return j;
}

struct jx * jx_integer( jx_int_t integer_value )
{
	struct jx *j = jx_create(JX_INTEGER);
	j->integer_value = integer_value;
	return j;
}

struct jx * jx_float( double float_value )
{
	struct jx *j = jx_create(JX_FLOAT);
	j->float_value = float_value;
	return j;
}

struct jx * jx_boolean( int boolean_value )
{
	struct jx *j = jx_create(JX_BOOLEAN);
	j->boolean_value = boolean_value;
	return j;
}

struct jx * jx_object( struct jx_pair *pairs )
{
	struct jx *j = jx_create(JX_OBJECT);
	j->pairs = pairs;
	return j;
}

struct jx * jx_array( struct jx_item *items )
{
	struct jx *j = jx_create(JX_ARRAY);
	j->items = items;
	return j;
}

struct jx * jx_object_lookup( struct jx *j, const char *key )
{
	struct jx_pair *p;

	if(!j || j->type!=JX_OBJECT) return 0;

	for(p=j->pairs;p;p=p->next) {
		if(p && p->key && p->key->type==JX_STRING) {
			if(!strcmp(p->key->string_value,key)) {
				return p->value;
			}
		}
	}

	return 0;
}

struct jx * jx_object_lookup_expr( struct jx *j, struct jx *key )
{
	struct jx_pair *p;

	if(!j || j->type!=JX_OBJECT) return 0;

	for(p=j->pairs;p;p=p->next) {
		if(p && jx_equals(p->key,key)) {
			return p->value;
		}
	}

	return 0;
}

int jx_object_insert( struct jx *j, struct jx *key, struct jx *value )
{
	if(!j || j->type!=JX_OBJECT) return 0;
	j->pairs = jx_pair(key,value,j->pairs);
	return 1;
}

void jx_array_insert( struct jx *array, struct jx *value )
{
	array->items = jx_item( value, array->items->next );
}

void jx_array_append( struct jx *array, struct jx *value )
{
	if(!array->items) {
		array->items = jx_item( value, 0 );
	} else {
		struct jx_item *i;
		for(i=array->items;i->next;i=i->next) { }
		i->next = jx_item(value,0);
	}
}

void jx_pair_delete( struct jx_pair *pair )
{
	if(!pair) return;
	jx_delete(pair->key);
	jx_delete(pair->value);
       	jx_pair_delete(pair->next);
	free(pair);
}

void jx_item_delete( struct jx_item *item )
{
	if(!item) return;
	jx_delete(item->value);
       	jx_item_delete(item->next);
	free(item);
}

void jx_delete( struct jx *j )
{
	if(!j) return;

	switch(j->type) {
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_NULL:
			break;
		case JX_SYMBOL:
			free(j->symbol_name);
			break;
		case JX_STRING:
			free(j->string_value);
			break;
		case JX_ARRAY:
			jx_item_delete(j->items);
			break;
		case JX_OBJECT:
			jx_pair_delete(j->pairs);
			break;
	}
}

int jx_istype( struct jx *j, jx_type_t type )
{
	return j && j->type==type;
}

int jx_pair_equals( struct jx_pair *j, struct jx_pair *k ) 
{
	return jx_equals(j->key,k->key) && jx_equals(j->value,k->value) && jx_pair_equals(j->next,k->next);
}

int jx_item_equals( struct jx_item *j, struct jx_item *k ) 
{
	return jx_equals(j->value,k->value) && jx_item_equals(j->next,k->next);
}

int jx_equals( struct jx *j, struct jx *k )
{
	if(!j && !k) return 1;
	if(!j || !k) return 0;
	if(j->type!=k->type) return 0;

	switch(j->type) {
		case JX_NULL:
			return 1;
		case JX_FLOAT:
			return j->float_value==k->float_value;
		case JX_BOOLEAN:
			return j->boolean_value==k->boolean_value;
		case JX_INTEGER:
			return j->integer_value==k->integer_value;
		case JX_SYMBOL:
			return !strcmp(j->symbol_name,k->symbol_name);
		case JX_STRING:
			return !strcmp(j->string_value,k->string_value);
		case JX_ARRAY:
			return jx_item_equals(j->items,k->items);
		case JX_OBJECT:
			return jx_pair_equals(j->pairs,k->pairs);
	}

	/* not reachable, but some compilers complain. */
	return 0;
}

struct jx_pair * jx_pair_copy( struct jx_pair *p )
{
	struct jx_pair *pair = malloc(sizeof(*pair));
	pair->key = jx_copy(p->key);
	pair->value = jx_copy(p->value);
	pair->next = jx_pair_copy(p->next);
	return pair;
}

struct jx_item * jx_item_copy( struct jx_item *i )
{
	struct jx_item *item = malloc(sizeof(*item));
	item->value = jx_copy(i->value);
	item->next = jx_item_copy(i->next);
	return item;
}

struct jx  *jx_copy( struct jx *j )
{
	switch(j->type) {
		case JX_NULL:
			return jx_null();
		case JX_FLOAT:
			return jx_float(j->float_value);
		case JX_BOOLEAN:
			return jx_boolean(j->boolean_value);
		case JX_INTEGER:
			return jx_integer(j->integer_value);
		case JX_SYMBOL:
			return jx_symbol(j->symbol_name);
		case JX_STRING:
			return jx_string(j->string_value);
		case JX_ARRAY:
			return jx_array(jx_item_copy(j->items));
		case JX_OBJECT:
			return jx_object(jx_pair_copy(j->pairs));
	}

	/* not reachable, but some compilers complain. */
	return 0;
}

int jx_pair_is_constant( struct jx_pair *p )
{
	return jx_is_constant(p->key)
		&& jx_is_constant(p->value)
		&& jx_pair_is_constant(p->next);
}

int jx_item_is_constant( struct jx_item *i )
{
	return jx_is_constant(i->value) && jx_item_is_constant(i->next);
}

int jx_is_constant( struct jx *j )
{
	switch(j->type) {
		case JX_SYMBOL:
			return 0;
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return 1;
		case JX_ARRAY:
			return jx_item_is_constant(j->items);
		case JX_OBJECT:
			return jx_pair_is_constant(j->pairs);
	}

	/* not reachable, but some compilers complain. */
	return 0;
}

struct jx_pair * jx_pair_evaluate( struct jx_pair *pair, jx_eval_func_t func )
{
	return jx_pair(
		jx_evaluate(pair->key,func),
		jx_evaluate(pair->value,func),
		jx_pair_evaluate(pair->next,func)
	);
}

struct jx_item * jx_item_evaluate( struct jx_item *item, jx_eval_func_t func )
{
	return jx_item(
		jx_evaluate(item->value,func),
		jx_item_evaluate(item->next,func)
	);
}

struct jx * jx_evaluate( struct jx *j, jx_eval_func_t func )
{
	switch(j->type) {
		case JX_SYMBOL:
			return func(j->symbol_name);
		case JX_FLOAT:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return jx_copy(j);
		case JX_ARRAY:
			return jx_array(jx_item_evaluate(j->items,func));
		case JX_OBJECT:
			return jx_object(jx_pair_evaluate(j->pairs,func));
	}
	/* not reachable, but some compilers complain. */
	return 0;
}
