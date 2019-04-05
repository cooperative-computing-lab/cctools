/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"
#include "stringtools.h"
#include "buffer.h"
#include "xxmalloc.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct jx_pair * jx_pair( struct jx *key, struct jx *value, struct jx_pair *next )
{
	struct jx_pair *pair = calloc(1, sizeof(*pair));
	pair->key = key;
	pair->value = value;
	pair->next = next;
	return pair;
}

struct jx_item * jx_item( struct jx *value, struct jx_item *next )
{
	struct jx_item *item = calloc(1, sizeof(*item));
	item->value = value;
	item->next = next;
	return item;
}

struct jx_comprehension *jx_comprehension(const char *variable, struct jx *elements, struct jx *condition, struct jx_comprehension *next) {
	assert(variable);
	assert(elements);
	struct jx_comprehension *comp = calloc(1, sizeof(*comp));
	comp->variable = strdup(variable);
	comp->elements = elements;
	comp->condition = condition;
	comp->next = next;
	return comp;
}

static struct jx * jx_create( jx_type_t type )
{
	struct jx *j = xxcalloc(1, sizeof(*j));
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
	j->u.symbol_name = strdup(symbol_name);
	return j;
}

struct jx * jx_string( const char *string_value )
{
	assert(string_value);
	struct jx *j = jx_create(JX_STRING);
	j->u.string_value = strdup(string_value);
	return j;
}

struct jx * jx_format( const char *fmt, ... )
{
	va_list va;
	struct jx *j;
	buffer_t B[1];
	char *str;

	buffer_init(B);
	buffer_abortonfailure(B, 1);
	va_start(va, fmt);
	buffer_putvfstring(B, fmt, va);
	va_end(va);
	buffer_dup(B, &str);
	buffer_free(B);

	j = jx_create(JX_STRING);
	j->u.string_value = str;

	return j;
}

struct jx * jx_integer( jx_int_t integer_value )
{
	struct jx *j = jx_create(JX_INTEGER);
	j->u.integer_value = integer_value;
	return j;
}

struct jx * jx_double( double double_value )
{
	struct jx *j = jx_create(JX_DOUBLE);
	j->u.double_value = double_value;
	return j;
}

struct jx * jx_boolean( int boolean_value )
{
	struct jx *j = jx_create(JX_BOOLEAN);
	j->u.boolean_value = !!boolean_value;
	return j;
}

struct jx * jx_object( struct jx_pair *pairs )
{
	struct jx *j = jx_create(JX_OBJECT);
	j->u.pairs = pairs;
	return j;
}

struct jx * jx_array( struct jx_item *items )
{
	struct jx *j = jx_create(JX_ARRAY);
	j->u.items = items;
	return j;
}

struct jx * jx_operator( jx_operator_t type, struct jx *left, struct jx *right )
{
	struct jx * j = jx_create(JX_OPERATOR);
	j->u.oper.type = type;
	j->u.oper.left = left;
	j->u.oper.right = right;
	return j;
}

struct jx * jx_error( struct jx *err )
{
	if(!err) return NULL;
	struct jx *j = jx_create(JX_ERROR);
	j->u.err = err;
	return j;
}

struct jx *jx_function(const char *name, jx_builtin_t op,
	struct jx_item *params, struct jx *body) {
	assert(name);
	struct jx *j = jx_create(JX_FUNCTION);
	j->u.func.name = strdup(name);
	j->u.func.params = params;
	j->u.func.body = body;
	j->u.func.builtin = op;
	return j;
}

struct jx * jx_arrayv( struct jx *value, ... )
{
	va_list args;
	va_start(args,value);

	struct jx *array = jx_array(0);
	while(value) {
		jx_array_append(array,value);
		value = va_arg(args,struct jx *);
	}

	va_end(args);
	return array;
}

struct jx * jx_lookup_guard( struct jx *j, const char *key, int *found )
{
	struct jx_pair *p;

	if(found)
		*found = 0;

	if(!j || j->type!=JX_OBJECT) return 0;

	for(p=j->u.pairs;p;p=p->next) {
		if(p && p->key && p->key->type==JX_STRING) {
			if(!strcmp(p->key->u.string_value,key)) {
				if(found)
					*found = 1;
				return p->value;
			}
		}
	}

	return 0;
}

struct jx * jx_lookup( struct jx *j, const char *key )
{
	return jx_lookup_guard(j, key, NULL);
}

const char * jx_lookup_string( struct jx *object, const char *key )
{
	struct jx *j = jx_lookup(object,key);
	if(j && jx_istype(j,JX_STRING)) {
		return j->u.string_value;
	} else {
		return 0;
	}
}

jx_int_t jx_lookup_integer( struct jx *object, const char *key )
{
	struct jx *j = jx_lookup(object,key);
	if(j && jx_istype(j,JX_INTEGER)) {
		return j->u.integer_value;
	} else {
		return 0;
	}
}

int jx_lookup_boolean( struct jx *object, const char *key )
{
	struct jx *j = jx_lookup(object,key);
	if(j && jx_istype(j,JX_BOOLEAN)) {
		return !!j->u.boolean_value;
	} else {
		return 0;
	}
}

double jx_lookup_double( struct jx *object, const char *key )
{
	struct jx *j = jx_lookup(object,key);
	if(j && jx_istype(j,JX_DOUBLE)) {
		return j->u.double_value;
	} else {
		return 0;
	}
}

struct jx * jx_remove( struct jx *object, struct jx *key )
{
	if(!object || object->type!=JX_OBJECT) return 0;

	struct jx_pair *p;
	struct jx_pair *last = 0;

	for(p=object->u.pairs;p;p=p->next) {
		if(jx_equals(key,p->key)) {
			struct jx *value = p->value;
			if(last) {
				last->next = p->next;
			} else {
				object->u.pairs = p->next;
			}
			p->value = 0;
			p->next = 0;
			jx_pair_delete(p);
			return value;
		}
		last = p;
	}

	return 0;
}

int jx_insert( struct jx *j, struct jx *key, struct jx *value )
{
	if(!j || j->type!=JX_OBJECT) return 0;
	j->u.pairs = jx_pair(key,value,j->u.pairs);
	return 1;
}

int jx_insert_unless_empty( struct jx *object, struct jx *key, struct jx *value ) {
	switch(value->type) {
		case JX_OBJECT:
		case JX_ARRAY:
			/* C99 says union members have the same start address, so
			 * just pick one, they're both pointers. */
			if(value->u.pairs == NULL) {
				jx_delete(key);
				jx_delete(value);
				return -1;
			} else {
				return jx_insert(object, key, value);
			}
			break;
		default:
			return jx_insert(object, key, value);
			break;
	}
}

void jx_insert_integer( struct jx *j, const char *key, jx_int_t value )
{
	jx_insert(j,jx_string(key),jx_integer(value));
}

void jx_insert_double( struct jx *j, const char *key, double value )
{
	jx_insert(j,jx_string(key),jx_double(value));
}

void jx_insert_string( struct jx *j, const char *key, const char *value )
{
	jx_insert(j,jx_string(key),jx_string(value));
}

void jx_array_insert( struct jx *array, struct jx *value )
{
	array->u.items = jx_item(value, array->u.items);
}

void jx_array_append( struct jx *array, struct jx *value )
{
	struct jx_item **i;
	for(i=&array->u.items;*i;i=&(*i)->next) { }
	*i = jx_item(value,0);
}

struct jx * jx_array_index( struct jx *j, int nth )
{
	if (!jx_istype(j, JX_ARRAY)) return NULL;
	if (nth < 0) return NULL;
	struct jx_item *item = j->u.items;

	for(int i = 0; i < nth; i++) {
		if (!item) return NULL;
		item = item->next;
	}
	return item ? item->value : NULL;
}

int jx_array_length( struct jx *array )
{
	if(!jx_istype(array, JX_ARRAY)) return -1;
	int count = 0;
	for(struct jx_item *i = array->u.items; i; i = i->next) ++count;
	return count;
}

struct jx *jx_array_concat( struct jx *array, ...) {
	struct jx *result = jx_array(NULL);
	struct jx_item **tail = &result->u.items;
	va_list ap;
	va_start(ap, array);
	for(struct jx *a = array; a; a = va_arg(ap, struct jx *)) {
		if (!jx_istype(a, JX_ARRAY)) {
			break;
		}
		*tail = a->u.items;
		while(*tail) tail = &(*tail)->next;
		free(a);
	}
	va_end(ap);
	return result;
}

struct jx *jx_array_shift(struct jx *array) {
	if (!jx_istype(array, JX_ARRAY)) return NULL;
	struct jx_item *i = array->u.items;
	struct jx *result = NULL;
	if (i) {
		result = i->value;
		array->u.items = i->next;
		free(i);
	}
	return result;

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
	jx_comprehension_delete(item->comp);
	jx_item_delete(item->next);
	free(item);
}

void jx_comprehension_delete(struct jx_comprehension *comp) {
	if (!comp) return;
	free(comp->variable);
	jx_delete(comp->elements);
	jx_delete(comp->condition);
	jx_comprehension_delete(comp->next);
	free(comp);
}

void jx_delete( struct jx *j )
{
	if(!j) return;

	switch(j->type) {
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_NULL:
			break;
		case JX_SYMBOL:
			free(j->u.symbol_name);
			break;
		case JX_STRING:
			free(j->u.string_value);
			break;
		case JX_ARRAY:
			jx_item_delete(j->u.items);
			break;
		case JX_OBJECT:
			jx_pair_delete(j->u.pairs);
			break;
		case JX_OPERATOR:
			jx_delete(j->u.oper.left);
			jx_delete(j->u.oper.right);
			break;
		case JX_FUNCTION:
			free(j->u.func.name);
			jx_item_delete(j->u.func.params);
			jx_delete(j->u.func.body);
			break;
		case JX_ERROR:
			jx_delete(j->u.err);
			break;
	}
	free(j);
}

int jx_isatomic( struct jx *j )
{
	switch(j->type) {
		case JX_BOOLEAN:
		case JX_STRING:
		case JX_INTEGER:
		case JX_DOUBLE:
			return 1;
		default:
			return 0;
	}
}

int jx_istype( struct jx *j, jx_type_t type )
{
	return j && j->type==type;
}

int jx_istrue( struct jx *j )
{
	return j && j->type==JX_BOOLEAN && j->u.boolean_value;
}

int jx_comprehension_equals(struct jx_comprehension *j, struct jx_comprehension *k) {
	if (!j && !k) return 1;
	if (!j || !k) return 0;
	return !strcmp(j->variable, k->variable)
		&& jx_equals(j->elements, k->elements)
		&& jx_equals(j->condition, k->condition)
		&& jx_comprehension_equals(j->next, k->next);
}

int jx_pair_equals( struct jx_pair *j, struct jx_pair *k )
{
	if(!j && !k) return 1;
	if(!j || !k) return 0;
	return jx_equals(j->key,k->key) && jx_equals(j->value,k->value) && jx_pair_equals(j->next,k->next);
}

int jx_item_equals( struct jx_item *j, struct jx_item *k )
{
	if(!j && !k) return 1;
	if(!j || !k) return 0;
	return jx_equals(j->value, k->value)
		&& jx_comprehension_equals(j->comp, k->comp)
		&& jx_item_equals(j->next, k->next);
}

int jx_equals( struct jx *j, struct jx *k )
{
	if(!j && !k) return 1;
	if(!j || !k) return 0;
	if(j->type!=k->type) return 0;

	switch(j->type) {
		case JX_NULL:
			return 1;
		case JX_DOUBLE:
			return j->u.double_value==k->u.double_value;
		case JX_BOOLEAN:
			return j->u.boolean_value==k->u.boolean_value;
		case JX_INTEGER:
			return j->u.integer_value==k->u.integer_value;
		case JX_SYMBOL:
			return !strcmp(j->u.symbol_name,k->u.symbol_name);
		case JX_STRING:
			return !strcmp(j->u.string_value,k->u.string_value);
		case JX_ARRAY:
			return jx_item_equals(j->u.items,k->u.items);
		case JX_OBJECT:
			return jx_pair_equals(j->u.pairs,k->u.pairs);
		case JX_OPERATOR:
			return j->u.oper.type == k->u.oper.type
				&& jx_equals(j->u.oper.left,k->u.oper.right)
				&& jx_equals(j->u.oper.right,j->u.oper.right);
		case JX_FUNCTION:
			return !strcmp(j->u.func.name, k->u.func.name)
				&& jx_item_equals(
					   j->u.func.params, k->u.func.params)
				&& jx_equals(j->u.func.body, k->u.func.body);
		case JX_ERROR:
			return jx_equals(j->u.err, k->u.err);
	}

	/* not reachable, but some compilers complain. */
	return 0;
}

struct jx_comprehension *jx_comprehension_copy(struct jx_comprehension *c) {
	if (!c) return NULL;
	struct jx_comprehension *comp = calloc(1, sizeof(*comp));
	comp->line = c->line;
	comp->variable = strdup(c->variable);
	comp->elements = jx_copy(c->elements);
	comp->condition = jx_copy(c->condition);
	comp->next = jx_comprehension_copy(c->next);
	return comp;
}

struct jx_pair * jx_pair_copy( struct jx_pair *p )
{
	if (!p) return NULL;
	struct jx_pair *pair = calloc(1, sizeof(*pair));
	pair->key = jx_copy(p->key);
	pair->value = jx_copy(p->value);
	pair->next = jx_pair_copy(p->next);
	pair->line = p->line;
	return pair;
}

struct jx_item * jx_item_copy( struct jx_item *i )
{
	if (!i) return NULL;
	struct jx_item *item = calloc(1, sizeof(*item));
	item->line = i->line;
	item->value = jx_copy(i->value);
	item->comp = jx_comprehension_copy(i->comp);
	item->next = jx_item_copy(i->next);
	return item;
}

struct jx  *jx_copy( struct jx *j )
{
	if(!j) return 0;
	struct jx *c;

	switch(j->type) {
		case JX_NULL:
			c = jx_null();
			break;
		case JX_DOUBLE:
			c = jx_double(j->u.double_value);
			break;
		case JX_BOOLEAN:
			c = jx_boolean(j->u.boolean_value);
			break;
		case JX_INTEGER:
			c = jx_integer(j->u.integer_value);
			break;
		case JX_SYMBOL:
			c = jx_symbol(j->u.symbol_name);
			break;
		case JX_STRING:
			c = jx_string(j->u.string_value);
			break;
		case JX_ARRAY:
			c = jx_array(jx_item_copy(j->u.items));
			break;
		case JX_OBJECT:
			c = jx_object(jx_pair_copy(j->u.pairs));
			break;
		case JX_OPERATOR:
			c = jx_operator(j->u.oper.type, jx_copy(j->u.oper.left), jx_copy(j->u.oper.right));
			break;
		case JX_FUNCTION:
			c = jx_function(j->u.func.name, j->u.func.builtin,
				jx_item_copy(j->u.func.params),
				jx_copy(j->u.func.body));
			break;
		case JX_ERROR:
			c = jx_error(jx_copy(j->u.err));
			break;
	}

	c->line = j->line;
	return c;
}

struct jx *jx_merge(struct jx *j, ...) {
	va_list ap;
	va_start (ap, j);
	struct jx_pair *result = NULL;
	for (struct jx *next = j; jx_istype(next, JX_OBJECT); next = va_arg(ap, struct jx *)) {
		struct jx_pair *tmp = result;
		result = jx_pair_copy(next->u.pairs);
		struct jx_pair **last = &result;
		while (*last) last = &(*last)->next;
		*last = tmp;
	}
	va_end(ap);
	return jx_object(result);
}

static int jx_pair_is_constant(struct jx_pair *p) {
	if(!p) return 1;
	return jx_is_constant(p->key)
		&& jx_is_constant(p->value)
		&& jx_pair_is_constant(p->next);
}

static int jx_item_is_constant(struct jx_item *i) {
	if(!i) return 1;
	if (i->comp) return 0;
	return jx_is_constant(i->value) && jx_item_is_constant(i->next);
}

int jx_is_constant( struct jx *j )
{
	if(!j) return 0;
	switch(j->type) {
		case JX_SYMBOL:
			return 0;
		case JX_DOUBLE:
		case JX_BOOLEAN:
		case JX_INTEGER:
		case JX_STRING:
		case JX_NULL:
			return 1;
		case JX_ARRAY:
			return jx_item_is_constant(j->u.items);
		case JX_OBJECT:
			return jx_pair_is_constant(j->u.pairs);
		case JX_ERROR:
		case JX_FUNCTION:
		case JX_OPERATOR:
			return 0;
	}

	/* not reachable, but some compilers complain. */
	return 0;
}

void jx_export( struct jx *j )
{
	if(!j || !jx_istype(j,JX_OBJECT)) return;

	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		if(p->key->type==JX_STRING && p->value->type==JX_STRING) {
			setenv(p->key->u.string_value,p->value->u.string_value,1);
		}
	}
}

struct jx * jx_iterate_array(struct jx *j, void **i) {
	struct jx_item **x = (struct jx_item **) i;
	assert(x);

	if (*x) {
		*x = (*x)->next;
	} else if (jx_istype(j, JX_ARRAY)) {
		*x = j->u.items;
	}
	return *x ? (*x)->value : NULL;
}

static void advance_object_iter(struct jx *j, void **i) {
	struct jx_pair **p = (struct jx_pair **) i;
	assert(p);

	if (*p) {
		*p = (*p)->next;
	} else if (jx_istype(j, JX_OBJECT)) {
		*p = j->u.pairs;
	}
}

const char *jx_get_key(void **i) {
	assert(i);
	struct jx_pair *p = *i;
	return p ? p->key->u.string_value : NULL;
}

struct jx *jx_get_value(void **i) {
	assert(i);
	struct jx_pair *p = *i;
	return p ? p->value : NULL;
}

const char *jx_iterate_keys(struct jx *j, void **i) {
	advance_object_iter(j, i);
	return jx_get_key(i);
}

struct jx * jx_iterate_values(struct jx *j, void **i) {
	advance_object_iter(j, i);
	return jx_get_value(i);
}
