
#include "deltadb_value.h"

#include <math.h>
#include <string.h>

static struct deltadb_value * deltadb_sin( struct deltadb_value *arg )
{
	struct deltadb_value *a = deltadb_value_to_type(arg,DELTADB_TYPE_REAL);

	if(a && a->type==DELTADB_TYPE_REAL) {
		a->u.real = sin(a->u.real);
		return a;
	} else {
		deltadb_value_delete(a);
		return deltadb_value_create_error();
	}
}

static struct deltadb_value * deltadb_cos( struct deltadb_value *arg )
{
	struct deltadb_value *a = deltadb_value_to_type(arg,DELTADB_TYPE_REAL);

	if(a && a->type==DELTADB_TYPE_REAL) {
		a->u.real = cos(a->u.real);
		return a;
	} else {
		deltadb_value_delete(a);
		return deltadb_value_create_error();
	}
}

static struct deltadb_value * deltadb_log( struct deltadb_value *arg )
{
	struct deltadb_value *a = deltadb_value_to_type(arg,DELTADB_TYPE_REAL);

	if(a && a->type==DELTADB_TYPE_REAL) {
		a->u.real = log(a->u.real);
		return a;
	} else {
		deltadb_value_delete(a);
		return deltadb_value_create_error();
	}
}

/*
COUNT(..) is expecting a list like so: COUNT([1,2,3])
If multiple non-list arguments are passed in instead: COUNT(1,2,3)
then we count those instead.  Need to think about whether that is correct.
*/

static struct deltadb_value * deltadb_count( struct deltadb_value *args )
{
	struct deltadb_value *v;

	if(args && args->type==DELTADB_TYPE_LIST) {
		v = args->u.list;
	} else {
		v = args;
	}

	int count = 0;

	while(v) { v=v->next; count++; }

	deltadb_value_delete(args);

	return deltadb_value_create_integer(count);
}

struct deltadb_value * deltadb_function_call( const char *name, struct deltadb_value *args )
{
	if(!strcmp(name,"sin")) {
		return deltadb_sin(args);
	} else if(!strcmp(name,"cos")) {
		return deltadb_cos(args);
	} else if(!strcmp(name,"log")) {
		return deltadb_log(args);
	} else if(!strcmp(name,"count")) {
		return deltadb_count(args);
	} else {
		return deltadb_value_create_error();
	}

}

