/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_value.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

struct deltadb_value * deltadb_value_create_integer( int i )
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_INTEGER;
	v->u.integer = i;
	v->next = 0;
	return v;
}

struct deltadb_value * deltadb_value_create_boolean( int b )
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_BOOLEAN;
	v->u.boolean = b;
	v->next = 0;
	return v;
}

struct deltadb_value * deltadb_value_create_real( double r )
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_REAL;
	v->u.real = r;
	v->next = 0;
	return v;
}

struct deltadb_value * deltadb_value_create_string( const char *s )
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_STRING;
	v->u.string = strdup(s);
	v->next = 0;
	return v;
}

struct deltadb_value * deltadb_value_create_error()
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_ERROR;
	v->next = 0;
	return v;
}

struct deltadb_value * deltadb_value_create_list( struct deltadb_value *head )
{
	struct deltadb_value *v = malloc(sizeof(*v));
	v->type = DELTADB_TYPE_LIST;
	v->next = 0;
	v->u.list = head;
	return v;
}

void deltadb_value_print( FILE *file, struct deltadb_value *v )
{
	switch(v->type) {
		case DELTADB_TYPE_INTEGER:
			fprintf(file,"%d",v->u.integer);
			break;
		case DELTADB_TYPE_BOOLEAN:
			fprintf(file,"%s",v->u.boolean?"true":"false");
			break;
		case DELTADB_TYPE_REAL:
			fprintf(file,"%lf",v->u.real);
			break;
		case DELTADB_TYPE_STRING:
			// XXX NEED TO ESCAPE STRING HERE
			fprintf(file,"\"%s\"",v->u.string);
			break;
		case DELTADB_TYPE_LIST:
			fprintf(file,"[");
			v = v->u.list;
			while(v) {
				deltadb_value_print(file,v);
				if(v->next) fprintf(file,",");
				v = v->next;
			}
			fprintf(file,"]");
			break;
		case DELTADB_TYPE_ERROR:
			fprintf(file,"ERROR ");
			break;
	}
}

void deltadb_value_delete( struct deltadb_value *v )
{
	struct deltadb_value *n;

	if(!v) return;

	switch(v->type) {
		case DELTADB_TYPE_INTEGER:
		case DELTADB_TYPE_BOOLEAN:
		case DELTADB_TYPE_REAL:
		case DELTADB_TYPE_ERROR:
			break;
		case DELTADB_TYPE_STRING:
			free(v->u.string);
			break;
		case DELTADB_TYPE_LIST:
			deltadb_value_delete(v->u.list);
			break;
	}

	n = v->next;
	free(v);
	deltadb_value_delete(n);
}

struct deltadb_value * deltadb_value_copy( struct deltadb_value *v )
{
	if(!v) return 0;

	struct deltadb_value *n = malloc(sizeof(*n));

	n->type = v->type;

	switch(v->type) {
		case DELTADB_TYPE_INTEGER:
			n->u.integer = v->u.integer;
			break;
		case DELTADB_TYPE_BOOLEAN:
			n->u.boolean = v->u.boolean;
			break;
		case DELTADB_TYPE_REAL:
			n->u.real = v->u.real;
			break;
		case DELTADB_TYPE_STRING:
			n->u.string = strdup(v->u.string);
			break;
		case DELTADB_TYPE_LIST:
			n->u.list = deltadb_value_copy(v->u.list);
			break;
		case DELTADB_TYPE_ERROR:
			break;
	}

	n->next = deltadb_value_copy(v->next);

	return n;
}

int deltadb_value_check_type( struct deltadb_value *v, deltadb_type_t type )
{
	return v->type==type;
}

struct deltadb_value * deltadb_value_to_string( struct deltadb_value *v )
{
	char str[32];
	struct deltadb_value *result;

	switch(v->type) {
		case DELTADB_TYPE_BOOLEAN:
			if(v->u.boolean) {
				result = deltadb_value_create_string("true");
			} else {
				result = deltadb_value_create_string("false");
			}
			break;
		case DELTADB_TYPE_INTEGER:
			sprintf(str,"%d",v->u.integer);
			result = deltadb_value_create_string(str);
			break;
		case DELTADB_TYPE_REAL:
			sprintf(str,"%lf",v->u.real);
			result = deltadb_value_create_string(str);
			break;
		case DELTADB_TYPE_STRING:
			result = deltadb_value_copy(v);
			break;
		case DELTADB_TYPE_LIST:
			result = deltadb_value_create_string("[ ... ]");
			break;
		case DELTADB_TYPE_ERROR:
			result = deltadb_value_create_error();
			break;
	}

	deltadb_value_delete(v);
	return result;
}

struct deltadb_value * deltadb_value_to_integer( struct deltadb_value *v )
{
	int i;
	struct deltadb_value *result;

	switch(v->type) {
		case DELTADB_TYPE_BOOLEAN:
			result = deltadb_value_create_error();
			break;
		case DELTADB_TYPE_INTEGER:
			result = deltadb_value_create_integer(v->u.integer);
			break;
		case DELTADB_TYPE_REAL:
			result = deltadb_value_create_integer(v->u.real);
			break;
		case DELTADB_TYPE_STRING:
			if(sscanf(v->u.string,"%d",&i)==1) {
				result = deltadb_value_create_integer(i);
			} else {
				result = deltadb_value_create_error();
			}
			break;
		case DELTADB_TYPE_LIST:
		case DELTADB_TYPE_ERROR:
			result = deltadb_value_create_error();
			break;
	}

	deltadb_value_delete(v);
	return result;
}

struct deltadb_value * deltadb_value_to_real( struct deltadb_value *v )
{
	double r;
	struct deltadb_value *result;

	switch(v->type) {
		case DELTADB_TYPE_BOOLEAN:
			result = deltadb_value_create_error();
			break;
		case DELTADB_TYPE_INTEGER:
			result = deltadb_value_create_real(v->u.integer);
			break;
		case DELTADB_TYPE_REAL:
			result = deltadb_value_create_real(v->u.real);
			break;
		case DELTADB_TYPE_STRING:
			if(sscanf(v->u.string,"%lf",&r)==1) {
				result = deltadb_value_create_real(r);
			} else {
				result = deltadb_value_create_error();
			}
			break;
		case DELTADB_TYPE_LIST:
		case DELTADB_TYPE_ERROR:
			result = deltadb_value_create_error();
			break;
	}

	deltadb_value_delete(v);
	return result;
}

struct deltadb_value * deltadb_value_to_type( struct deltadb_value *v, deltadb_type_t type )
{
	if(v->type==type) return v;

	switch(type) {
		case DELTADB_TYPE_INTEGER:
			return deltadb_value_to_integer(v);
			break;
		case DELTADB_TYPE_REAL:
			return deltadb_value_to_real(v);
			break;
		case DELTADB_TYPE_STRING:
			return deltadb_value_to_string(v);
			break;
		case DELTADB_TYPE_BOOLEAN:
		case DELTADB_TYPE_LIST:
		case DELTADB_TYPE_ERROR:
			deltadb_value_delete(v);
			return deltadb_value_create_error();
			break;
	}

	deltadb_value_delete(v);
	return deltadb_value_create_error();
}

int deltadb_value_promote( struct deltadb_value **a, struct deltadb_value **b )
{
	if((*a)->type==(*b)->type) return 1;

	if((*a)->type==DELTADB_TYPE_INTEGER && (*b)->type==DELTADB_TYPE_REAL) {
		*a = deltadb_value_to_real(*a);
		return 1;
	}

	if((*a)->type==DELTADB_TYPE_REAL && (*b)->type==DELTADB_TYPE_INTEGER) {
		*b = deltadb_value_to_real(*b);
		return 1;
	}

	if((*a)->type==DELTADB_TYPE_STRING) {
		*b = deltadb_value_to_string(*b);
		return 1;
	}

	if((*b)->type==DELTADB_TYPE_STRING) {
		*a = deltadb_value_to_string(*a);
		return 1;
	}

	return 0;
}

struct deltadb_value * deltadb_value_lt( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer<b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real<b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)<0);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_le( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer<=b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real<=b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)<=0);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_gt( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer>b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real>b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)>0);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_ge( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer>=b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real>=b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)>=0);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_eq( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer==b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real==b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)==0);
		} else if(a->type==DELTADB_TYPE_BOOLEAN) {
			result = deltadb_value_create_boolean(a->u.boolean==b->u.boolean);
		} else if(a->type==DELTADB_TYPE_ERROR) {
			result = deltadb_value_create_boolean(a->type==b->type);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_ne( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_boolean(a->u.integer!=b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_boolean(a->u.real!=b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			result = deltadb_value_create_boolean(strcmp(a->u.string,b->u.string)!=0);
		} else if(a->type==DELTADB_TYPE_BOOLEAN) {
			result = deltadb_value_create_boolean(a->u.boolean!=b->u.boolean);
		} else if(a->type==DELTADB_TYPE_ERROR) {
			result = deltadb_value_create_boolean(a->type!=b->type);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_add( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_integer(a->u.integer+b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_real(a->u.real+b->u.real);
		} else if(a->type==DELTADB_TYPE_STRING) {
			char *s = malloc(strlen(a->u.string)+strlen(b->u.string)+1);
			strcpy(s,a->u.string);
			strcat(s,b->u.string);
			result = deltadb_value_create_string(s);
			free(s);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_subtract( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			result = deltadb_value_create_integer(a->u.integer-b->u.integer);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_real(a->u.real-b->u.real);
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_multiply( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(!deltadb_value_promote(&a,&b)) result = deltadb_value_create_error();	

	if(a->type==DELTADB_TYPE_INTEGER) {
		result = deltadb_value_create_integer(a->u.integer*b->u.integer);
	} else if(a->type==DELTADB_TYPE_REAL) {
		result = deltadb_value_create_real(a->u.real*b->u.real);
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_divide( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			if(b->u.integer==0) {
				result = deltadb_value_create_error();
			} else {
				result = deltadb_value_create_integer(a->u.integer/b->u.integer	);
			}
		} else if(a->type==DELTADB_TYPE_REAL) {
			if(b->u.real==0) {
				result = deltadb_value_create_error();
			} else {
				result = deltadb_value_create_real(a->u.real/b->u.real);
			}
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_modulus( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			if(b->u.integer==0) {
				result = deltadb_value_create_error();
			} else {
				result = deltadb_value_create_integer(a->u.integer%b->u.integer);
			}
		} else if(a->type==DELTADB_TYPE_REAL) {
			if(b->u.real==0) {
				result = deltadb_value_create_error();
			} else {
				result = deltadb_value_create_real(fmod(a->u.real,b->u.real));
			}
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();	
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_power( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(deltadb_value_promote(&a,&b)) {
		if(a->type==DELTADB_TYPE_INTEGER) {
			int r = 1;
			int i;
			for(i=0;i<b->u.integer;i++) {
				r *= a->u.integer;
			}
			result = deltadb_value_create_integer(r);
		} else if(a->type==DELTADB_TYPE_REAL) {
			result = deltadb_value_create_real(pow(a->u.real,b->u.real));
		} else {
			result = deltadb_value_create_error();
		}
	} else {
		result = deltadb_value_create_error();	
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_negate( struct deltadb_value *a )
{
	struct deltadb_value *result;

	if(a->type==DELTADB_TYPE_INTEGER) {
		result = deltadb_value_create_integer(-a->u.integer);
	} else if(a->type==DELTADB_TYPE_REAL) {
		result = deltadb_value_create_real(-a->u.real);
	} else {
		result = deltadb_value_create_error();
	}

	deltadb_value_delete(a);

	return result;
}

struct deltadb_value * deltadb_value_and( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(a->type!=DELTADB_TYPE_BOOLEAN || b->type!=DELTADB_TYPE_BOOLEAN) {
		result = deltadb_value_create_error();
	} else {
		result = deltadb_value_create_boolean(a->u.boolean && b->u.boolean);
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_or( struct deltadb_value *a, struct deltadb_value *b )
{
	struct deltadb_value *result;

	if(a->type!=DELTADB_TYPE_BOOLEAN || b->type!=DELTADB_TYPE_BOOLEAN) {
		result = deltadb_value_create_error();
	} else {
		result = deltadb_value_create_boolean(a->u.boolean || b->u.boolean);
	}

	deltadb_value_delete(a);
	deltadb_value_delete(b);

	return result;
}

struct deltadb_value * deltadb_value_not( struct deltadb_value *a )
{
	struct deltadb_value *result;

	if(a->type!=DELTADB_TYPE_BOOLEAN) {
 		result = deltadb_value_create_error();
	} else {
		result = deltadb_value_create_boolean(!a->u.boolean);
	}

	deltadb_value_delete(a);

	return result;
}
