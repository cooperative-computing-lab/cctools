/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_VALUE_H
#define DELTADB_VALUE_H

#include <stdio.h>

typedef enum {
	DELTADB_TYPE_BOOLEAN,
	DELTADB_TYPE_INTEGER,
	DELTADB_TYPE_REAL,
	DELTADB_TYPE_STRING,
	DELTADB_TYPE_LIST,
	DELTADB_TYPE_ERROR
} deltadb_type_t;

struct deltadb_value {
	deltadb_type_t type;
	union {
		int integer;
		int boolean;
		char *string;
		double real;
		struct deltadb_value *list;
	} u;
	struct deltadb_value *next;
};

struct deltadb_value * deltadb_value_create_integer( int i );
struct deltadb_value * deltadb_value_create_boolean( int i );
struct deltadb_value * deltadb_value_create_real( double r );
struct deltadb_value * deltadb_value_create_string( const char *s );
struct deltadb_value * deltadb_value_create_list( struct deltadb_value *v );
struct deltadb_value * deltadb_value_create_error();
void deltadb_value_delete( struct deltadb_value *v );

void deltadb_value_print( FILE *file, struct deltadb_value *v );

int deltadb_value_check_type( struct deltadb_value *v, deltadb_type_t type );

struct deltadb_value * deltadb_value_to_type( struct deltadb_value *v, deltadb_type_t type );

struct deltadb_value * deltadb_value_copy( struct deltadb_value *v );

struct deltadb_value * deltadb_value_lt( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_le( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_gt( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_ge( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_eq( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_ne( struct deltadb_value *a, struct deltadb_value *b );

struct deltadb_value * deltadb_value_add( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_subtract( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_multiply( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_divide( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_modulus( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_power( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_negate( struct deltadb_value *a );
struct deltadb_value * deltadb_value_and( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_or( struct deltadb_value *a, struct deltadb_value *b );
struct deltadb_value * deltadb_value_not( struct deltadb_value *a );

#endif
