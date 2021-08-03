/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_REDUCTION_H
#define DELTADB_REDUCTION_H

#include "jx.h"
#include "hash_table.h"

typedef enum {
	COUNT,
	SUM,
	FIRST,
	LAST,
	MIN,
	AVERAGE,
	MAX,
	INC,
	UNIQUE
} deltadb_reduction_t;

struct deltadb_reduction {
	deltadb_reduction_t type;
	struct jx *expr;
	struct hash_table *unique_table;
	struct jx *unique_value;
	double count;
	double sum;
	double first;
	double last;
	double min;
	double max;
};

struct deltadb_reduction *deltadb_reduction_create( const char *name, struct jx *expr );
void deltadb_reduction_delete( struct deltadb_reduction *r );
void deltadb_reduction_reset( struct deltadb_reduction *r );
void deltadb_reduction_update( struct deltadb_reduction *r, struct jx *value );
char * deltadb_reduction_string( struct deltadb_reduction *r );

#endif
