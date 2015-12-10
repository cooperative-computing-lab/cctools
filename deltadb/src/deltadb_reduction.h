/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_REDUCTION_H
#define DELTADB_REDUCTION_H

typedef enum {
	COUNT,
	SUM,
	FIRST,
	LAST,
	MIN,
	AVERAGE,
	MAX,
	INC
} deltadb_reduction_t;

struct deltadb_reduction {
	deltadb_reduction_t type;
	const char *attr;
	double count;
	double sum;
	double first;
	double last;
	double min;
	double max;
};

struct deltadb_reduction *deltadb_reduction_create( const char *name, const char *attr );
void deltadb_reduction_delete( struct deltadb_reduction *r );
void deltadb_reduction_reset( struct deltadb_reduction *r );
void deltadb_reduction_update( struct deltadb_reduction *r, double value );
double deltadb_reduction_value( struct deltadb_reduction *r );

#endif
