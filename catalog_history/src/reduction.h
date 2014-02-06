/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef REDUCTION_H
#define REDUCTION_H

typedef enum {
	CNT,
	SUM,
	FIRST,
	LAST,
	MIN,
	AVG,
	MAX,
	PAVG,
	INC
} reduction_t;

struct reduction {
	reduction_t type;
	const char *attr;
	long cnt;
	long sum;
	long first;
	long last;
	long min;
	long max;
};

struct reduction *reduction_create( const char *name, const char *attr );
void reduction_delete( struct reduction *r );
void reduction_reset( struct reduction *r );
void reduction_update( struct reduction *r, const char *value );
void reduction_print( struct reduction *r );

#endif
