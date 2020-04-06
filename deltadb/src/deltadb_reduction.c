/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_reduction.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

struct deltadb_reduction *deltadb_reduction_create( const char *name, struct jx *expr )
{
	struct deltadb_reduction *r;
	deltadb_reduction_t type;

	if (strcmp(name,"COUNT")==0)		type = COUNT;
	else if (strcmp(name,"SUM")==0)		type = SUM;
	else if (strcmp(name,"FIRST")==0)	type = FIRST;
	else if (strcmp(name,"LAST")==0)	type = LAST;
	else if (strcmp(name,"MIN")==0)		type = MIN;
	else if (strcmp(name,"AVERAGE")==0)	type = AVERAGE;
	else if (strcmp(name,"MAX")==0)		type = MAX;
	else if (strcmp(name,"INC")==0)		type = INC;
	else	return 0;

	r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->type = type;
	r->expr = expr;

	return r;
};

void deltadb_reduction_delete( struct deltadb_reduction *r )
{
	if(!r) return;
	jx_delete(r->expr);
	free(r);
}

void deltadb_reduction_reset( struct deltadb_reduction *r )
{
	r->count = r->sum = r->first = r->last = r->min = r->max = 0;
};

void deltadb_reduction_update( struct deltadb_reduction *r, double val )
{
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

double deltadb_reduction_value( struct deltadb_reduction *r )
{
	double value = 0;
	switch(r->type) {
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
	return value;
}

/* vim: set noexpandtab tabstop=4: */
