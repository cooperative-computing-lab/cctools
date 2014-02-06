/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "reduction.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

struct reduction *reduction_create( const char *name, const char *attr )
{
	struct reduction *r;
	reduction_t type;

	if (strcmp(name,"CNT")==0)		type = CNT;
	else if (strcmp(name,"SUM")==0)		type = SUM;
	else if (strcmp(name,"FIRST")==0)	type = FIRST;
	else if (strcmp(name,"LAST")==0)	type = LAST;
	else if (strcmp(name,"MIN")==0)		type = MIN;
	else if (strcmp(name,"AVG")==0)		type = AVG;
	else if (strcmp(name,"MAX")==0)		type = MAX;
	else if (strcmp(name,"PAVG")==0)	type = PAVG;
	else if (strcmp(name,"INC")==0)		type = INC;
	else	return 0;

	r = malloc(sizeof(*r));
	memset(r,0,sizeof(*r));
	r->type = type;
	r->attr = strdup(attr);
	return r;
};

void reduction_delete( struct reduction *r )
{
	if(!r) return;
	free((char*)r->attr);
	free(r);
}

void reduction_reset( struct reduction *r )
{
	r->cnt = r->sum = r->first = r->last = r->min = r->max = 0;
};

void reduction_update( struct reduction *r, const char *value )
{
	double val = atof(value);

	if(r->cnt==0) {
		r->min = r->max = r->first = val;
	} else {
		if (val < r->min) r->min = val;
		if (val > r->max) r->max = val;
	}

	r->sum += val;
	r->last = val;
	r->cnt++;
};

void reduction_print( struct reduction *r )
{
	printf("%s",r->attr);
	switch(r->type) {
		case CNT:
			printf(".CNT %lf\n",r->cnt);
			break;
		case SUM:
			printf(".SUM %lf\n",r->sum);
			break;
		case FIRST:
			printf(".FIRST %lf\n",r->first);
			break;
		case LAST:
			printf(".LAST %lf\n",r->last);
			break;
		case MIN:
			printf(".MIN %lf\n",r->min);
			break;
		case AVG:
			printf(".AVG %lf\n",r->cnt>0 ? r->sum/r->cnt : 0);
			break;
		case MAX:
			printf(".MAX %lf\n",r->max);
			break;
		case PAVG:
			printf(".PAVG %lf\n",r->cnt>0 ? r->sum/r->cnt : 0 );
			break;
		case INC:
			printf(".INC %lf\n",r->last-r->first);
			break;
	}
}
