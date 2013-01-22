/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

struct params {
       double v1;
       double v2;
       double q1;
       double q2;
};

struct params * params_load( const char *path )
{
	struct params *p;

	FILE *file = fopen(path,"r");
	if(!file) return 0;

	p = malloc(sizeof(*p));

	fscanf(file,"%lf %lf %lf %lf",&p->v1,&p->v2,&p->q1,&p->q2);

	fclose(file);

	return p;
}

int main( int argc, char *argv[] )
{
	struct params *x,*y,*d;
	int X, Y;

	if(argc!=6) {
		fprintf(stderr,"use: nashfunc x y xfile yfile dfile\n");
		fprintf(stderr,"The contents of xfile, yfile, and file should be simply: v1,v2,q1,q2\n");
		return 1;
	}

	X = atoi(argv[1]);
	Y = atoi(argv[2]);
	x = params_load(argv[3]);
	y = params_load(argv[4]);
	d = params_load(argv[5]);

	double b1 = 1;
	double b2 = 2;
	double th1 = 1-1.0/X;
	double th2 = 1-1.0/Y; 

	double bestnash = 1000000000000.0;
	double bestnash1 = 1000000000000.0;
	double bestnash2 = 1000000000000.0;
	double bestq1 = 0;
	double bestq2 = 0;

	double q1, q2, nash1, nash2, nash;
	int i,j;
	const int size = 1000;

	for(i=0;i<size;i++) {
		for(j=0;j<size;j++) {

			q1 = (double)i/size;
			q2 = (double)j/size;
    
			nash1 =   4 - 12*pow(q1,2) - 8*pow(q1,3) + 4*q2 - 8*q1*q2 - 28*pow(q1,2)*q2 - 12*pow(q1,3)*q2 - 4*pow(q2,2) - 16*q1*pow(q2,2) - 18*pow(q1,2)*pow(q2,2) - 4*pow(q1,3)*pow(q2,2) - 4*pow(q2,3) - 8*q1*pow(q2,3) - 2*pow(q1,2)*pow(q2,3) - 4*b1*q1*th1 - 8*b1*pow(q1,2)*th1 - 4*b1*pow(q1,3)*th1 - 8*b1*q1*q2*th1 - 15*b1*pow(q1,2)*q2*th1 - 6*b1*pow(q1,3)*q2*th1 - 4*b1*q1*pow(q2,2)*th1 - 7*b1*pow(q1,2)*pow(q2,2)*th1 - 2*b1*pow(q1,3)*pow(q2,2)*th1 - 2*d->v1 - q2*d->v1 - 2*q2*y->v1 - pow(q2,2)*y->v1 + 2*x->v1 + 2*q2*x->v1;

			nash2 =   4 + 4*q1 - 4*pow(q1,2) - 4*pow(q1,3) - 8*q1*q2 - 16*pow(q1,2)*q2 - 8*pow(q1,3)*q2 - 12*pow(q2,2) - 28*q1*pow(q2,2) - 18*pow(q1,2)*pow(q2,2) - 2*pow(q1,3)*pow(q2,2) - 8*pow(q2,3) - 12*q1*pow(q2,3) - 4*pow(q1,2)*pow(q2,3) - 4*b2*q2*th2 - 8*b2*q1*q2*th2 - 4*b2*pow(q1,2)*q2*th2 - 8*b2*pow(q2,2)*th2 - 15*b2*q1*pow(q2,2)*th2 - 7*b2*pow(q1,2)*pow(q2,2)*th2 - 4*b2*pow(q2,3)*th2 - 6*b2*q1*pow(q2,3)*th2 - 2*b2*pow(q1,2)*pow(q2,3)*th2 - 2*d->v2 - q1*d->v2 + 2*y->v2 + 2*q1*y->v2 - 2*q1*x->v2 - pow(q1,2)*x->v2;

			nash = nash1*nash1 + nash2*nash2;

			if(nash < bestnash) {	
				bestnash = nash;
				bestnash1 = nash1;
				bestnash2 = nash2;
				bestq1 = q1;
				bestq2 = q2;
			}
		}
	}

	printf("%lf,%lf,%lf,%lf\n",X,Y,bestnash1,bestnash2,bestq1,bestq2);

	return 0;
}
