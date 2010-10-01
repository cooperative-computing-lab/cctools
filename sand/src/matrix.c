/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "matrix.h"

struct matrix * matrix_create( int width, int height )
{
	struct matrix *m = malloc(sizeof(*m));

	m->width = width;
	m->height = height;
	m->data = malloc(sizeof(cell*)*(height+1));

	int j;

	for(j=0;j<=m->height;j++) {
		m->data[j] = malloc(sizeof(cell)*(width+1));
	}

	return m;
}

void matrix_delete( struct matrix *m )
{
	int j;
	for(j=0; j<=m->height; j++) free(m->data[j]);
	free(m->data);
	free(m);
}

void matrix_print( struct matrix *m, const char *a, const char *b )
{
	int i,j;

	if(b) printf("     ");

	if(a) for(i=0;i<m->width;i++) printf("    %c",a[i]);

	printf("\n");

	for(j=0;j<=m->height;j++) {
		if(b) {
			if(j>0) {
				printf("%c ",b[j-1]);
			} else {
				printf("  ");
			}
		}

		for(i=0;i<=m->width;i++) {
			char t = m->data[j][i].tb;
			if(t==0) t=' ';
			printf("%3d%c ",m->data[j][i].score,t);
		}
		printf("\n");
	}

}
