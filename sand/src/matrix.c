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
	m->data = malloc(sizeof(struct cell) * (m->width+1) * (height+1) );

	return m;
}

void matrix_delete( struct matrix *m )
{
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
			char t = matrix(m,i,j).traceback;
			if(t==0) t=' ';
			printf("%3d%c ",matrix(m,i,j).score,t);
		}
		printf("\n");
	}

}

/* vim: set noexpandtab tabstop=4: */
