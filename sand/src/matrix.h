/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MATRIX_H
#define MATRIX_H

struct s_cell
{
	int score;
	char tb;
};

typedef struct s_cell cell;

struct matrix {
	int width;
	int height;
	cell **data;
};

struct matrix * matrix_create( int width, int height );
void            matrix_delete( struct matrix *m );
void            matrix_print( struct matrix *m, const char *a, const char *b );

#endif

