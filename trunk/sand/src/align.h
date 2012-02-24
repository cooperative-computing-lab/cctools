/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __SEQUENCE_ALIGNMENT_H_
#define __SEQUENCE_ALIGNMENT_H_

#include <stdio.h>

#include "matrix.h"

struct alignment {
	int start1;
	int end1;
	int length1;
	int start2;
	int end2;
	int length2;
	char * traceback;
	int gap_count;
	int mismatch_count;
	int score;
	double quality;
	char ori;
};

struct alignment * align_prefix_suffix( struct matrix *m, const char *a, const char *b, int min_align );
struct alignment * align_smith_waterman( struct matrix *m, const char *a, const char *b );
struct alignment * align_banded( struct matrix *m, const char *a, const char *b, int astart, int bstart, int k );

void alignment_print( FILE * file, const char * str1, const char * str2, struct alignment *a );
void alignment_delete( struct alignment *a );

#endif
