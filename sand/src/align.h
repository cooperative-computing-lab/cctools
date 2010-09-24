/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __SEQUENCE_ALIGNMENT_H_
#define __SEQUENCE_ALIGNMENT_H_

#include <sys/time.h>
#include <stdio.h>

#include "macros.h"

#define MAX_STRING 102048

struct s_delta
{
	int start1;
	int end1;
	int length1;
	int start2;
	int end2;
	int length2;
	int * tb;
	int gap_count;
	int mismatch_count;
	int score;
	int total_score;
	float quality;
	char ori;
};

typedef struct s_delta delta;

delta align_prefix_suffix(const char * str1, const char * str2, int min_align);
delta align_smith_waterman(const char * str1, const char * str2);
delta align_banded(const char * str1, const char * str2, int start1, int start2, int k);
int   align_max(int length1, int length2, int start1, int start2);

void delta_print_alignment(FILE * file, const char * str1, const char * str2, delta tb, int line_width);
void delta_print_local(FILE * file, const char * str1, const char * str2, delta tb, int line_width);
void delta_free(delta tb);


#endif  // __SEQUENCE_ALIGNMENT_H_
