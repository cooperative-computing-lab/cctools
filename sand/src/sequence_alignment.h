/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <sys/time.h>
#include <stdio.h>
//#include <string.h>
//#include <math.h>

#ifndef __SEQUENCE_ALIGNMENT_H_
#define __SEQUENCE_ALIGNMENT_H_

#define TB_STR_1_PREFIX 1
#define TB_STR_2_PREFIX 2
#define MAX_STRING 102048

#ifndef MIN  // Don't conflict with another implementation of this (like in cctools macros.h)
#define MIN(x,y) ( ((x) < (y)) ? (x) : (y) )
#endif

#ifndef MAX  // Don't conflict with another implementation of this (like in cctools macros.h)
#define MAX(x,y) ( ((x) > (y)) ? (x) : (y) )
#endif

struct s_seq
{
	char * id;
	char * seq;
	char * metadata;
	int length;
};

typedef struct s_seq seq;

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

struct s_cell
{
	int score;
	int tb;
};

typedef struct s_cell cell;

delta prefix_suffix_align(const char * str1, const char * str2, int min_align);
delta sw_align(const char * str1, const char * str2);
delta local_align(const char * str1, const char * str2);
delta banded_prefix_suffix(const char * str1, const char * str2, int start1, int start2, int k);
void print_alignment(FILE * file, const char * str1, const char * str2, delta tb, int line_width);
void print_local(FILE * file, const char * str1, const char * str2, delta tb, int line_width);
void print_delta(FILE * file, delta tb, const char * id1, const char * id2);
void print_OVL_message(FILE * file, delta tb, const char * id1, const char * id2);
void print_OVL_envelope_start(FILE * file);
void print_OVL_envelope_end(FILE * file);
void free_delta(delta tb);
void revcomp(seq * s);
void print_sequence(FILE * file, seq s);
size_t sprint_seq(char * buf, seq s);
void free_seq(seq s);
float benchmark(FILE * file, const char * message);
seq get_next_sequence(FILE * file);
seq get_next_sequence_clip(FILE * file); 
seq get_next_sequence_internal(FILE * file); 
int max_alignment_length(int length1, int length2, int start1, int start2);
int sequence_count(FILE * file);


#endif  // __SEQUENCE_ALIGNMENT_H_
