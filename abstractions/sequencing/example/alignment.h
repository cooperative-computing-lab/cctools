#include <sys/time.h>
#include <stdio.h>
//#include <string.h>
//#include <math.h>

#ifndef __ALIGNMENT_H_
#define __ALIGNMENT_H_

#define TB_STR_1_PREFIX 1
#define TB_STR_2_PREFIX 2
#define MAX_STRING 102048

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
void print_alignment(FILE * file, const char * str1, const char * str2, delta tb, int line_width);
void print_delta(FILE * file, delta tb, const char * id1, const char * id2, int seq2_dir);
void print_OVL_message(FILE * file, delta tb, const char * id1, const char * id2);
void free_delta(delta tb);
void revcomp(seq * s);
void print_sequence(FILE * file, seq s);
void free_seq(seq s);
float benchmark(FILE * file, const char * message);
seq get_next_sequence(FILE * file);


#endif  // __ALIGNMENT_H_
