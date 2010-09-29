/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <sys/time.h>
#include <math.h>

#include "int_sizes.h"
#include "align.h"

#include "compressed_sequence.h"

#define TIME time(0) - start_time
#define KB_PER_SEQUENCE 9  // Defined by running some tests, not very exact.

typedef UINT64_T mer_t;

struct candidate_s
{
	unsigned int cand1;
	unsigned int cand2;
	char dir;
	short loc1;
	short loc2;
};
typedef struct candidate_s candidate_t;

struct minimizer_s
{
	mer_t mer;
	mer_t value;
	short loc;
	char dir;
};
typedef struct minimizer_s minimizer;

void load_mer_table();
void load_mer_table_subset(int start_x, int end_x, int start_y, int end_y, int is_same_rect);
void generate_candidates();
candidate_t * retrieve_candidates(int * total_cand);
int output_candidate_list(FILE * file, candidate_t * list, int total_output );
void free_mer_table();
void free_cand_table();
void init_cand_table( int buckets );
void init_mer_table(int buckets );
int init_repeat_mer_table(FILE * repeats, unsigned long buckets, int max_mer_repeat);
int load_seqs(FILE * input );
int load_seqs_two_files(FILE * f1, int * end1, FILE * f2, int * end2);
void rearrange_seqs_for_dist_framework();
void test_mers();
void print_mer_table(FILE * file);
void set_k(int new_k);
void set_window_size(int new_size);
int get_next_minimizer(int seq_num, minimizer * next_minimizer);
void print_kmer(FILE * file, mer_t mer);
void translate_kmer(mer_t mer, char * str, int length);

extern int rectangle_size;
extern int curr_rect_x;
extern int curr_rect_y;
extern unsigned long total_cand;

