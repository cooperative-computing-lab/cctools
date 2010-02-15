#include <stdio.h>
#include <sys/time.h>
#include <math.h>
#include "sequence_alignment.h"
#include "sequence_compression.h"
#include "sand_align_macros.h"

//#include "int_hash.h"
#include "itable.h"
#include "memory_info.h"

#define TIME time(0) - start_time
//#define NUM_SEQUENCES 1801182
#define KB_PER_SEQUENCE 9  // Defined by running some tests, not very exact.

#define CANDIDATE_FORMAT_OVL 1
#define CANDIDATE_FORMAT_LINE 2
#define CANDIDATE_FORMAT_BINARY 3

typedef unsigned long mer_t;

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

void load_mer_table(int verbose_level);
void load_mer_table_subset(int verbose_level, int start_x, int end_x, int start_y, int end_y, int is_same_rect);
void generate_candidates(int verbose_level);
candidate_t * retrieve_candidates(int * total_cand);
int output_candidates(FILE * file, int format);
int output_candidate_list(FILE * file, candidate_t * list, int total_output, int format);
void free_mer_table();
void free_cand_table();
void init_cand_table();
void init_mer_table();
int init_repeat_mer_table(FILE * repeats, unsigned long buckets, int max_mer_repeat);
int load_seqs(FILE * input, int sequence_count);
int load_seqs_two_files(FILE * f1, int count1, int * end1, FILE * f2, int count2, int * end2);
void set_seq_array(cseq * array, int size);
cseq * get_seq_array();
int get_seq_count();
void rearrange_seqs_for_dist_framework();
void test_mers();
void print_mer_table(FILE * file);
void set_k(int new_k);
void set_window_size(int new_size);
unsigned long get_mem_usage();
unsigned long get_mem_avail();
int get_next_minimizer(int seq_num, minimizer * next_minimizer, int verbose_level);
void print_kmer(FILE * file, mer_t mer);
void translate_kmer(mer_t mer, char * str, int length);
