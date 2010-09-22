/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <sys/time.h>
#include <math.h>
#include <limits.h>
#include <stdarg.h>
#include <stdlib.h>
#include "sequence_filter.h"

// This has to be prime
#define MAX_MER_REPEAT 25

#define EVEN_MASK 0xCCCCCCCCCCCCCCCCULL
#define ODD_MASK  0x3333333333333333ULL

unsigned short short_masks[8] = { 65535, 16383, 4095, 1023, 255, 63, 15, 3 };

//unsigned short short_masks[8] = { 0, 3, 15, 63, 255, 1023, 4095, 16383 };
#define SHORT_MASK_8 65535
#define SHORT_MASK_7 16383
#define SHORT_MASK_6 4095
#define SHORT_MASK_5 1023
#define SHORT_MASK_4 255
#define SHORT_MASK_3 63
#define SHORT_MASK_2 15
#define SHORT_MASK_1 3

#define TIME time(0) - start_time
int start_time = 0;

int k = 22;
mer_t k_mask = 0;
int WINDOW_SIZE = 22;
mer_t repeat_mask = 0;

#define MER_VALUE(mer) ( (mer & (EVEN_MASK & k_mask)) | ((~mer) & (ODD_MASK & k_mask)) )

#define SEQ_ID(seq_num) ( all_seqs[seq_num].ext_id )


struct mer_list_elem_s
{
	int seq_num;
	short loc;
	char dir;
	struct mer_list_elem_s * next;
};

typedef struct mer_list_elem_s mer_list_element;

struct mer_hash_element_s
{
	mer_t mer;
	mer_list_element * mle;
	unsigned char count;
	struct mer_hash_element_s * next;
};
typedef struct mer_hash_element_s mer_hash_element;

struct cand_list_element_s
{
	int cand1;
	int cand2;
	char dir;
	int count;
	short loc1;
	short loc2;
	struct cand_list_element_s * next;
};
typedef struct cand_list_element_s cand_list_element;

//cseq all_seqs[NUM_SEQUENCES];
cseq * all_seqs = 0;
//cand_list_element ** candidates_x;
//cand_list_element ** candidates_y;
int MER_TABLE_BUCKETS = 5000011; //20000003;
int CAND_TABLE_BUCKETS= 5000011; //20000003;
//cand_list_element * candidates[CAND_TABLE_BUCKETS];
//mer_hash_element * mer_table[MER_TABLE_BUCKETS];
cand_list_element ** candidates;
mer_hash_element ** mer_table;
struct itable * repeat_mer_table = 0;
int rectangle_size = 1000;
int num_seqs = 0;

int curr_rect_x = 0;
int curr_rect_y = 0;

static int start_x = 0;
static int end_x = 0;
static int start_y = 0;
static int end_y = 0;
static int same_rect;

unsigned long total_cand = 0;

#define is_x(x) ( (x >= (curr_rect_x*rectangle_size)) && (x < ((curr_rect_x+1)*rectangle_size)) )
#define is_y(y) ( (y >= (curr_rect_y*rectangle_size)) && (y < ((curr_rect_y+1)*rectangle_size)) )
#define in_range(x, s, e) (  ((s) <= (x)) && ((x) < (e))   )

void add_sequence_to_mer(mer_t mer, int i, char dir, short loc);
mer_hash_element * find_mer(mer_t mer);
void free_mer_table();
void free_mer_hash_element(mer_hash_element * mhe);
void free_mer_table();
void free_cand_table();
mer_list_element * create_mer_list_element(int seq_num, char dir, short loc);
mer_hash_element * get_mer_hash_element(mer_t mer);
void free_mer_list_element(mer_list_element * mle);
void mer_generate_cands(mer_hash_element * mhe, int verbose_level);
void print_8mer(unsigned short mer);
void set_k_mask();
mer_t make_mer(const char * str);

void add_candidate(int seq, int cand, char dir, mer_t mer, short loc1, short loc2);
void free_cand_list_element(cand_list_element * cle);

void find_all_kmers(int seq_num);
void find_minimizers(int seq_num, int verbose_level);
minimizer get_minimizer(int seq_num, int window_start);
mer_t get_kmer(cseq c, int i);
void print_16mer(mer_t mer16);
mer_t rev_comp_mer(mer_t mer);

void print_mhe(FILE * file, mer_hash_element * mhe);

void init_cand_table()
{
	int i;
	//candidates = calloc(CAND_TABLE_BUCKETS, sizeof(cand_list_element *));
	candidates = malloc(CAND_TABLE_BUCKETS * sizeof(cand_list_element *));
	for (i=0; i < CAND_TABLE_BUCKETS; i++)
	{
		candidates[i] = 0;
	}
}

void init_mer_table()
{
	int i;
	//mer_table = calloc(MER_TABLE_BUCKETS, sizeof(mer_hash_element *));
	mer_table = malloc(MER_TABLE_BUCKETS * sizeof(mer_hash_element *));
	for (i=0; i < MER_TABLE_BUCKETS; i++)
	{
		mer_table[i] = 0;
	}
}

cseq * get_seq_array()
{
	return all_seqs;
}

int get_seq_count()
{
	return num_seqs;
}

void set_seq_array(cseq * array, int size)
{
	all_seqs = array;
	num_seqs = size;
}

int load_seqs(FILE * input, int seq_count)
{
	if (seq_count <= 0)
	{
		seq_count = sequence_count(input);
	}
	all_seqs = malloc(seq_count*sizeof(cseq));
	cseq c;

	while (!feof(input))
	{
		c = get_next_cseq(input);
		if (!c.metadata) continue;

		all_seqs[num_seqs] = c;
		
		num_seqs++;
	}

	return num_seqs;

}

int load_seqs_two_files(FILE * f1, int count1, int * end1, FILE * f2, int count2, int * end2)
{
	num_seqs = 0;
	if (count1 == 0)
	{
		count1 = sequence_count(f1);
	}
	if (count2 == 0)
	{
		count2 = sequence_count(f2);
	}
	all_seqs = malloc((count1+count2)*sizeof(cseq));
	cseq c;

	while (!feof(f1))
	{
		c = get_next_cseq(f1);
		if (!c.metadata) continue;

		all_seqs[num_seqs] = c;
		
		num_seqs++;
	}

	*end1 = num_seqs;

	cseq_file_reset();

	while (!feof(f2))
	{
		c = get_next_cseq(f2);
		if (!c.metadata) continue;

		all_seqs[num_seqs] = c;
		
		num_seqs++;
	}
	*end2 = num_seqs;

	return num_seqs;

}

// Loads up a set of mers from a file and stores them.
// These mers cannot be treated as minimizers.
int init_repeat_mer_table(FILE * repeats, unsigned long buckets, int max_mer_repeat)
{
	// Estimate the number of buckets here by dividing the file
	// size by 25.
	if (buckets == 0)
	{
		unsigned long curr_pos = ftell(repeats);
		fseek(repeats, 0, SEEK_END);
		unsigned long end_pos = ftell(repeats);
		buckets = (end_pos - curr_pos) / 25;
		fseek(repeats, curr_pos, SEEK_SET);
	}

	char str[1024];
	int *count = malloc(sizeof(int));;
	mer_t mer, rev;
	repeat_mer_table = itable_create(buckets);
	if (k_mask == 0) set_k_mask();

	while (fscanf(repeats, ">%d %s\n", count, str) == 2)
	{
		if (*count >= max_mer_repeat)
		{
			mer = make_mer(str);
			rev = rev_comp_mer(mer);
			if (MER_VALUE(mer) < MER_VALUE(rev))
				itable_insert(repeat_mer_table, mer, count);
			else
				itable_insert(repeat_mer_table, rev, count);
		}
		count = malloc(sizeof(int));
	}

	return itable_size(repeat_mer_table);
}

mer_t make_mer(const char * str)
{
	mer_t mer = 0;
	int i;
	int len = strlen(str);
	for (i=0; i<len; i++)
	{
		mer = (mer << 2) | base_to_num(str[i]);
	}
	return mer;
}

void rearrange_seqs_for_dist_framework()
{
	int i;

	cseq * top = malloc(rectangle_size*(sizeof(cseq)));
	cseq * bottom = malloc(rectangle_size*(sizeof(cseq)));
	for (i=0; i < num_seqs; i+=2)
	{
		top[i/2] = all_seqs[i];
		bottom[i/2] = all_seqs[i+1];
	}
	for (i=0; i<rectangle_size; i++)
	{
		all_seqs[i] = top[i];
		all_seqs[i+rectangle_size] = bottom[i];
	}
	free(top);
	free(bottom);
}

int output_candidates(FILE * file, int format)
{
	int curr_index;
	int total_output = 0;

	cand_list_element * cle;
	candidate_t curr_cand;

	for (curr_index = 0; curr_index < CAND_TABLE_BUCKETS; curr_index++)
	{
		cle = candidates[curr_index];	
		while (cle)
		{
			if (cle->count >= 1) 
			{
				if (format == CANDIDATE_FORMAT_OVL)
				{
					fprintf(file, "{OVL\nafr:%s\nbfr:%s\nori:%c\nolt:D\nahg:0\nbhg:0\nqua:0.000000\nmno:0\nmxo:0\npct:0\n}\n", all_seqs[cle->cand1].ext_id, all_seqs[cle->cand2].ext_id, (cle->dir == 1) ? 'N' : 'I');
				}
				else if (format == CANDIDATE_FORMAT_LINE)
				{
					fprintf(file, "%s\t%s\t%d\t%d\t%d\n", all_seqs[cle->cand1].ext_id, all_seqs[cle->cand2].ext_id, cle->dir, cle->loc1, (cle->dir == 1) ? cle->loc2 : all_seqs[cle->cand2].length - cle->loc2 - k);
				}
				else if (format == CANDIDATE_FORMAT_BINARY)
				{
					curr_cand.cand1 = atoi(all_seqs[cle->cand1].ext_id);
					curr_cand.cand2 = atoi(all_seqs[cle->cand2].ext_id);
					curr_cand.dir = cle->dir;
					curr_cand.loc1 = cle->loc1;
					curr_cand.loc2 = (cle->dir == 1) ? cle->loc2 : all_seqs[cle->cand2].length - cle->loc2 - k;
					fwrite(&curr_cand, sizeof(struct candidate_s), 1, file);
				}
				//printf("%s\t%s\t%d\n", all_seqs[cle->cand1].ext_id, all_seqs[cle->cand2].ext_id, cle->dir);
				total_output++;
			}
			cle = cle->next;
		}
		free_cand_list_element(candidates[curr_index]);
		candidates[curr_index] = 0;
	}

	return total_output;

}


int compare_cand(const void * pair1, const void * pair2)
{

	// If c1.cand1 < c2.cand1, return a negative number, meaning less than.
	// If c1.cand1 and c2.cand1 are equal, check the second one.
	int diff = ((candidate_t *) pair1)->cand1 - ((candidate_t *) pair2)->cand1;
	if (!diff) return ((candidate_t *) pair1)->cand2 - ((candidate_t *) pair2)->cand2;
	return diff;
	
}

int output_candidate_list(FILE * file, candidate_t * list, int total_output, int format)
{
	if (!list) return 0;

	int i;
	int total_printed = 0;

	for (i=0; i<total_output; i++)
	{
		candidate_t pair = list[i];
		if (format == CANDIDATE_FORMAT_OVL)
		{
			fprintf(file, "{OVL\nafr:%s\nbfr:%s\nori:%c\nolt:D\nahg:0\nbhg:0\nqua:0.000000\nmno:0\nmxo:0\npct:0\n}\n", all_seqs[pair.cand1].ext_id, all_seqs[pair.cand2].ext_id, (pair.dir == 1) ? 'N' : 'I');
		}
		else if (format == CANDIDATE_FORMAT_LINE)
		{
			fprintf(file, "%s\t%s\t%d\t%d\t%d\n", all_seqs[pair.cand1].ext_id, all_seqs[pair.cand2].ext_id, pair.dir, pair.loc1, (pair.dir == 1) ? pair.loc2 : all_seqs[pair.cand2].length - pair.loc2 - k);
		}
		else if (format == CANDIDATE_FORMAT_BINARY)
		{
			pair.loc2 = (pair.dir == 1) ? pair.loc2 : all_seqs[pair.cand2].length - pair.loc2 - k;
			pair.cand1 = atoi(all_seqs[pair.cand1].ext_id);
			pair.cand2 = atoi(all_seqs[pair.cand2].ext_id);
			fwrite(&pair, sizeof(struct candidate_s), 1, file);
		}
		total_printed++;
	}
	return total_printed;
}

candidate_t * retrieve_candidates(int * total_cand_ret)
{
	int curr_index;
	int total_output = 0;

	cand_list_element * cle;

	candidate_t * candidate_list = malloc(total_cand*sizeof(struct candidate_s));

	for (curr_index = 0; curr_index < CAND_TABLE_BUCKETS; curr_index++)
	{
		cle = candidates[curr_index];	
		while (cle)
		{
			if (cle->count >= 1) 
			{
				candidate_list[total_output].cand1 = cle->cand1;
				candidate_list[total_output].cand2 = cle->cand2;
				candidate_list[total_output].dir = cle->dir;
				candidate_list[total_output].loc1 = cle->loc1;
				candidate_list[total_output].loc2 = cle->loc2;
				total_output++;
			}
			cle = cle->next;
		}
		free_cand_list_element(candidates[curr_index]);
		candidates[curr_index] = 0;
	}

	*total_cand_ret = total_output;
	qsort(candidate_list, total_output, sizeof(struct candidate_s), compare_cand);
	return candidate_list;

}

void generate_candidates(int verbose_level)
{
	int curr_bucket;

	mer_hash_element * mhe, *old_mhe;

	for(curr_bucket = 0; curr_bucket < MER_TABLE_BUCKETS; curr_bucket++)
	{
		mhe = mer_table[curr_bucket];
		//printf("Bucket %d\n", curr_bucket);
		while (mhe)
		{
			//if (mhe->mer == 1073318718) { fprintf(stderr, "Processing mer %u with count %d:\n", mhe->mer, mhe->count); print_16mer(mhe->mer); }
			mer_generate_cands(mhe, verbose_level);
			//if (mhe->count <= MAX_MER_REPEAT) mer_generate_cands(mhe, verbose_level);
			//else if (verbose_level >= 2) { fprintf(stderr, "Mer %ux was repeated %d times: ", mhe->mer, mhe->count); print_kmer(stderr, mhe->mer); }
			//else { fprintf(stderr, "Mer %010llx was repeated %d times: ", mhe->mer, mhe->count); print_kmer(stderr, mhe->mer); }
			old_mhe = mhe;
			mhe = mhe->next;
			free(old_mhe);
		}
		mer_table[curr_bucket] = 0;
	}
}

void mer_generate_cands(mer_hash_element * mhe, int verbose_level)
{
	if (!mhe) return;

	mer_list_element *head, *curr;

	head = mhe->mle;

	// int debug = 0; //if (mhe->mer == 1073318718) debug = 1;

	//printf("%d sequences\n", mhe->count);

	while (head)
	{
		curr = head->next;
		while (curr)
		{
			if (verbose_level >= 1)
			{
				char mer_str[k+1];
				translate_kmer(mhe->mer, mer_str, k);
				fprintf(stderr, "%s (%u): %s\t%s\t%d\t%d\t%d\n", mer_str, (unsigned) mhe->mer, all_seqs[head->seq_num].ext_id, all_seqs[curr->seq_num].ext_id, (int) (head->dir * curr->dir), head->loc, curr->loc);
			}
			add_candidate(head->seq_num, curr->seq_num, head->dir * curr->dir, mhe->mer, head->loc, curr->loc);
			curr = curr->next;
		}
		head = head->next;
	}

	free_mer_list_element(mhe->mle);
	mhe->mle = 0;
}

void print_mer_table(FILE * file)
{
	int curr_bucket;
	mer_hash_element * mhe, *old_mhe;
	for (curr_bucket = 0; curr_bucket < MER_TABLE_BUCKETS; curr_bucket++)
	{
		mhe = mer_table[curr_bucket];
		while (mhe)
		{
			print_mhe(file, mhe);
			old_mhe = mhe;
			mhe = mhe->next;
		}
	}
}

void print_mhe(FILE * file, mer_hash_element * mhe)
{
	if (!mhe) return;

	mer_list_element *head, *curr;
	head = mhe->mle;
	char mer_str[k+1];

	while(head)
	{
		curr = head->next;
		while (curr)
		{
			translate_kmer(mhe->mer, mer_str, k);
			fprintf(file, "%s\t%d\t%s\t%s\t%d\n", mer_str, mhe->count, all_seqs[head->seq_num].ext_id, all_seqs[curr->seq_num].ext_id, (int) (head->dir * curr->dir));
			curr = curr->next;
		}
		head = head->next;
	}
}

mer_t rev_comp_mer(mer_t mer)
{
	mer_t new_mer = 0;
	int i;
	//if (mer == 0) { fprintf(stderr, "Rev comping (k=%d): ", k); print_kmer(stderr, mer); }
	for (i = 0; i < k; i++)
	{
		// Build new_mer by basically popping off the LSB of mer (mer >> 2)
		// and pushing to the LSB of new_mer.
		new_mer = new_mer << 2;
		new_mer = new_mer | (mer & 3);
		mer = mer >> 2;
	}
	// Now it's reversed, so complement it, but mask it by k_mask so only the important bits get complemented.
	return (~new_mer) & k_mask;
}


void find_all_kmers(int seq_num)
{
	mer_t mer16;
	int i;
	int end = all_seqs[seq_num].length - 15;


	//seq s = uncompress_seq(all_seqs[seq_num]); printf("Processing sequence %d (%s):\n", seq_num, all_seqs[seq_num].ext_id); print_sequence(stdout, s);
	for (i = 0; i<end; i+=8)
	{
		mer16 = get_kmer(all_seqs[seq_num], i);
		//printf("Adding to mer (%u): ", mer16); print_16mer(mer16);
		add_sequence_to_mer(mer16, seq_num, (char) 1, (short) i);
		//add_sequence_to_mer(rev_comp_mer(mer16), seq_num, (char) -1);

		// Debug stuff
		//mer_hash_element * mhe = find_mer(mer16);
		//if (!mhe) { printf("ERROR: found no mhe\n"); }
		//else if (!mhe->mle) { printf("ERROR: found no mle\n"); }
		//else { printf("Current sequence: %s, found sequence: %s\n", all_seqs[seq_num].ext_id, all_seqs[mhe->mle->seq_num].ext_id); }
	}
}

// Each time you move the window, you add a new kmer.
// Then do the following two things.
// 1. Check if the new kmer is less than the current minimizer.
//    If so, set it as the new one.
// 2. Is the current absolute minimizer now outside the window?
//    If so, check the window to find a NEW absolute minimizer, and add it.
void find_minimizers(int seq_num, int verbose_level)
{
	int i;
	int end = all_seqs[seq_num].length - k + 1;
	mer_t mer, rev, mer_val, rev_val;

	minimizer window[WINDOW_SIZE];
	minimizer abs_min;
	int abs_min_index = 0;
	int index;
	int j;

	// int debug = 0;
	//if ((strcmp(SEQ_ID(seq_num), "1101751653708") == 0) || (strcmp(SEQ_ID(seq_num), "1101751885812") == 0))	debug = 1;
	if (verbose_level >= 1) { fprintf(stderr, "Processing %s\n", all_seqs[seq_num].ext_id); seq s=uncompress_seq(all_seqs[seq_num]); print_sequence(stderr, s); }

	memset(&abs_min,0,sizeof(abs_min));
	abs_min.value = ULONG_MAX;
	abs_min.dir = 0;

	// First, just populate the first window and get the first minimizer.
	for (i = 0; i < WINDOW_SIZE; i++)
	{
		mer = get_kmer(all_seqs[seq_num], i);
		rev = rev_comp_mer(mer);
		mer_val = MER_VALUE(mer);
		rev_val = MER_VALUE(rev);

		//if (debug) { fprintf(stderr, "Dealing with kmer (%13u): ", mer); print_kmer(stderr, mer); }
		//if (debug) { fprintf(stderr, "Reverse           (%13u): ", rev); print_kmer(stderr, rev); }

		if (mer_val < rev_val)
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding forward kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, mer); }
			window[i].mer = mer;
			window[i].value = mer_val;
			window[i].dir = 1;
			window[i].loc = i;
		}
		else
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding reverse kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, rev); }
			window[i].mer = rev;
			window[i].value = rev_val;
			window[i].dir = -1;
			window[i].loc = i;
		}
		if (window[i].value < abs_min.value)
		{
			abs_min = window[i];
			abs_min_index = i;
		}
	}

	// Add the absolute minimizer for the first window.
	add_sequence_to_mer(abs_min.mer, seq_num, abs_min.dir, abs_min.loc);
	if (verbose_level >= 1) { fprintf(stderr, "Adding new minimizer (init) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }

	for (i = WINDOW_SIZE; i < end; i++)
	{
		index = i%WINDOW_SIZE;

		// First, add the new k-mer to the window, evicting the k-mer that is
		// no longer in the window
		mer = get_kmer(all_seqs[seq_num], i);
		rev = rev_comp_mer(mer);
		mer_val = MER_VALUE(mer);
		rev_val = MER_VALUE(rev);
		//if (debug) { fprintf(stderr, "Dealing with kmer (%13u): ", mer); print_kmer(stderr, mer); }
		//if (debug) { fprintf(stderr, "Reverse           (%13u): ", rev); print_kmer(stderr, rev); }

		if (mer_val < rev_val)
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding forward kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, mer); }
			window[index].mer = mer;
			window[index].value = mer_val;
			window[index].dir = 1;
			window[index].loc = i;
		}
		else
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding reverse kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, rev); }
			window[index].mer = rev;
			window[index].value = rev_val;
			window[index].dir = -1;
			window[index].loc = i;
		}

		// Now, check if the new k-mer is better than the current absolute minimizer.
		//if (window[index].value < abs_min.value)
		if (window[index].value < abs_min.value)
		{
			// If so, set it as the new absolute minimizer and add this sequence to the mer table
			abs_min = window[index];
			abs_min_index = index;
			add_sequence_to_mer(abs_min.mer, seq_num, abs_min.dir, abs_min.loc);
			if (verbose_level >= 1) { fprintf(stderr, "Adding new minimizer (better) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }
		}
		// Now, check if the current absolute minimizer is out of the window
		// We just replaced index, so if abs_min_index == index, we just evicted
		// the current minimizer.
		else if (abs_min_index == index)
		{
			// Find the new minimizer
			// If runtime starts to suffer I can implement something better than a linear search,
			// but because WINDOW_SIZE is a small constant (around 20) it should be OK.
			abs_min.value = ULONG_MAX;
			abs_min.dir = 0;
			for (j = 0; j < WINDOW_SIZE; j++)
			{
				if (window[j].value < abs_min.value)
				{
					abs_min = window[j];
					abs_min_index = j;
				}
			}
			// Add the new current minimizer to the mer table.
			add_sequence_to_mer(abs_min.mer, seq_num, abs_min.dir, abs_min.loc);
			if (verbose_level >= 1) { fprintf(stderr, "Adding new minimizer (eviction) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }
		}
	}
}

// Gets the next minimizer for this sequence.  Returns 1 if it worked, 0 if we're at the end of the sequence.
int get_next_minimizer(int seq_num, minimizer * next_minimizer, int verbose_level)
{
	static int i = 0;
	static int index = 0;
	static minimizer * window = 0;
	static minimizer abs_min = {0, ULONG_MAX, -1, 0};
	static int abs_min_index = 0;
	static int prev_seq_num = -1;
	static int end;

	mer_t mer, rev, mer_val, rev_val;

	// Re-initialize everything if the sequence we are using changes.
	if (seq_num != prev_seq_num)
	{
		if (!window) window = malloc(WINDOW_SIZE*sizeof(minimizer));
		i = 0;
		index = 0;
		abs_min.mer = 0;
		abs_min.value = ULONG_MAX;
		abs_min.dir = 0;
		abs_min.loc = -1;
		abs_min_index = 0;
		end = all_seqs[seq_num].length - k + 1;
		prev_seq_num = seq_num;
	}

	// If we haven't populated the whole window yet, do so now.
	if (i == 0)
	{
		index = i;
		for (i = 0; i < WINDOW_SIZE; i++)
		{
			// Get the current mer, its reverse complement, and its minimizer values.
			mer = get_kmer(all_seqs[seq_num], i);
			rev = rev_comp_mer(mer);
			mer_val = MER_VALUE(mer);
			rev_val = MER_VALUE(rev);

			// Add them to the window.

			if (mer_val < rev_val)
			{
				if (verbose_level >= 2) { fprintf(stderr, "Adding forward kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, mer); }
				window[index].mer = mer;
				window[index].value = mer_val;
				window[index].dir = 1;
				window[index].loc = i;
			}
			else
			{
				if (verbose_level >= 2) { fprintf(stderr, "Adding reverse kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, rev); }
				window[index].mer = rev;
				window[index].value = rev_val;
				window[index].dir = -1;
				window[index].loc = i;
			}
			if (window[index].value < abs_min.value)
			{
				abs_min = window[index];
				abs_min_index = index;
			}
		}

		// Now, return the minimizer we found (it will be the first minimizer for the list)
		*next_minimizer = abs_min;
		if (verbose_level >= 1) { fprintf(stderr, "Found minimizer (init) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }

		return 1;
	}

	// Otherwise, we can just continue on from the current position of i.
	for ( ; i < end; i++)
	{
		index = i%WINDOW_SIZE;

		// First, add the new k-mer to the window, evicting the k-mer that is
		// no longer in the window
		mer = get_kmer(all_seqs[seq_num], i);
		rev = rev_comp_mer(mer);
		mer_val = MER_VALUE(mer);
		rev_val = MER_VALUE(rev);

		if (mer_val < rev_val)
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding forward kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, mer); }
			window[index].mer = mer;
			window[index].value = mer_val;
			window[index].dir = 1;
			window[index].loc = i;
		}
		else
		{
			if (verbose_level >= 2) { fprintf(stderr, "Adding reverse kmer %2d (%010llx/%010llx): ", i, MER_VALUE(mer), MER_VALUE(rev)); print_kmer(stderr, rev); }
			window[index].mer = rev;
			window[index].value = rev_val;
			window[index].dir = -1;
			window[index].loc = i;
		}

		// Now, check if the new k-mer is better than the current absolute minimizer.
		//if (window[index].value < abs_min.value)
		if (window[index].value < abs_min.value)
		{
			// If so, set it as the new absolute minimizer and add this sequence to the mer table
			abs_min = window[index];
			abs_min_index = index;
			*next_minimizer = abs_min;
			if (verbose_level >= 1) { fprintf(stderr, "Found new minimizer (better) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }
			i++;  // Increment i so we won't process this k-mer again.
			return 1;
		}
		// Now, check if the current absolute minimizer is out of the window
		// We just replaced index, so if abs_min_index == index, we just evicted
		// the current minimizer.
		else if (abs_min_index == index)
		{
			// Find the new minimizer
			// If runtime starts to suffer I can implement something better than a linear search,
			// but because WINDOW_SIZE is a small constant (around 20) it should be OK.
			int j;
			abs_min.value = ULONG_MAX;
			abs_min.dir = 0;
			for (j = 0; j < WINDOW_SIZE; j++)
			{
				if (window[j].value < abs_min.value)
				{
					abs_min = window[j];
					abs_min_index = j;
				}
			}
			// Add the new current minimizer to the mer table.
			*next_minimizer = abs_min;
			if (verbose_level >= 1) { fprintf(stderr, "Found new minimizer (eviction) (%010llx, %d): ", MER_VALUE(abs_min.mer), abs_min.dir); print_kmer(stderr, abs_min.mer); }
			i++;   // Increment i so we won't process this k-mer again.
			return 1;
		}
	}

	// We made it to the end of the sequence without finding a new minimizer, so we are done.
	return 0;
}

/*
void find_minimizers(int seq_num)
{
	int i;
	int end = all_seqs[seq_num].length - k + 1;
	minimizer min;

	int debug = 0;
	if ((strcmp(SEQ_ID(seq_num), "1101671397550") == 0) || (strcmp(SEQ_ID(seq_num), "1101854204516") == 0))	debug = 1;
	if (debug) fprintf(stderr, "Processing %s\n", all_seqs[seq_num].ext_id);

	for (i = 0; i < end; i+= WINDOW_SIZE)
	{
		min = get_minimizer(seq_num, i);
		if (debug) { fprintf(stderr, "Adding minimizer for window [%d, %d] (%u): ", i, i+WINDOW_SIZE-1, min.mer); print_16mer(min.mer); }
		add_sequence_to_mer(min.mer, seq_num, min.dir);
	}
}

minimizer get_minimizer(int seq_num, int window_start)
{
	int i;
	mer_t curr_mer;
	mer_t curr_mer_val;
	mer_t min_mer = UINT_MAX;
	mer_t min_mer_val = UINT_MAX;
	minimizer min;


	for (i = window_start; i < window_start + WINDOW_SIZE; i++)
	{
		curr_mer = get_kmer(all_seqs[seq_num], i);
		curr_mer_val = MER_VALUE(curr_mer);
		if (curr_mer_val < min_mer_val)
		{
			min_mer_val = curr_mer_val;
			min.mer = curr_mer;
			min.dir = 1;
		}

		curr_mer = rev_comp_mer(curr_mer);
		if (curr_mer_val < min_mer_val)
		{
			min_mer_val = curr_mer_val;
			min.mer = curr_mer;
			min.dir = -1;
		}
	}
	return min;
}
*/

void test_mers()
{
	cseq c;

	int i;

	set_k_mask();

	c = all_seqs[0];
	seq s = uncompress_seq(c);
	revcomp(&s);
	print_sequence(stdout, s);

	int end = c.length - k + 1;
	mer_t mer;

	for (i = 0; i<end; i++)
	{
		mer = rev_comp_mer(get_kmer(c, i));
		printf("%13llu: ", (long long unsigned int)mer);
		print_kmer(stdout, mer);
	}
}

mer_t get_kmer(cseq c, int curr)
{

	// Which mer does this kmer start in? 
	int which_mer = curr/8;
	int which_base = curr%8;
	unsigned short curr_mer = 0;
	mer_t mer = 0;

	int bases_left = k;

	// Start from the first base and push k bases.
	while (bases_left > 0)
	{
		// We can fit the rest of this short inside mer, so do it.
		if ((bases_left >= 8) || (bases_left > (8-which_base)))
		{
			// Mask out everything before which_base
			curr_mer = c.mers[which_mer] & short_masks[which_base];

			//printf("(1)curr_mer: %hu, mask: %hu, mer : ", curr_mer, short_masks[which_base]); print_8mer(curr_mer);
			//mer_t test = (((mer_t) curr_mer) & (mer_t) 65535) << 48;
			//char str[9]; translate_kmer(test , str, 8);
			//printf("%llu, bases_left: %d, which_base: %d, which_mer: %d, curr_mer: %s\n", test, bases_left, which_base, which_mer, str);

			// Push mer so that there is enough space for curr_mer
			mer = mer << ( (8-which_base)*2 );

			// Add curr_mer onto mer.
			mer = mer | (mer_t) curr_mer;

			//printf("%llu: ", mer);
			//print_kmer(stdout, mer);
			bases_left -= (8 - which_base);
			which_mer++;
			which_base = 0;
		}
		// Now we're down to the last few bases and we need to handle overflow.
		else
		{
			// The bases in this short will be enough

			// Make enough room for the rest of the bases
			mer = mer << bases_left*2;

			// Shift the curr mer to the end and mask it out.
			curr_mer = c.mers[which_mer];
			if ((c.mercount-1) == which_mer) { curr_mer = curr_mer << ((8-(c.length - (8*which_mer)))*2); }
			curr_mer = (curr_mer >> ((8 - (bases_left+which_base))*2 )) & short_masks[8-bases_left];

			//printf("(2)curr_mer: %hu, mask: %hu, mer : ", curr_mer, short_masks[which_base]); print_8mer(curr_mer);
			//mer_t test = (((mer_t) c.mers[which_mer]) & (mer_t) 65535) << 48;
			//char str[9]; translate_kmer(test , str, 8);
			//printf("%llu, bases_left: %d, which_base: %d, which_mer: %d, curr_mer: %s\n", test, bases_left, which_base, which_mer, str);

			// Now add it on to mer.
			mer = mer | curr_mer;

			//printf("%llu: ", mer);
			//print_kmer(stdout, mer);
			bases_left = 0;
		}
	}
	//printf("DONE!\n");
	return mer;
	// NEED TO TEST THIS PART OF THE CODE, DON'T JUST TRY TO RUN THE WHOLE PROGRAM!!!
/*
	//fprintf(stdout, "curr: %d, which_mer: %d, which_base: %d\n", curr, which_mer, which_base);
	unsigned short first_mer = c.mers[which_mer];
	unsigned short second_mer = c.mers[which_mer+1];
	unsigned short third_mer = 0;
	if (which_base > 0)
	{
		third_mer = c.mers[which_mer+2];
	}

	// Move the first base to the MSB and put the MSB of the
	// second mer in the LSB of the first mer.
	//fprintf(stdout, "first_mer before: %d\n", first_mer);
	first_mer = (first_mer << (which_base*2)) | (second_mer >> ((8-which_base)*2));
	//fprintf(stdout, "first_mer after: %d\n", first_mer);

	// Do the same with the second mer.
	if (which_mer+2 < c.mercount-1)
	{
		//printf(", using method a\n");
		second_mer = (second_mer << (which_base*2)) | (third_mer >> ((8-which_base)*2));
	}
	else
	{
		//printf(", using method b\n");
		second_mer = (second_mer << (which_base*2)) | third_mer;
	}

	// Return the final value.
	return ((mer_t)first_mer << 16) | (mer_t)second_mer;	
*/
}

void print_8mer(unsigned short mer)
{
	char str[9];
	int i;
	for (i=0; i<8; i++)
	{
		str[i] = num_to_base((mer >> ((8-1)-i)*2) & 3);
		//fprintf(stderr, "i:%d, mer >> %i & 3: %d\n", i, (length-1)-i, (mer >> (length-1)-i) & 3);
	}
	str[8] = '\0';

	printf("%s\n", str);
}

void print_16mer(mer_t mer16)
{
	char str[17];

	translate_to_str(mer16, str, 16);
	fprintf(stderr, "%s\n", str);
}

void print_kmer(FILE * file, mer_t mer)
{
	char str[k+1];

	translate_kmer(mer, str, k);
	fprintf(file, "%s\n", str);
}

void translate_kmer(mer_t mer, char * str, int length)
{
	//print_mer(stderr, mer);
	int i;

	//int int_len = sizeof(mer_t)*8;

	// 2 bits represent each base. So, to get the first base, we the
	// two most significant bits. To get the second base, the two second
	// most significant bits, etc. In other, we need to bit shift all but
	// 2 (aka bitshift 14 to the right) when i = 0, bitshift 12 when i=1,
	// etc.
	// We mask by 3 to make sure we only have the two numbers and nothing
	// but 0's in the rest.
	for (i=0; i<length; i++)
	{
		str[i] = num_to_base((mer >> ((length-1)-i)*2) & 3);
		//fprintf(stderr, "i:%d, mer >> %i & 3: %d\n", i, (length-1)-i, (mer >> (length-1)-i) & 3);
	}
	str[length] = '\0';
}


void add_sequence_to_mer(mer_t mer, int seq_num, char dir, short loc)
{

	// Store the sequence and its reverse complement as the same key.
	//mer_t mer_key;
	//mer_t rev = rev_comp_mer(mer);
	//printf("mer: %u, rev: %u, mer_key: %u, dir: %d\n", mer, rev, mer_key, dir);
	//if (mer < rev) { mer_key = mer; dir = 1; }
	//else { mer_key = rev; dir = -1; }

	//printf("add_sequence_to_mer %u (%d): ", mer, (int) dir); print_16mer(mer);
	if (repeat_mer_table && itable_lookup(repeat_mer_table, mer))
	{
		//fprintf(stderr, "Filtering out mer %010llx due to high count.\n");
		return;
	}

	mer_list_element * mle, * new_mle;

	// This will create it if it doesn't exist.
	//mer_hash_element * mhe = get_mer_hash_element(mer_key);
	mer_hash_element * mhe = get_mer_hash_element(mer);

	// Don't deal with mers that are repeated more than MAX_MER_REPEAT times
	//if (mhe->count > MAX_MER_REPEAT) return; 

	// Check that the list exists.
	if (!mhe->mle)
	{
		// If not, create it, increment the count and we're done.
		new_mle = create_mer_list_element(seq_num, dir, loc);
		mhe->mle = new_mle;
		mhe->count++;
		//printf("Just created this mhe, mer has %d sequences\n", mhe->count);
		return;
	}

	// If a list does exist, add this sequence it.

	mle = mhe->mle;

	// Because we add one sequence at a time, this will be at the front
	// if it has ever had this mer before, so we don't add it twice.
	if (mle->seq_num == seq_num) return;

	// Create a new list element
	new_mle = create_mer_list_element(seq_num, dir, loc);
	// Put the front of the list after the new one
	new_mle->next = mle;
	// Make the new one the front of the list.
	mhe->mle = new_mle;
	// Increment count.
	mhe->count++;
	//printf("this mer now has %d sequences\n", mhe->count);
	
}

mer_list_element * create_mer_list_element(int seq_num, char dir, short loc)
{
	mer_list_element * new_mle = malloc(sizeof(*new_mle));
	new_mle->seq_num = seq_num;
	new_mle->dir = dir;
	new_mle->loc = loc;
	new_mle->next = 0;

	return new_mle;
}

mer_hash_element * get_mer_hash_element(mer_t mer)
{
	int bucket = mer % MER_TABLE_BUCKETS;
	mer_hash_element * mhe;

	//printf("Calling get_mer_hash_element for %u: ", mer); print_16mer(mer);

	// If there are no hash elements in this bucket
	// Add a new one.
	if (!mer_table[bucket])
	{
	//printf("Nothing in the bucket, so creating one\n");
		mhe = malloc(sizeof(*mhe));
		mhe->mer = mer;
		mhe->count = 0;
		mhe->mle = 0;
		mhe->next = 0;
		mer_table[bucket] = mhe;
		return mhe;
	}

	mhe = find_mer(mer);

	// If this bucket is not empty, but does not contain this mer, add it.
	if (!mhe)
	{
		//printf("Found a bucket, but no match\n");
		mer_hash_element * new_mhe = malloc(sizeof(*new_mhe));
		new_mhe->mer = mer;
		new_mhe->count = 0;
		new_mhe->mle = 0;
		new_mhe->next = mer_table[bucket];
		mer_table[bucket] = new_mhe;
		return new_mhe;
	}
	else
	{
		//printf("Found a match\n");
		return mhe;
	}
}

mer_hash_element * find_mer(mer_t mer)
{
	int bucket = mer % MER_TABLE_BUCKETS;

	mer_hash_element * mhe = mer_table[bucket];

	while (mhe)
	{
		//printf("find_mer: mhe->mer: %u\n", mhe->mer);
		if (mhe->mer == mer) { return mhe; }
		mhe = mhe->next;
	}

	return 0;
}

void free_mer_table()
{
	int curr_mhe = 0;

	for (; curr_mhe < MER_TABLE_BUCKETS; curr_mhe++)
	{
		free_mer_hash_element(mer_table[curr_mhe]);
	}
}

void free_mer_hash_element(mer_hash_element * mhe)
{
	if (!mhe) return;

	free_mer_list_element(mhe->mle);
	mer_hash_element * n = mhe->next;
	free(mhe);
	free_mer_hash_element(n);
}

void free_mer_list_element(mer_list_element * mle)
{
	if (!mle) return;

	mer_list_element * n = mle->next;
	free(mle);
	free_mer_list_element(n);
}

void set_k(int new_k)
{
	k = new_k;
	set_k_mask();
}

void set_window_size(int new_size)
{
	WINDOW_SIZE = new_size;
}

void set_k_mask()
{
	int i;

	k_mask = 0;
	for (i=0; i<k; i++)
	{
		// Push it over two bits and or by binary 11
		// This amounts to pushing two 1's onto the right side.
		k_mask = (k_mask << 2) | 3;
	}
}

void load_mer_table(int verbose_level)
{
	int curr_col, end_col, curr_row, end_row;
	curr_col = curr_rect_x*rectangle_size;
	end_col = curr_col+rectangle_size;
	if (end_col > num_seqs) { end_col = num_seqs; }

	curr_row = curr_rect_y*rectangle_size;
	end_row = curr_row+rectangle_size;
	if (end_row > num_seqs) { end_row = num_seqs; }

	load_mer_table_subset(verbose_level, curr_col, end_col, curr_row, end_row, (curr_rect_x == curr_rect_y));
}

void load_mer_table_subset(int verbose_level, int curr_col, int end_col, int curr_row, int end_row, int is_same_rect)
{

	mer_t repeat_mask_rev;

	if (k_mask == 0) { set_k_mask(); }
	repeat_mask_rev = rev_comp_mer(repeat_mask);
	if (MER_VALUE(repeat_mask_rev) < MER_VALUE(repeat_mask)) repeat_mask = repeat_mask_rev;

	start_x = curr_col;
	end_x = end_col;
	start_y = curr_row;
	end_y = end_row;
	same_rect = is_same_rect;

	// This is an imaginary matrix, but we're loading all the sequences
	// on a given rectangle, defined by curr_rect_x, curr_rect_y and rectangle_size.
	// Load the mers in each of these sequences, then we'll output any matches.

	//printf("Set 1:\n");
	for ( ;	curr_col < end_col; curr_col++ )
	{
		//printf(" %s\n", all_seqs[curr_col].ext_id);
		//find_all_kmers(curr_col);
		// Ignore any sequences named "dummyX". These are there to make the matrix square.
		//if (strncmp(all_seqs[curr_col].ext_id, "dummy", 5) == 0) { continue; }
		find_minimizers(curr_col, verbose_level);
	}

	// If we are on the diagonal, don't need to add both, because they are
	// the same.
	if (is_same_rect) { return; }

	//printf("Set 2\n");
	for ( ;	curr_row < end_row; curr_row++ )
	{
		//printf(" %s\n", all_seqs[curr_col].ext_id);
		//find_all_kmers(curr_row);
		// Ignore any sequences named "dummyX". These are there to make the matrix square.
		//if (strncmp(all_seqs[curr_row].ext_id, "dummy", 5) == 0) { continue; }
		find_minimizers(curr_row, verbose_level);
	}
}

cand_list_element * create_new_cle(int seq1, int seq2, int dir, int loc1, int loc2)
{
	cand_list_element * new_cle = malloc(sizeof(*new_cle));
	if (seq1 < seq2)
	{
		new_cle->cand1 = seq1;
		new_cle->cand2 = seq2;
		new_cle->loc1 = loc1;
		new_cle->loc2 = loc2;
	}
	else
	{
		new_cle->cand2 = seq1;
		new_cle->cand1 = seq2;
		new_cle->loc2 = loc1;
		new_cle->loc1 = loc2;
	}
	new_cle->dir = dir;
	new_cle->next = 0;
	new_cle->count = 1;

	return new_cle;
}

int should_compare_cands(int c1, int c2)
{
	// If the two rectangles are equal, then we are intended to compare
	// two from the same rectangle, so return 1.
	if (same_rect) { return 1; }

	// Otherwise, return false if they are in the same rectangle,
	// true otherwise.
	if (in_range(c1, start_x, end_x) && in_range(c2, start_x, end_x)) {  return 0; }
	if (in_range(c1, start_y, end_y) && in_range(c2, start_y, end_y)) { return 0; }

	return 1;
}

void add_candidate(int seq, int cand, char dir, mer_t min, short loc1, short loc2)
{
	//printf("Adding cand %s %s %d\n%d: %d %d; %d: %d %d\n", all_seqs[seq].ext_id, all_seqs[cand].ext_id, dir, seq, is_x(seq), is_y(seq), cand, is_x(cand), is_y(cand));
	// Unless this is a diagonal, ones from the same block have already been compared.
	// If I don't do this step, then ones from the same block on the same axis
	// could get compared, because we don't really distinguish them.
	int debug = 0; //if (min == 1073318718) { debug = 1; fprintf(stderr, "adding candidate %s %s %d\n",all_seqs[seq].ext_id, all_seqs[cand].ext_id, dir); }

	//if ( (curr_rect_x != curr_rect_y) && ( (is_x(seq) && is_x(cand)) || (is_y(seq) && is_y(cand)) ) ) { return; }
	if (!should_compare_cands(seq, cand)) return;
	//printf("Added it!\n");

	if (debug) fprintf(stderr, "OK\n");
	int index = (seq*cand*499);
	if (index < 0) { index *= -1; }
	index = index % CAND_TABLE_BUCKETS;

	cand_list_element * cle = candidates[index];
	// There are no candidate pairs in this bucket
	if (!cle)
	{
		/*
		cand_list_element * new_cle = malloc(sizeof(*new_cle));
		new_cle->cand1 = seq;
		new_cle->cand2 = cand;
		new_cle->dir = dir;
		new_cle->next = 0;
		new_cle->count = 1;
		new_cle->loc1 = loc1;
		new_cle->loc2 = loc2;
		*/
		candidates[index] = create_new_cle(seq, cand, dir, loc1, loc2);

		total_cand++;
		//printf("{OVL\nafr:%s\nbfr:%s\nori:%c\nolt:D\nahg:0\nbhg:0\nqua:0.000000\nmno:0\nmxo:0\npct:0\n}\n", all_seqs[seq].ext_id, all_seqs[cand].ext_id, (dir == 1) ? 'N' : 'I');
	//printf("%s\t%s\t%d\t", all_seqs[seq].ext_id, all_seqs[cand].ext_id, dir); print_16mer(min);
		return;
	}

	// This bucket has candidate pairs, if ours is one of them just leave
	// because we've already printed it out.
	while (cle)
	{
		if ((cle->cand1 == seq) && (cle->cand2 == cand) && (cle->dir == dir))
		{
			cle->count++;
			return;
		}
		else if ((cle->cand2 == seq) && (cle->cand1 == cand) && (cle->dir == dir))
		{
			cle->count++;
			return;
		}
		cle = cle->next;
	}

	// If we made it this far, we did not find this candidate pair, so add it.
	/*
	cand_list_element * new_cle = malloc(sizeof(*new_cle));
	new_cle->cand1 = seq;
	new_cle->cand2 = cand;
	new_cle->dir = dir;
	new_cle->count = 1;
	new_cle->loc1 = loc1;
	new_cle->loc2 = loc2;
	*/
	cand_list_element * new_cle = create_new_cle(seq, cand, dir, loc1, loc2);
	new_cle->next = candidates[index];
	candidates[index] = new_cle;
	total_cand++;
	//printf("{OVL\nafr:%s\nbfr:%s\nori:%c\nolt:D\nahg:0\nbhg:0\nqua:0.000000\nmno:0\nmxo:0\npct:0\n}\n", all_seqs[seq].ext_id, all_seqs[cand].ext_id, (dir == 1) ? 'N' : 'I');
	//printf("%s\t%s\t%d\t", all_seqs[seq].ext_id, all_seqs[cand].ext_id, dir); print_16mer(min);
	return;

}

void free_cand_table()
{
	int i;

	for (i = 0; i < CAND_TABLE_BUCKETS; i++)
	{
		free_cand_list_element(candidates[i]);
		candidates[i] = 0;
	}
}

void free_cand_list_element(cand_list_element * cle)
{
	if (!cle) return;

	cand_list_element * n = cle->next;
	free(cle);
	free_cand_list_element(n);
}

unsigned long get_mem_avail()
{
	UINT64_T total, avail;
	memory_info_get(&total, &avail);
	return (unsigned long) avail/1024;
}

unsigned long get_mem_usage()
{
	char cmd[512];
	unsigned int rss;
	int pid;
	FILE *stat = fopen("/proc/self/stat", "r");
	fscanf(stat, "%d", &pid);
	fclose(stat);
	sprintf(cmd, "ps -p %d -o rss | tail -1", pid);
	//printf("cmd: %s\n", cmd);
	FILE *fp = popen(cmd, "r");
	//fgets(line, 512, fp);
	//printf(line);
	//fgets(line, 512, fp);
	//printf(line);
	fscanf(fp, "%d\n", &rss);
	//printf("rss: %d\n", rss);
	pclose(fp);
	return rss;
}
