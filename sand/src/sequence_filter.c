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
#include "itable.h"

#define EVEN_MASK 0xCCCCCCCCCCCCCCCCULL
#define ODD_MASK  0x3333333333333333ULL

unsigned short short_masks[8] = { 65535, 16383, 4095, 1023, 255, 63, 15, 3 };

#define SHORT_MASK_8 65535
#define SHORT_MASK_7 16383
#define SHORT_MASK_6 4095
#define SHORT_MASK_5 1023
#define SHORT_MASK_4 255
#define SHORT_MASK_3 63
#define SHORT_MASK_2 15
#define SHORT_MASK_1 3

static int k = 22;
static mer_t k_mask = 0;
static int WINDOW_SIZE = 22;
static mer_t repeat_mask = 0;

#define MER_VALUE(mer) ( (mer & (EVEN_MASK & k_mask)) | ((~mer) & (ODD_MASK & k_mask)) )

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

/* These are globals referenced directly by sand_filter_mer_seq. */
/* This needs to be cleaned up. */

int curr_rect_x = 0;
int curr_rect_y = 0;
int rectangle_size = 1000;
unsigned long total_cand = 0;

static int MER_TABLE_BUCKETS = 5000011; //20000003;
static int CAND_TABLE_BUCKETS= 5000011; //20000003;
static struct cseq ** all_seqs = 0;
static cand_list_element ** candidates;
static mer_hash_element ** mer_table;
static struct itable * repeat_mer_table = 0;
static int num_seqs = 0;

static int start_x = 0;
static int end_x = 0;
static int start_y = 0;
static int end_y = 0;
static int same_rect;

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
void mer_generate_cands(mer_hash_element * mhe);
void print_8mer(unsigned short mer);
void set_k_mask();
mer_t make_mer(const char * str);

void add_candidate(int seq, int cand, char dir, mer_t mer, short loc1, short loc2);
void free_cand_list_element(cand_list_element * cle);

void find_all_kmers(int seq_num);
void find_minimizers(int seq_num);
minimizer get_minimizer(int seq_num, int window_start);
mer_t get_kmer(struct cseq *c, int i);
void print_16mer(mer_t mer16);
mer_t rev_comp_mer(mer_t mer);

void print_mhe(FILE * file, mer_hash_element * mhe);

void init_cand_table( int buckets )
{
	int i;
	CAND_TABLE_BUCKETS = buckets;
	candidates = malloc(CAND_TABLE_BUCKETS * sizeof(cand_list_element *));
	for (i=0; i < CAND_TABLE_BUCKETS; i++)
	{
		candidates[i] = 0;
	}
}

void init_mer_table( int buckets )
{
	int i;
	MER_TABLE_BUCKETS = buckets;
	mer_table = malloc(MER_TABLE_BUCKETS * sizeof(mer_hash_element *));
	for (i=0; i < MER_TABLE_BUCKETS; i++)
	{
		mer_table[i] = 0;
	}
}

int load_seqs(FILE * input )
{
	int seq_count = sequence_count(input);

	all_seqs = malloc(seq_count*sizeof(struct cseq *));
	struct cseq *c;

	while ((c=cseq_read(input))) {
		all_seqs[num_seqs++] = c;
	}

	return num_seqs;

}

int load_seqs_two_files(FILE * f1, int * end1, FILE * f2, int * end2)
{
	num_seqs = 0;
	int count1 = sequence_count(f1);
	int count2 = sequence_count(f2);

	all_seqs = malloc((count1+count2)*sizeof(struct cseq *));
	struct cseq *c;

	while((c = cseq_read(f1))) {
		all_seqs[num_seqs++] = c;
	}

	*end1 = num_seqs;

	while((c = cseq_read(f2))) {
		all_seqs[num_seqs++] = c;
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

	struct cseq ** top = malloc(rectangle_size*(sizeof(struct cseq *)));
	struct cseq ** bottom = malloc(rectangle_size*(sizeof(struct cseq*)));

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

int compare_cand(const void * pair1, const void * pair2)
{

	// If c1.cand1 < c2.cand1, return a negative number, meaning less than.
	// If c1.cand1 and c2.cand1 are equal, check the second one.
	int diff = ((candidate_t *) pair1)->cand1 - ((candidate_t *) pair2)->cand1;
	if (!diff) return ((candidate_t *) pair1)->cand2 - ((candidate_t *) pair2)->cand2;
	return diff;
}

int output_candidate_list( FILE * file, candidate_t * list, int total_output )
{
	if (!list) return 0;

	int i;
	int total_printed = 0;

	for (i=0; i<total_output; i++)
	{
		candidate_t pair = list[i];
		fprintf(file, "%s\t%s\t%d\t%d\t%d\n", all_seqs[pair.cand1]->name, all_seqs[pair.cand2]->name, pair.dir, pair.loc1, (pair.dir == 1) ? pair.loc2 : all_seqs[pair.cand2]->num_bases - pair.loc2 - k);
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

void generate_candidates()
{
	int curr_bucket;

	mer_hash_element * mhe, *old_mhe;

	for(curr_bucket = 0; curr_bucket < MER_TABLE_BUCKETS; curr_bucket++)
	{
		mhe = mer_table[curr_bucket];
		while (mhe)
		{
			mer_generate_cands(mhe);
			old_mhe = mhe;
			mhe = mhe->next;
			free(old_mhe);
		}
		mer_table[curr_bucket] = 0;
	}
}

void mer_generate_cands(mer_hash_element * mhe)
{
	if (!mhe) return;

	mer_list_element *head, *curr;

	head = mhe->mle;

	while (head)
	{
		curr = head->next;
		while (curr)
		{
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
			fprintf(file, "%s\t%d\t%s\t%s\t%d\n", mer_str, mhe->count, all_seqs[head->seq_num]->name, all_seqs[curr->seq_num]->name, (int) (head->dir * curr->dir));
			curr = curr->next;
		}
		head = head->next;
	}
}

mer_t rev_comp_mer(mer_t mer)
{
	mer_t new_mer = 0;
	int i;
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
	int end = all_seqs[seq_num]->num_bases - 15;

	for (i = 0; i<end; i+=8)
	{
		mer16 = get_kmer(all_seqs[seq_num], i);
		add_sequence_to_mer(mer16, seq_num, (char) 1, (short) i);
	}
}

// Each time you move the window, you add a new kmer.
// Then do the following two things.
// 1. Check if the new kmer is less than the current minimizer.
//    If so, set it as the new one.
// 2. Is the current absolute minimizer now outside the window?
//    If so, check the window to find a NEW absolute minimizer, and add it.
void find_minimizers(int seq_num)
{
	int i;
	int end = all_seqs[seq_num]->num_bases - k + 1;
	mer_t mer, rev, mer_val, rev_val;

	minimizer window[WINDOW_SIZE];
	minimizer abs_min;
	int abs_min_index = 0;
	int index;
	int j;

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

		if (mer_val < rev_val)
		{
			window[i].mer = mer;
			window[i].value = mer_val;
			window[i].dir = 1;
			window[i].loc = i;
		}
		else
		{
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

	for (i = WINDOW_SIZE; i < end; i++)
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
			window[index].mer = mer;
			window[index].value = mer_val;
			window[index].dir = 1;
			window[index].loc = i;
		}
		else
		{
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
		}
	}
}

// Gets the next minimizer for this sequence.  Returns 1 if it worked, 0 if we're at the end of the sequence.
int get_next_minimizer(int seq_num, minimizer * next_minimizer )
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
		end = all_seqs[seq_num]->num_bases - k + 1;
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
				window[index].mer = mer;
				window[index].value = mer_val;
				window[index].dir = 1;
				window[index].loc = i;
			}
			else
			{
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
			window[index].mer = mer;
			window[index].value = mer_val;
			window[index].dir = 1;
			window[index].loc = i;
		}
		else
		{
			window[index].mer = rev;
			window[index].value = rev_val;
			window[index].dir = -1;
			window[index].loc = i;
		}

		// Now, check if the new k-mer is better than the current absolute minimizer.
		if (window[index].value < abs_min.value)
		{
			// If so, set it as the new absolute minimizer and add this sequence to the mer table
			abs_min = window[index];
			abs_min_index = index;
			*next_minimizer = abs_min;
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
			i++;   // Increment i so we won't process this k-mer again.
			return 1;
		}
	}

	// We made it to the end of the sequence without finding a new minimizer, so we are done.
	return 0;
}

mer_t get_kmer(struct cseq *c, int curr)
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
			curr_mer = c->data[which_mer] & short_masks[which_base];

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
			curr_mer = c->data[which_mer];

			int mercount = c->num_bases/8;
			if (c->num_bases%8 > 0) { mercount++; }

			if ((mercount-1) == which_mer) { curr_mer = curr_mer << ((8-(c->num_bases - (8*which_mer)))*2); }
			curr_mer = (curr_mer >> ((8 - (bases_left+which_base))*2 )) & short_masks[8-bases_left];

			// Now add it on to mer.
			mer = mer | curr_mer;

			//printf("%llu: ", mer);
			//print_kmer(stdout, mer);
			bases_left = 0;
		}
	}
	return mer;
}

void print_8mer(unsigned short mer)
{
	char str[9];
	int i;
	for (i=0; i<8; i++)
	{
		str[i] = num_to_base((mer >> ((8-1)-i)*2) & 3);
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
	}
	str[length] = '\0';
}


void add_sequence_to_mer(mer_t mer, int seq_num, char dir, short loc)
{
	// Store the sequence and its reverse complement as the same key.

	if (repeat_mer_table && itable_lookup(repeat_mer_table, mer)) return;

	mer_list_element * mle, * new_mle;

	// This will create it if it doesn't exist.
	mer_hash_element * mhe = get_mer_hash_element(mer);

	// Check that the list exists.
	if (!mhe->mle)
	{
		// If not, create it, increment the count and we're done.
		new_mle = create_mer_list_element(seq_num, dir, loc);
		mhe->mle = new_mle;
		mhe->count++;
		return;
	}

	// If a list does exist, add this sequence it.

	mle = mhe->mle;

	// Because we add one sequence at a time, this will be at the front
	// if it has ever had this mer before, so we don't add it twice.
	if (mle->seq_num == seq_num) return;

	new_mle = create_mer_list_element(seq_num, dir, loc);
	new_mle->next = mle;
	mhe->mle = new_mle;
	mhe->count++;
	
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

	// If there are no hash elements in this bucket
	// Add a new one.
	if (!mer_table[bucket])
	{
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
	if (!mhe) {
		mer_hash_element * new_mhe = malloc(sizeof(*new_mhe));
		new_mhe->mer = mer;
		new_mhe->count = 0;
		new_mhe->mle = 0;
		new_mhe->next = mer_table[bucket];
		mer_table[bucket] = new_mhe;
		return new_mhe;
	} else {
		return mhe;
	}
}

mer_hash_element * find_mer(mer_t mer)
{
	int bucket = mer % MER_TABLE_BUCKETS;

	mer_hash_element * mhe = mer_table[bucket];

	while (mhe)
	{
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

void load_mer_table()
{
	int curr_col, end_col, curr_row, end_row;
	curr_col = curr_rect_x*rectangle_size;
	end_col = curr_col+rectangle_size;
	if (end_col > num_seqs) { end_col = num_seqs; }

	curr_row = curr_rect_y*rectangle_size;
	end_row = curr_row+rectangle_size;
	if (end_row > num_seqs) { end_row = num_seqs; }

	load_mer_table_subset(curr_col, end_col, curr_row, end_row, (curr_rect_x == curr_rect_y));
}

void load_mer_table_subset(int curr_col, int end_col, int curr_row, int end_row, int is_same_rect)
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

	for ( ;	curr_col < end_col; curr_col++ )
	{
		find_minimizers(curr_col);
	}

	// If we are on the diagonal, don't need to add both, because they are the same.
	if (is_same_rect) { return; }

	for ( ;	curr_row < end_row; curr_row++ )
	{
		find_minimizers(curr_row);
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
	// Unless this is a diagonal, ones from the same block have already been compared.
	// If I don't do this step, then ones from the same block on the same axis
	// could get compared, because we don't really distinguish them.

	if (!should_compare_cands(seq, cand)) return;

	int index = (seq*cand*499);
	if (index < 0) { index *= -1; }
	index = index % CAND_TABLE_BUCKETS;

	cand_list_element * cle = candidates[index];
	// There are no candidate pairs in this bucket
	if (!cle)
	{
		candidates[index] = create_new_cle(seq, cand, dir, loc1, loc2);

		total_cand++;
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
	cand_list_element * new_cle = create_new_cle(seq, cand, dir, loc1, loc2);
	new_cle->next = candidates[index];
	candidates[index] = new_cle;
	total_cand++;
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

