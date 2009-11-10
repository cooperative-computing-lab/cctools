/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
//#include <cstring.h>
#include "alignment.h"
#include "align_macros.h"

#define TB_LEFT -1
#define TB_UP 1
#define TB_DIAG 0

#define MAX_ID 100
#define MAX_METADATA 100

#define INIT_PREFIX_SUFFIX 0
#define INIT_SW 1

int score_mismatch = 1;
int score_match_open = 0;
int score_match_extend = 0;
int score_gap_open = 1;
int score_gap_extend = 1;

void print_matrix(FILE* file, cell ** matrix, const char * str1, int length1, const char * str2, int length2);
cell new_score(cell ** matrix, int i, int j, const char * str1, const char * str2);
cell new_score_gap_extensions(cell ** matrix, int i, int j, const char * str1, const char * str2);
void choose_best(cell ** matrix, int * i, int * j, int length1, int length2, int min_align);
delta generate_traceback(cell ** matrix, int i, int j, int length1, int length2);
void convert_to_upper(char * str);
void chomp(char * str);
void process_string(char * str);
void seq_cat(seq * sequence, char * new_str);
cell ** init_matrix(int length1, int length2, int type);
void free_matrix(cell ** matrix, int length2);

delta sw_align(const char * str1, const char * str2)
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int i,j;
	delta tb;

#ifdef TRUNCATE
	length1 = length1/TRUNCATE;
	length2 = length2/TRUNCATE;
#endif


	cell ** matrix = init_matrix(length1, length2, INIT_SW);

	for (i=1; i<=length1; i++)
	{
		for (j=1; j<=length2; j++)
		{
			//matrix[j][i] = new_score(matrix, i, j, str1, str2);
			matrix[j][i] = new_score_gap_extensions(matrix, i, j, str1, str2);
		}
	}

	tb = generate_traceback(matrix, length1, length2, length1, length2);

	//print_matrix(stdout, matrix, str1, length1, str2, length2);
	//fprintf(stderr, "best row: %d, best column: %d\n", best_j, best_i);

	fflush(stdout);
	free_matrix(matrix, length2);

	return tb;
}

delta prefix_suffix_align(const char * str1, const char * str2, int min_align)
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int i,j;
	delta tb;

#ifdef TRUNCATE
	length1 = length1/TRUNCATE;
	length2 = length2/TRUNCATE;
#endif

	//fprintf(stderr, "init matrix\n");
	cell ** matrix = init_matrix(length1, length2, INIT_PREFIX_SUFFIX);
	//fprintf(stderr, "filling out matrix\n");
	for (i=1; i<=length1; i++)
	{
		for (j=1; j<=length2; j++)
		{
			//matrix[j][i] = new_score(matrix, i, j, str1, str2);
			matrix[j][i] = new_score_gap_extensions(matrix, i, j, str1, str2);
		}
	}
	//print_matrix(stdout, matrix, str1, length1, str2, length2);

	int best_i, best_j;
	choose_best(matrix, &best_i, &best_j, length1, length2, min_align);

	//fprintf(stdout, "best: %d, %d\n", best_i, best_j);

	tb = generate_traceback(matrix, best_i, best_j, length1, length2);

	//print_matrix(stdout, matrix, str1, length1, str2, length2);
	//fprintf(stderr, "best row: %d, best column: %d\n", best_j, best_i);

	fflush(stdout);
	free_matrix(matrix, length2);

	return tb;
}

cell ** init_matrix(int length1, int length2, int type)
{
	int i, j;

	// Create the list of rows, each itself an int *
	// Add 1 to make sure it has enough room for the
	// first row of 0's
#ifdef STATIC_MATRIX_SIZE
	static cell ** matrix = 0;
	if (matrix == 0)
	{
		matrix = malloc(STATIC_MATRIX_SIZE*sizeof(cell *));
		for(j=0; j<=MAX_LENGTH; j++)
		{
			matrix[j] = malloc(STATIC_MATRIX_SIZE*sizeof(cell));
		}
		
	}
#else
	cell ** matrix = malloc((length2+1)*sizeof(cell *));
	for(j=0; j<=length2; j++)
	{
		matrix[j] = malloc((length1+1)*sizeof(cell));
	}
#endif

	if (type == INIT_PREFIX_SUFFIX)
	{
		// Initialize the first row to all 0's
		for(i=0; i<=length1; i++)
		{
			matrix[0][i].score = 0;
			matrix[0][i].tb = 0;
		}
		// Initialize the first column to all 0's
		for(j=1; j<=length2; j++)
		{
			matrix[j][0].score = 0;
			matrix[j][0].tb = 0;
		}
	}
	else if (type == INIT_SW)
	{
		// Initialize the first row to all 0's
		matrix[0][0].score = 0;
		matrix[0][0].tb = 0;
		matrix[0][1].score = score_gap_open;
		matrix[0][1].tb = TB_LEFT;
		for(i=2; i<=length1; i++)
		{
			matrix[0][i].score = matrix[0][i-1].score+score_gap_extend;
			matrix[0][i].tb = TB_LEFT;
		}
		// Initialize the first column to all 0's
		matrix[1][0].score = score_gap_open;
		matrix[1][0].tb = TB_UP;
		for(j=1; j<=length2; j++)
		{
			matrix[j][0].score = matrix[j-1][0].score+score_gap_extend;
			matrix[j][0].tb = TB_UP;
		}
	}
	return matrix;
}


void free_matrix(cell ** matrix, int length2)
{
	int j;
#ifndef STATIC_MATRIX_SIZE
	// Free each row
	for(j=0; j<=length2; j++)
	{
		free(matrix[j]);
	}

	// Free the list of rows.
	free(matrix);
#endif
}

cell new_score(cell ** matrix, int i, int j, const char * str1, const char * str2)
{
	cell min;
	int incr;
	cell retval;

	min.score = 999999999;
	incr = (str1[i-1] == str2[j-1]) ? 0 : 1;
	if (matrix[j-1][i-1].score + incr < min.score)
	{
		min.score = matrix[j-1][i-1].score + incr;
		min.tb = TB_DIAG;
	}

	if (matrix[j-1][i].score+1 < min.score)
	{
		min.score = matrix[j-1][i].score+1;
		min.tb = TB_UP;
	}

	if (matrix[j][i-1].score+1 < min.score)
	{
		min.score = matrix[j][i-1].score+1;
		min.tb = TB_LEFT;
	}

	//printf("(%d, %d) %c %c = %d (%d, %d)\n", i, j, str1[i-1], str2[j-1], incr, min.score, min.tb);
	return min;
}

cell new_score_gap_extensions(cell ** matrix, int i, int j, const char * str1, const char * str2)
{
	cell min;
	int incr;
	cell retval;

	min.score = 999999999;

	// If this is a gap extension, don't increase the score.
	// If it's a gap open, increase the score.
	incr = (matrix[j-1][i].tb == TB_UP) ? score_gap_extend : score_gap_open;
	if (matrix[j-1][i].score+incr < min.score)
	{
		min.score = matrix[j-1][i].score+incr;
		min.tb = TB_UP;
	}

	// If this is a gap extension, don't increase the score.
	// If it's a gap open, increase the score.
	incr = (matrix[j][i-1].tb == TB_LEFT) ? score_gap_extend : score_gap_open;
	if (matrix[j][i-1].score+incr < min.score)
	{
		min.score = matrix[j][i-1].score+incr;
		min.tb = TB_LEFT;
	}

	if (str1[i] == str2[j])
	{
		if ((matrix[j-1][i-1].tb == TB_DIAG) && (str1[i-1] == str2[j-1]))
		{
			incr = score_match_extend;
		}
		else
		{
			incr = score_match_open;
		}
	}
	else
	{
		incr = score_mismatch;
	}
	if (matrix[j-1][i-1].score + incr < min.score)
	{
		min.score = matrix[j-1][i-1].score + incr;
		min.tb = TB_DIAG;
	}

	//printf("(%d, %d) %c %c = %d (%d, %d)\n", i, j, str1[i-1], str2[j-1], incr, min.score, min.tb);
	return min;
}

int min(int i, int j)
{
	if (i < j) { return i; }
	else { return j; }
}

void choose_best(cell ** matrix, int * best_i, int * best_j, int length1, int length2, int min_align)
{
	int i, j;
	int min_score = length1 + length2;
	float quality;
	float min_qual = (float) length1 + (float)length2;
	

	// Find the best in the last column
	for (i=length1, j=min_align; j <= length2; j++)
	{
		//fprintf(stdout, "%d, %d, %d, %d\n", i, j, matrix[j][i].score, matrix[j][i].score*2 - j);
		//if (matrix[j][i].score*2 - j < min_score)
		//{
		//	min_score = matrix[j][i].score*2 - j;
		quality = ((float) matrix[j][i].score) / (float)min(i, j);
		//fprintf(stdout, "quality (%d, %d): %d / %d = %f\n", i, j, matrix[j][i].score, min(i, j), quality);
		if (quality < min_qual)
		{
			//fprintf(stdout, "Setting min_qual (%d, %d): %d / %d = %f\n", i, j, matrix[j][i].score, min(i, j), quality);
			min_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}

	// Find the best in the last row
	for (i=min_align, j=length2; i <= length1; i++)
	{
		//fprintf(stdout, "%d, %d, %d, %d\n", i, j, matrix[j][i].score, matrix[j][i].score*2 - i);
		//if (matrix[j][i].score*2 - i < min_score)
		//{
		//	min_score = matrix[j][i].score*2 - i;
		quality = ((float) matrix[j][i].score) / (float)min(i, j);
		//fprintf(stdout, "quality (%d, %d): %d / %d = %f\n", i, j, matrix[j][i].score, min(i, j), quality);
		if (quality < min_qual)
		{
			//fprintf(stdout, "Setting min_qual (%d, %d): %d / %d = %f\n", i, j, matrix[j][i].score, min(i, j), quality);
			min_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}
	//fprintf(stdout, "best quality (%d, %d): %d / %d = %f\nscore: %d", *best_i, *best_j, matrix[*best_j][*best_i].score, min(*best_i, *best_j), min_qual);
}

delta generate_traceback(cell ** matrix, int i, int j, int length1, int length2)
{
	delta tb;
	int curr_i, curr_j, curr_gap_type, count;
	int first = 1, last_gap_type=0, count_since_last = 0;
	int temp_tb[MAX_STRING];


	// The return array requires 4 pieces of info
	// 1. Whether string 1 or string 2 is the string for which the alignment is a prefix.
	// 2. Which position in the prefix string the alignment starts at
	// 3. Which position in the suffix string the alignment ends at.
	// 4. The positions of the gaps.
	tb.end1 = i-1;
	tb.end2 = j-1;
	tb.mismatch_count = 0;

	curr_i = i;
	curr_j = j;
	curr_gap_type = 0;
	count = 0;

	while (curr_i > 0 && curr_j > 0)
	{
		curr_gap_type = matrix[curr_j][curr_i].tb;

		// If it's left, create a gap in the second sequence
		if (curr_gap_type != TB_DIAG)
		{
			// If this is the first gap we've seen, then
			// it's the last gap in the alignment.
			// We don't need to record the distance between
			// this gap and the previous one.
			if (first)
			{
				first = 0;
			}
			else
			{
				// If the last gap was a left gap, make it a positive gap (gap on str2)
				// If the last gap was an up gap, make it a negative gap (gap on str1)
				temp_tb[count++] = count_since_last * ( (last_gap_type == TB_LEFT) ? 1 : -1) ;
			}
			last_gap_type = curr_gap_type;
			count_since_last = 0;

			// Decrement either curr_i or curr_j, depending on which direction
			// the traceback is meant to go.
			(curr_gap_type == TB_LEFT) ? curr_i-- : curr_j--;
		}
		else
		{
			// See if it's a mismatch.
			if (matrix[curr_j][curr_i].score > matrix[curr_j-1][curr_i-1].score)
			{
				tb.mismatch_count++;
			}
			curr_i--;
			curr_j--;
		}

		count_since_last++;
	}

	temp_tb[count] = count_since_last * ( (last_gap_type == TB_LEFT) ? 1 : -1) ;

	int k;
	tb.tb = malloc((count+1)*sizeof(int));
	for (k=count; k >= 0; k--)
	{
		
		tb.tb[count-k] = temp_tb[k];
	}
	tb.start1 = curr_i;
	tb.start2 = curr_j;
	tb.gap_count = count+1;
	tb.length1 = length1;
	tb.length2 = length2;
	tb.score = matrix[j][i].score;
	tb.total_score = tb.score + (length1 - i) + curr_i + (length2 - j) + curr_j;
	tb.quality = (float)(tb.score) / (float) min(i, j);
	
	return tb;
}

void print_delta(FILE * file, delta tb, const char * id1, const char * id2, int seq2_dir)
{
	int i, start2, end2;
	if (seq2_dir == 1)
	{
		start2 = tb.start2;
		end2 = tb.end2;
	}
	else
	{
		start2 = tb.end2;
		end2 = tb.start2;
	}
	fprintf(file, ">%s %s %d %d\n%d %d %d %d %d %d %d %d\n", id1, id2, tb.length1, tb.length2, tb.start1, tb.end1, start2, end2, tb.mismatch_count, tb.gap_count, tb.score, tb.total_score);

	for(i=0; i<tb.gap_count; i++)
	{
		fprintf(file, "%d\n", tb.tb[i]);
	}
	fprintf(file, "0\n");
}

void print_OVL_message(FILE * file, delta tb, const char * id1, const char * id2)
{
	int ahg, bhg;
	char olt;

	fprintf(file, "{OVL\n");
	
	// IDs of overlapping fragments.
	fprintf(file, "afr:%s\n", id1);
	fprintf(file, "bfr:%s\n", id2);

	// Orientation
	fprintf(file, "ori:%c\n", tb.ori);

	ahg = tb.start1 - tb.start2;
	bhg = (tb.length2-1) - tb.end2;
	if (bhg == 0) { bhg = tb.end1 - tb.length1; }
	//fprintf(stderr, "tb.start1: %d, tb.end1: %d, tb.length1: %d\ntb.start2: %d, tb.end1: %d, tb.length2: %d\nahg: %d, bhg: %d\n", tb.start1, tb.end1, tb.length1, tb.start2, tb.end2, tb.length2, ahg, bhg);

	// If ahg and bhg are of opposite signs, then it's a containment.
	// If they are the same sign, it's a dovetail.
	if (ahg*bhg < 0) { olt = 'C'; }
	else { olt = 'D'; }
	//fprintf(file, "olt:%c\n", olt);
	fprintf(file, "olt:D\n");  // Always put D to mimic Celera more closely.

	// How much each piece hangs off the end. Not sure what to do
	// for containment overlaps, or really what this means at all.
	fprintf(file, "ahg:%d\n", ahg);
	fprintf(file, "bhg:%d\n", bhg);

	// Again, need to do more work to see how quality is computed.
	// For now just making something up.
	// As far as I can tell, Celera defines the quality score as
	// (gaps + mismatches) / min(end1, end2)
	fprintf(file, "qua:%f\n", tb.quality);

	// This seems to be pretty much meaningless in Celera, so leaving it as 0.
	// Scratch that, set it to the length of the overlap.
	fprintf(file, "mno:%d\n", min(tb.end1 - tb.start1, tb.end2 - tb.start2));
	fprintf(file, "mxo:%d\n", tb.score);

	// Polymorphism count.
	//fprintf(file, "pct:%d\n", tb.mismatch_count);
	fprintf(file, "pct:0\n");  // Again, try to match Celera

	/*
	// My additions, need to remove eventually:
	fprintf(file, "gap:%d\n", tb.gap_count);
	fprintf(file, "sco:%d\n", tb.score);
	fprintf(file, "tsc:%d\n", tb.total_score);
	fprintf(file, "st1:%d\n", tb.start1);
	fprintf(file, "en1:%d\n", tb.end1);
	fprintf(file, "st2:%d\n", tb.start2);
	fprintf(file, "en2:%d\n", tb.end2);
	*/

	fprintf(file, "}\n");
}

int abs(int i) { return (i >= 0) ? i : -i; }

void print_alignment(FILE * file, const char * str1, const char * str2, delta tb, int line_width)
{
	int curr1, curr2, curr, a_curr1, a_curr2, start;
	char a_str1[MAX_STRING];
	char a_str2[MAX_STRING];
	char print_str1[line_width+1];
	char print_str2[line_width+1];

	curr = 0;
	curr1 = 0;
	curr2 = 0;
	a_curr1 = 0;
	a_curr2 = 0;

	// First, determine which one needs to have
	// itself printed first. It is the one whose
	// start is not 0.
	if (tb.start1 > 0)
	{
		while (curr < tb.start1)
		{
			a_str1[curr] = str1[curr1++];
			a_str2[curr] = ' ';
			curr++;
		}
	}
	else
	{
		while (curr < tb.start2)
		{
			a_str2[curr] = str2[curr2++];
			a_str1[curr] = ' ';
			curr++;
		}
	}

	// Now, start printing the alignment.
	// Go until we've reached the end of one of the strings
	int count_since_last_gap = 1, curr_gap = 0;
	while (curr_gap < tb.gap_count)
	{
		if (count_since_last_gap < abs(tb.tb[curr_gap]))
		{
			a_str1[curr] = str1[curr1++];
			a_str2[curr] = str2[curr2++];
			curr++;
			count_since_last_gap++;
		}
		else
		{
			if (tb.tb[curr_gap] < 0)
			{
				a_str1[curr] = '.';
				a_str2[curr] = str2[curr2++];
				curr++;
			}
			else
			{
				a_str1[curr] = str1[curr1++];
				a_str2[curr] = '.';
				curr++;
			}
			count_since_last_gap = 1;
			curr_gap++;
		}
	}

	// We've printed all the gaps.
	// Now, print the rest of each string.
	int tmp_curr = curr;
	while (curr1 < tb.length1)
	{
		a_str1[tmp_curr++] = str1[curr1++];
	}
	while (curr2 < tb.length2)
	{
		a_str2[curr++] = str2[curr2++];
	}

	while (tmp_curr < curr) { a_str1[tmp_curr++] = ' '; }
	while (curr < tmp_curr) { a_str2[curr++] = ' '; }

	a_str1[curr] = '\0';
	a_str2[curr] = '\0';

	int i=0;
	int j=0;
	
	while (i < curr)
	{
		print_str1[j] = a_str1[i];
		print_str2[j] = a_str2[i];
		i++; j++;

		if (j == line_width || i == curr)
		{
			print_str1[j] = '\0';
			print_str2[j] = '\0';
			fprintf(file, "%s\n%s\n\n", print_str1, print_str2);
			j=0;
		}
	}
	//fprintf(file, "%s|\n%s|\n", a_str1, a_str2);
}

char arrow(cell ** matrix, int i, int j)
{
	if (matrix[j][i].tb == TB_LEFT) { return '-'; }
	if (matrix[j][i].tb == TB_DIAG) { return '*'; }
	if (matrix[j][i].tb == TB_UP) { return '^'; }
	return 'x';
}

void print_matrix(FILE * file, cell ** matrix, const char * str1, int length1, const char * str2, int length2)
{
	int i, j;
	fprintf(file, "    |     X | ");
	for(i=1; i<=length1; i++)
	{
		fprintf(file, "    %c | ", str1[i-1]);
	}
	printf("\n  X | ");
	for (i=0; i<=length1; i++)
	{
		fprintf(file, "  %3d | ", matrix[0][i].score);
	}
	fprintf(file,"\n");
	for(j=1; j<=length2; j++)
	{
		fprintf(file, "  %c | ", str2[j-1]);
		fprintf(file, "  %3d | ", matrix[j][0].score);
		for(i=1; i<=length1; i++)
		{
			fprintf(file, "%c %3d | ", arrow(matrix, i, j), matrix[j][i].score);
		}
		fprintf(file, "\n");
	}

}

static char comp(char c)
{
	switch(c)
	{
		case 'A':
		case 'a':
			return 'T';
		case 'T':
		case 't':
			return 'A';
		case 'G':
		case 'g':
			return 'C';
		case 'C':
		case 'c':
			return 'G';
		default:
			return 'N';
	}
}

// Ooh, reverse complement in place with a single scan of the string.
// No mallocing, copying and freeing for me!
void revcomp(seq * s)
{
	char * str = s->seq;
	int length, front, back;
	char c_i, c_j;
	int i, j;
	length = strlen(str);

	for(i=0, j=length-1; i <= j; i++, j--)
	{
		c_i = str[i];
		c_j = str[j];
		str[j] = comp(c_i);
		str[i] = comp(c_j);
	}
}


void free_delta(delta tb)
{
	if (tb.tb) free(tb.tb);
}

void free_seq(seq s)
{
	if (s.id) { free(s.id); s.id = 0; }
	if (s.seq) { free(s.seq); s.seq = 0; }
	if (s.metadata) { free(s.metadata); s.metadata = 0; }
}

void print_sequence(FILE * file, seq s)
{
	fprintf(file, ">%s", s.id);
	if (strlen(s.metadata) > 0) { fprintf(file, " "); }
	fprintf(file, "%s\n%s\n", s.metadata, s.seq);
}

float benchmark(FILE * file, const char * message)
{
	static struct timeval prev_tv;
	struct timeval curr_tv;
	float time_diff;

	gettimeofday(&curr_tv, NULL);

	if (prev_tv.tv_sec == 0)
	{
		fprintf(file, "%s: First benchmark\n", message);
		prev_tv = curr_tv;
		return 0.0;
	}
	
	time_diff = (curr_tv.tv_sec+ (curr_tv.tv_usec/1000000.0)) - (prev_tv.tv_sec+ (prev_tv.tv_usec/1000000.0));

	fprintf(file, "%s: %f\n", message, time_diff);

	prev_tv = curr_tv;

	return time_diff;
}



seq get_next_sequence(FILE * file)
{
	static char line[MAX_STRING] = "";
	static int count = 0;

	seq sequence;

	// Get the first line of the file.
	if (count == 0)
	{
		fgets(line, MAX_STRING, file);
		count = 1;
	}

	sequence.seq = 0;
	sequence.id = 0;
	sequence.metadata = 0;
	sequence.length = 0;

	if (line[0] == '>' && line[1] == '>')
	{

		// Get the next line in the file for the next iteration to start with.
		fgets(line, MAX_STRING, file);
		return sequence;
	}
	else
	{
		sequence.seq = malloc(MAX_STRING*sizeof(char));
		sequence.id = malloc(MAX_ID*sizeof(char));
		sequence.metadata = malloc(MAX_METADATA*sizeof(char));
		sequence.length = 0;
	}

	strcpy(sequence.metadata, "");

	int bases;
	int bytes;

	sscanf(line, ">%s %d %d %s\n", sequence.id, &bases, &bytes, sequence.metadata);

	while (1)
	{
		fgets(line, MAX_STRING, file);
		if (line[0] == '>') { break; }
		if (feof(file)) { break; }
		process_string(line);


		seq_cat(&sequence, line);
	}

	return sequence;
}

void seq_cat(seq * sequence, char * new_str)
{
	while (*new_str != '\0')
	{
		sequence->seq[sequence->length] = *new_str;
		sequence->length++;
		new_str++;
	}
	sequence->seq[sequence->length] = '\0';
}

void process_string(char * str)
{
	//int len = 0;
	while (*str != '\0')
	{
		*str = toupper(*str);
		str++;
		//len++;
	}
	str--;
	if (*str == '\n') { *str = '\0'; } //len--; }
	//return len;
}

void chomp(char * str)
{
	while (*str != '\0')
	{
		str++;
	}
	// If the last character is a newline, replace it with a \0.
	str--;
	if (*str == '\n') { *str = '\0'; }
}

void convert_to_upper(char * str)
{
	
	while (*str != '\0')
	{
		*str = toupper(*str);
		str++;
	}
}




