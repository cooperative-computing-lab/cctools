/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <sys/time.h>

#include "align.h"
#include "macros.h"
#include "matrix.h"

#define TB_LEFT '-'
#define TB_UP   '|'
#define TB_DIAG '\\'
#define TB_END  'X'

// XXX need to pass these in all the way from the beginning
static const int score_match = 2;
static const int score_mismatch = -1;
static const int score_gap = -1;

static void choose_best(struct matrix *m, int * i, int * j, int istart, int iend, int jstart, int jend );
static struct alignment * alignment_traceback(struct matrix *m, int i, int j, const char *a, const char *b );

/*
CAREFUL HERE:
The matrix indexes are [0:width] and [0:height], inclusive.
The 0th row and column of the matrix are used for initialization values.
BUT, the string indexes are [0:length), and are set one off from the matrix.
So, note that the value of matrix[i][j] evaluates string values a[i-1] and b[j-1].
*/

static inline cell score_cell( struct matrix *m, int i, int j, const char *a, const char *b, int is_smith_waterman )
{
	cell result;

	// Compute the score from the diagonal.
	int diagscore = m->data[j-1][i-1].score + ((a[i-1]==b[j-1]) ? score_match : score_mismatch);
	result.score = diagscore;
	result.tb = TB_DIAG;

	// Compute the score from the left, and accept if greater.
	int leftscore = m->data[j][i-1].score + score_gap;
	if(leftscore>result.score) {
		result.score = leftscore;
		result.tb = TB_LEFT;
	}

	// Compute the score from above, and accept if greater.
	int upscore = m->data[j-1][i].score + score_gap;
	if(upscore>result.score) {
		result.score = upscore;
		result.tb = TB_UP;
	}

	// Smith-Waterman alignments can never go below zero.
	// A zero will stop the traceback at that spot.

	if(is_smith_waterman) {
		if(result.score<0) {
			result.score = 0;
			result.tb = TB_END;
		}
	}

	return result;
}

struct alignment * align_smith_waterman( struct matrix *m, const char * a, const char * b )
{
	int alength = strlen(a);
	int blength = strlen(b);
	int i,j;

	int best_i = 0;
	int best_j = 0;
	int best_score = 0;

	// Zero out the first row.
	for(i=0; i<=alength; i++) {
		m->data[0][i].score = 0;
		m->data[0][i].tb = TB_LEFT;
	}

	// Zero out the first column.
	for(j=0; j<=blength; j++) {
		m->data[j][0].score = 0;
		m->data[j][0].tb = TB_UP;
	}

	// Sweep out the rest of the matrix.
	for (j=1; j<=blength; j++) {
		for (i=1; i<=alength; i++) {

			cell s = score_cell(m, i, j, a, b, 1 );
			m->data[j][i] = s;

			// Keep track of the cell with the best score.
			if(s.score>=best_score) {
				best_score = s.score;
				best_i = i;
				best_j = j;
			}
		}
	}

	// Start the traceback from the cell with the highest score.
	return alignment_traceback(m, best_i, best_j, a, b );
}

struct alignment * align_prefix_suffix( struct matrix *m, const char * a, const char * b, int min_align )
{
	int alength = strlen(a);
	int blength = strlen(b);
	int i,j;
	int best_i = 0;
	int best_j = 0;

	// Zero out the top row.
	for(i=0; i<=alength; i++) {
		m->data[0][i].score = 0;
		m->data[0][i].tb = TB_LEFT;
	}

	// Zero out the left column.
	for(j=1; j<=blength; j++) {
		m->data[j][0].score = 0;
		m->data[j][0].tb = TB_UP;
	}

	// Sweep out the entire matrix.
	for (i=1; i<=alength; i++) {
		for (j=1; j<=blength; j++) {
			m->data[j][i] = score_cell(m, i, j, a, b, 0 );
		}
	}

	// Find the maximum of the last row and last column.
	choose_best(m, &best_i, &best_j, min_align, alength, min_align, blength);

	// Start traceback from best position and go until we hit the top or left edge.
	return alignment_traceback(m, best_i, best_j, a, b );
}

struct alignment * align_banded( struct matrix *m, const char *a, const char *b, int astart, int bstart, int k )
{
	int alength = strlen(a);
	int blength = strlen(b);
	int i,j;
	int best_i = 0;
	int best_j = 0;

	int offset = astart - bstart;

	// Zero out the top border.
	for(i=0;i<=alength;i++) {
		m->data[0][i].score = 0;
		m->data[0][i].tb = TB_LEFT;
	}

	// Zero out the left border.
	for(j=0;j<=blength;j++) {
		m->data[j][0].score = 0;
		m->data[j][0].tb = TB_UP;
	}

	// QUESTION: what happens if the alignment wanders off the diagonals?
	// ANSWER: shouldn't happen, but scott will check.

	// Zero out the diagonals.
	for(j=0;j<=blength;j++) {
		i = offset + k + j + 1;
		if(i>=0 && i<=alength) {
			m->data[j][i].score = 0;
			m->data[j][i].tb = TB_LEFT;
		}
		i = offset - k + j - 1;
		if(i>=0 && i<=alength) {
			m->data[j][i].score = 0;
			m->data[j][i].tb = TB_UP;
		}
	}

	#define BRACKET( a, x, b ) MIN(MAX((a),(x)),(b))

	// For each row, sweep out the valid range of columns.
	for(j=1;j<=blength;j++) {

		int istart = BRACKET(1,offset+j-k,alength);
		int istop  = BRACKET(1,offset+j+k,alength);

		for(i=istart;i<=istop;i++) {
			m->data[j][i] = score_cell(m,i,j,a,b,0);
		}
	}

	// Choose the best value on the valid ranges of the alignment.
	choose_best(m, &best_i, &best_j,
		       BRACKET(0,blength+offset-k,alength),
		       BRACKET(0,blength+offset+k,alength),
		       BRACKET(0,alength-offset-k,blength),
		       BRACKET(0,alength-offset+k,blength) );

	// Run the traceback back to the edges of the matrix.
	return alignment_traceback(m, best_i, best_j, a, b );
}

static void choose_best(struct matrix *m, int * best_i, int * best_j, int istart, int iend, int jstart, int jend )
{
	int i, j;
	double quality;
	double best_qual = 1000000.0;

	// QUESTION: do we want maximum score, or Celera quality score here?
	// QUESTION: In Celera, is a good score high or low?

	// Find the best in the last column
	for (i=m->width, j=jstart; j <= jend; j++) {
		quality = ((double) m->data[j][i].score) / (double)MIN(i, j);
		if (quality < best_qual) {
			best_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}

	// Find the best in the last row
	for (i=istart, j=m->height; i <= iend; i++) {
		quality = ((double) m->data[j][i].score) / (double)MIN(i, j);
		if (quality < best_qual) {
			best_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}
}

static struct alignment * alignment_traceback(struct matrix *m, int istart, int jstart, const char *a, const char *b )
{
	struct alignment * aln = malloc(sizeof(*aln));
	memset(aln,0,sizeof(*aln));

	int max_traceback_length = m->width + m->height + 4;
	aln->tb = malloc(max_traceback_length*sizeof(*aln->tb));

	int i = istart;
	int j = jstart;
	int dir = 0;
	int length = 0;

	while ( (i>0) && (j>0) ) {

		dir = m->data[j][i].tb;
		aln->tb[length++] = dir;

		if(dir==TB_DIAG) {
			if(a[i-1]!=b[j-1]) {
				aln->mismatch_count++;
			}
			i--;
			j--;
		} else if(dir==TB_LEFT) {
			i--;
		} else if(dir==TB_UP) {
			j--;
		} else if(dir==TB_END) {
			length--;
			break;
		} else {
			fprintf(stderr,"traceback corrupted at i=%d j=%d\n",i,j);
			exit(1);
		}
	}

	aln->tb[length] = 0;

	// Reverse the traceback and resize the allocation as needed.
	char *n = malloc((length+1)*sizeof(char));
	int k;
	for(k=0;k<length;k++) {
		n[k] = aln->tb[length-k-1];
	}
	n[length] = 0;

	free(aln->tb);
	aln->tb = n;

	// QUESTION: Where are the Celera OVL records defined?  I don't trust this...
	// scott will look up the semantics of these.

	aln->start1 = i;
	aln->start2 = j;
	aln->end1 = istart-1;
	aln->end2 = jstart-1;
	aln->gap_count = length; // QUESTION: I don't think this is right: length is the number of steps in the traceback.
	aln->length1 = m->width;
	aln->length2 = m->height;
	aln->score = m->data[jstart][istart].score;
	aln->total_score = aln->score + (aln->length1 - istart) + i + (aln->length2 - jstart) + j; // CHECK
	aln->quality = (double)(aln->gap_count + aln->mismatch_count) / (double) MIN(i, j); // CHECK
	
	return aln;
}

// Find the maximum alignment length given the lengths and the start
// positions of the exact match. Assume the start position has already
// been corrected for distance.
int align_max(int alength, int blength, int start1, int start2)
{
	return MIN(start1, start2) + MIN(alength - start1, blength - start2);
}

#define LINE_WIDTH 80

static void print_rows( FILE * file, char a, char b )
{
	static char *linea=0;
	static char *lineb=0;
	static int i = 0;

	if(!linea) linea = malloc(LINE_WIDTH+1);
	if(!lineb) lineb = malloc(LINE_WIDTH+1);

	linea[i] = a;
	lineb[i] = b;
	i++;

	if(i==LINE_WIDTH || (a==0 && b==0) ) {
		linea[i] = lineb[i] = 0;
		fprintf(file,"%s\n%s\n\n",linea,lineb);
		i=0;
	}
}

void alignment_print( FILE * file, const char * a, const char * b, struct alignment *aln, int line_width )
{
	char *t = aln->tb;

	int offset = aln->start1 - aln->start2;
	int i;

	if(offset>0) {
		for(i=0;i<offset;i++)           print_rows(file,a[i],'*');
		for(i=offset;i<aln->start1;i++) print_rows(file,a[i],b[i-offset]);
	} else {
		offset = -offset;
		for(i=0;i<offset;i++)           print_rows(file,'*',b[i]);
		for(i=offset;i<aln->start1;i++) print_rows(file,a[i-offset],b[i]);
	}

	a = &a[aln->start1];
	b = &b[aln->start2];

	while(*t) {
		if(*t==TB_DIAG) {
			print_rows(file,*a++,*b++);
		} else if(*t==TB_LEFT) {
			print_rows(file,*a++,'.');
		} else if(*t==TB_UP) {
			print_rows(file,'.',*b++);
		} else {
			fprintf(stderr,"traceback corrupted\n");
			exit(1);
		}
		t++;
	}

	while(*a || *b) {
		char j = *a ? *a : '*';
		char k = *b ? *b : '*';
		print_rows(file,j,k);
		if(*a) a++;
		if(*b) b++;
	}

	print_rows(file,0,0);
	fflush(file);
}

void alignment_delete( struct alignment *aln )
{
	if(aln) {
		if(aln->tb) free(aln->tb);
		free(aln);
	}
}

