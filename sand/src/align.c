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

#define TRACEBACK_LEFT '-'
#define TRACEBACK_UP   '|'
#define TRACEBACK_DIAG '\\'
#define TRACEBACK_END  'X'

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

static inline struct cell score_cell( struct matrix *m, int i, int j, const char *a, const char *b, int is_smith_waterman )
{
	struct cell result;

	// Compute the score from the diagonal.
	int diagscore = matrix(m,i-1,j-1).score + ((a[i-1]==b[j-1]) ? score_match : score_mismatch);
	result.score = diagscore;
	result.traceback = TRACEBACK_DIAG;

	// Compute the score from the left, and accept if greater.
	int leftscore = matrix(m,i-1,j).score + score_gap;
	if(leftscore>result.score) {
		result.score = leftscore;
		result.traceback = TRACEBACK_LEFT;
	}

	// Compute the score from above, and accept if greater.
	int upscore = matrix(m,i,j-1).score + score_gap;
	if(upscore>result.score) {
		result.score = upscore;
		result.traceback = TRACEBACK_UP;
	}

	// Smith-Waterman alignments can never go below zero.
	// A zero will stop the traceback at that spot.

	if(is_smith_waterman) {
		if(result.score<0) {
			result.score = 0;
			result.traceback = TRACEBACK_END;
		}
	}

	return result;
}

struct alignment * align_smith_waterman( struct matrix *m, const char * a, const char * b )
{
	int width = m->width;
	int height = m->height;
	int i,j;

	int best_i = 0;
	int best_j = 0;
	int best_score = 0;

	// Zero out the first row.
	for(i=0; i<=width; i++) {
		matrix(m,i,0).score = 0;
		matrix(m,i,0).traceback = TRACEBACK_LEFT;
	}

	// Zero out the first column.
	for(j=0; j<=height; j++) {
		matrix(m,0,j).score = 0;
		matrix(m,0,j).traceback = TRACEBACK_UP;
	}

	// Sweep out the rest of the matrix.
	for (j=1; j<=height; j++) {
		for (i=1; i<=width; i++) {

			struct cell s = score_cell(m, i, j, a, b, 1 );
			matrix(m,i,j) = s;

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
	int width = m->width;
	int height = m->height;
	int i,j;
	int best_i = 0;
	int best_j = 0;

	// Zero out the top row.
	for(i=0; i<=width; i++) {
		matrix(m,i,0).score = 0;
		matrix(m,i,0).traceback = TRACEBACK_LEFT;
	}

	// Zero out the left column.
	for(j=1; j<=height; j++) {
		matrix(m,0,j).score = 0;
		matrix(m,0,j).traceback = TRACEBACK_UP;
	}

	// Sweep out the entire matrix.
	for (i=1; i<=width; i++) {
		for (j=1; j<=height; j++) {
			matrix(m,i,j) = score_cell(m, i, j, a, b, 0 );
		}
	}

	// Find the maximum of the last row and last column.
	choose_best(m, &best_i, &best_j, min_align, width, min_align, height);

	// Start traceback from best position and go until we hit the top or left edge.
	return alignment_traceback(m, best_i, best_j, a, b );
}

struct alignment * align_banded( struct matrix *m, const char *a, const char *b, int astart, int bstart, int k )
{
	int width = m->width;
	int height = m->height;
	int i,j;
	int best_i = 0;
	int best_j = 0;

	int offset = astart - bstart;

	// Zero out the top border.
	for(i=0;i<=width;i++) {
		matrix(m,i,0).score = 0;
		matrix(m,i,0).traceback = TRACEBACK_LEFT;
	}

	// Zero out the left border.
	for(j=0;j<=height;j++) {
		matrix(m,0,j).score = 0;
		matrix(m,0,j).traceback = TRACEBACK_UP;
	}

	// QUESTION: what happens if the alignment wanders off the diagonals?
	// ANSWER: all cells outside of band should be set to -infinity -- need to implement I believe

	// Zero out the diagonals.
	for(j=0;j<=height;j++) {
		i = offset + k + j + 1;
		if(i>=0 && i<=width) {
			matrix(m,i,j).score = 0;
			matrix(m,i,j).traceback = TRACEBACK_LEFT;
		}
		i = offset - k + j - 1;
		if(i>=0 && i<=width) {
			matrix(m,i,j).score = 0;
			matrix(m,i,j).traceback = TRACEBACK_UP;
		}
	}

	#define BRACKET( a, x, b ) MIN(MAX((a),(x)),(b))

	// For each row, sweep out the valid range of columns.
	for(j=1;j<=MIN(height,width-offset+k);j++) {

		int istart = BRACKET(1,offset+j-k,width);
		int istop  = BRACKET(1,offset+j+k,width);

		for(i=istart;i<=istop;i++) {
			matrix(m,i,j) = score_cell(m,i,j,a,b,0);
		}
	}

	// Choose the best value on the valid ranges of the alignment.
	choose_best(m, &best_i, &best_j,
		       BRACKET(0,height+offset-k,width),
		       BRACKET(0,height+offset+k,width),
		       BRACKET(0,width-offset-k,height),
		       BRACKET(0,width-offset+k,height) );

	// Run the traceback back to the edges of the matrix.
	return alignment_traceback(m, best_i, best_j, a, b );
}

static void choose_best(struct matrix *m, int * best_i, int * best_j, int istart, int iend, int jstart, int jend )
{
	int i, j;
	double best_score = 0;


	// QUESTION: do we want to use % identity like Celera? May require changing the score parameters 

	// QUESTION there are a couple of odd boundary cases where the limits
	// are a single cell in either row.  To avoid that, we initialize the
	// best to the first element in each dimension.

	*best_i = istart;
	*best_j = jstart;

	// QUESTION: do we want maximum score, or Celera quality score here?
	// QUESTION: In Celera, is a good score high or low?

	// Find the best in the last column
	if(jstart!=jend) {
		
	  for (i=m->width, j=jstart; j <= jend; j++) {
			
	    if ( matrix(m,i,j).score > best_score) {
		    
	      best_score =  matrix(m,i,j).score;
	      *best_i = i;
	      *best_j = j;
		  
	    }	
		
	  }

	}

	// Find the best in the last row
	if(istart!=iend) {
		for (i=istart, j=m->height; i <= iend; i++) {

		  if ( matrix(m,i,j).score > best_score) {
			
		    best_score =  matrix(m,i,j).score;
		    *best_i = i;
		    *best_j = j;
		  
		  }

		}
	}
}

static struct alignment * alignment_traceback(struct matrix *m, int istart, int jstart, const char *a, const char *b )
{
	struct alignment * aln = malloc(sizeof(*aln));
	memset(aln,0,sizeof(*aln));

	int max_traceback_length = m->width + m->height + 4;
	aln->traceback = malloc(max_traceback_length*sizeof(*aln->traceback));

	int i = istart;
	int j = jstart;
	int dir = 0;
	int length = 0;

	while ( (i>0) && (j>0) ) {

		dir = matrix(m,i,j).traceback;
		aln->traceback[length++] = dir;

		if(dir==TRACEBACK_DIAG) {
			if(a[i-1]!=b[j-1]) {
				aln->mismatch_count++;
			}
			i--;
			j--;
		} else if(dir==TRACEBACK_LEFT) {
			i--;
			aln->gap_count++;
		} else if(dir==TRACEBACK_UP) {
			j--;
			aln->gap_count++;
		} else if(dir==TRACEBACK_END) {
			length--;
			break;
		} else {
			fprintf(stderr,"traceback corrupted at i=%d j=%d\n",i,j);
			exit(1);
		}
	}

	aln->traceback[length] = 0;

	// Reverse the traceback and resize the allocation as needed.
	char *n = malloc((length+1)*sizeof(char));
	int k;
	for(k=0;k<length;k++) {
		n[k] = aln->traceback[length-k-1];
	}
	n[length] = 0;

	free(aln->traceback);
	aln->traceback = n;

	// NOTE: These parameters are what are needed for OVL records.  Other values are
        // calculated on runtime in the overlap output code

	aln->start1 = i;
	aln->start2 = j;
	aln->end1 = istart-1;
	aln->end2 = jstart-1;
	aln->length1 = m->width;
	aln->length2 = m->height;
	aln->score = matrix(m,istart,jstart).score;
	aln->quality = (double)(aln->gap_count + aln->mismatch_count) / (double) MIN(i, j); 
	
	return aln;
}

// Find the maximum alignment length given the lengths and the start
// positions of the exact match. Assume the start position has already
// been corrected for distance.
int align_max(int width, int height, int start1, int start2)
{
	return MIN(start1, start2) + MIN(width - start1, height - start2);
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

void alignment_print( FILE * file, const char * a, const char * b, struct alignment *aln )
{
	char *t = aln->traceback;

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
		if(*t==TRACEBACK_DIAG) {
			print_rows(file,*a++,*b++);
		} else if(*t==TRACEBACK_LEFT) {
			print_rows(file,*a++,'.');
		} else if(*t==TRACEBACK_UP) {
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
		if(aln->traceback) free(aln->traceback);
		free(aln);
	}
}

