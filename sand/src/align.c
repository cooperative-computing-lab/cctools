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

#include "align.h"

#define TB_LEFT -1
#define TB_UP 1
#define TB_DIAG 0
#define TB_END 2

#define INIT_PREFIX_SUFFIX 0
#define INIT_SW 1

#define CHECK_UP   1
#define CHECK_DIAG 2
#define CHECK_LEFT 4

struct s_cell
{
	int score;
	int tb;
};

typedef struct s_cell cell;

static int score_mismatch = 1;
static int score_match_open = 0;
static int score_match_extend = 0;
static int score_gap_open = 1;
static int score_gap_extend = 1;

static cell new_score(cell ** matrix, int i, int j, const char * str1, const char * str2);
static cell new_score_gap_extensions(cell ** matrix, int i, int j, const char * str1, const char * str2);
static cell new_score_banded(cell ** band, int band_row, int band_col, int matrix_row, int matrix_col, const char * str1, const char * str2, int which);
static void choose_best(cell ** matrix, int * i, int * j, int length1, int length2, int min_align);
static delta align_traceback(cell ** matrix, int i, int j, int length1, int length2, int min_score);

static cell ** matrix_init(int length1, int length2, int type);
static void matrix_free(cell ** matrix, int length2);
static int get_last_simple_row(int diag, int k, int length1, int length2);

delta align_smith_waterman( const char * str1, const char * str2 )
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int i,j;
	delta tb;

	cell ** matrix = matrix_init(length1, length2, INIT_SW);

	for (i=1; i<=length1; i++)
	{
		for (j=1; j<=length2; j++)
		{
			matrix[j][i] = new_score_gap_extensions(matrix, i, j, str1, str2);
		}
	}

	tb = align_traceback(matrix, length1, length2, length1, length2, INT_MIN);

	matrix_free(matrix, length2);

	return tb;
}

delta align_prefix_suffix(const char * str1, const char * str2, int min_align)
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int i,j;
	delta tb;

	cell ** matrix = matrix_init(length1, length2, INIT_PREFIX_SUFFIX);
	for (i=1; i<=length1; i++)
	{
		for (j=1; j<=length2; j++)
		{
			matrix[j][i] = new_score(matrix, i, j, str1, str2);
		}
	}

	int best_i=0;
	int best_j=0;
	choose_best(matrix, &best_i, &best_j, length1, length2, min_align);

	tb = align_traceback(matrix, best_i, best_j, length1, length2, INT_MIN);

	matrix_free(matrix, length2);

	return tb;
}

static void band2matrix(int band_row, int band_col, int diag, int k, int * matrix_row, int * matrix_col)
{
	*matrix_row = (diag < -k) ? ((band_row) + (-diag-k)) : band_row;
	*matrix_col = (*matrix_row + diag) + (band_col - k);
}

static int start_band_left(cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int band_row = 0;
	int matrix_row=0, first_full_row=0, band_first_col=0;
	int width = (2*k)+1;

	// In the first row, the first (and only) column populated
	// is the last one, but this is actually the first column
	// in the DP matrix.
	int matrix_col = 0;
	int band_col;

	int diag = start1 - start2;

	if (diag <= -k) // left side, no corner
	{
		matrix_row = start2 - k;
		first_full_row = (2*k)+1;
		band_first_col = width-1;
	} 
	else if (diag < 0) // left side on the corner.
	{
		matrix_row = 0;
		first_full_row = start2+k+1;
		band_first_col = width - (k-start2+1);
	}
	else if (diag < k) // right side on the corner
	{
		matrix_row = 0;
		first_full_row = k - start1+1;
		band_first_col = width - (k+start1+1);
	}

	// Initialize the very first row, which contains not more
	// than k+1 cells.
	for (band_col = band_first_col; band_col < width; band_col++)
	{
		band[band_row][band_col].score = 0;
		band[band_row][band_col].tb = TB_END;
	}
	band_row++;
	matrix_row++;
	band_first_col--;

	while (band_row < first_full_row)
	{
		band_col = band_first_col;

		// The first in each row is just 0 in this case,
		// which is part of the initialization.
		band[band_row][band_col].score = 0;
		band[band_row][band_col].tb = TB_END;
		matrix_col++;

		// For all but the last column, look at all three recursions.
		for (band_col = band_first_col+1; band_col < (width-1); band_col++)
		{
			band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, CHECK_UP | CHECK_LEFT | CHECK_DIAG);
			matrix_col++;
		}

		// Now, for the last column, just look at left and diag, don't
		// check up.
		band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, CHECK_LEFT | CHECK_DIAG);
		matrix_col++;

		// Now, increment everything
		band_row++;
		matrix_row++;
		band_first_col--;
		matrix_col = 0;
	}

	// We actually filled out the first full row, but don't tell
	// the rest of the code that. The reason is so that the rest
	// of the code doesn't have to care about making sure the
	// first column is set to 0.
	*matrix_row_ret = matrix_row;
	*matrix_col_ret = 1;
	return first_full_row;
}

// In this case, just initialize the first row.  No fuss, no muss.
static int start_band_upper(cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int band_row = 0;
	
	int width = (2*k)+1;
	int band_col;

	for (band_col=0; band_col < width; band_col++)
	{
		band[band_row][band_col].score = 0;
		band[band_row][band_col].tb = TB_END;
	}

	*matrix_row_ret = 1;
	*matrix_col_ret = start1 - k+1;
	return 1;
}

static int get_last_simple_row(int diag, int k, int length1, int length2)
{
	// Figure out when the border of the band will hit the right
	// side of the matrix:
	int right_side_intersect_row = length1 - (diag+k);

	// If the right border of the band intersects with the right
	// side of the matrix below the last row, then just stop
	// at the last row. Otherwise stop where it intersects.
	return MIN(right_side_intersect_row, length2);
}

static void choose_best_banded(cell ** band, int * best_row_ret, int * best_col_ret, int length1, int length2, int k, int band_row, int cols_in_last_row, int rows_in_last_col)
{
	int band_col;
	int best_row=0, best_col=0, best_score=0;
	int i;

	best_score = INT_MAX;

	// Now check the last row.
	for (band_col=cols_in_last_row; band_col>=0; band_col--)
	{
		if (band[band_row][band_col].score < best_score)
		{
			best_score = band[band_row][band_col].score;
			best_row = band_row;
			best_col = band_col;
		}
	}

	// Check the last column.
	for (i=0; i < rows_in_last_col; i++)
	{
		band_col = cols_in_last_row+i+1;
		band_row--;
		if (band[band_row][band_col].score < best_score)
		{
			best_score = band[band_row][band_col].score;
			best_row = band_row;
			best_col = band_col;
		}
	}

	*best_row_ret = best_row;
	*best_col_ret = best_col;
}

static delta align_traceback_banded(cell ** band, int best_row, int best_col, int length1, int length2, int k, int diag)
{
	delta tb;
	int band_row, band_col, curr_tb_type, count;
	int count_since_last=0, last_gap_type=0;
	int temp_tb[MIN(length1, length2)];
	int first = 1;
	int i;
	int total_bases = 0;

	// Need to figure out start and end positions in terms of the matrix,
	// not of the band.

	tb.mismatch_count = 0;

	band_row = best_row;
	band_col = best_col;
	count = 0;
	curr_tb_type = band[band_row][band_col].tb;

	while (curr_tb_type != TB_END)
	{
		// If it's not diagonal, then it's a gap.
		if (curr_tb_type != TB_DIAG)
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
			last_gap_type = curr_tb_type;
			count_since_last = 0;

			// Decrement either band_row or band_col, depending on
			// which direction the traceback is meant to go.

			// If it is a left gap, we go left but up.
			// If it is an up gap, we go up one, but because
			// of the offset band the column above is numbered
			// one higher.
			(curr_tb_type == TB_LEFT) ? band_col-- : (band_col++, band_row--);
		}
		else
		{
			// It was TB_DIAG, so see if it's a mismatch.
			if (band[band_row][band_col].score - band[band_row-1][band_col].score == score_mismatch)
			{
				tb.mismatch_count++;
			}
			band_row--;
			// band_col stays the same because of the offset nature of
			// the rows.
		}
		count_since_last++;
		total_bases++;

		curr_tb_type = band[band_row][band_col].tb;
	}

	// Add the last gap in the traceback (the first gap in the alignment)
	if (count_since_last < total_bases) temp_tb[count++] = count_since_last * ( (last_gap_type == TB_LEFT) ? 1 : -1) ;

	if (count > 0)
	{
		tb.tb = malloc((count)*sizeof(int));
		for (i=count-1; i >= 0; i--) tb.tb[count-1-i] = temp_tb[i];
	}
	else tb.tb = 0;

	// convert the start and end positions from their positions
	// in the band to their positions on the matrix.
	// matrix_row = (diag < 0) ? ((-diag - k)+band_row) : band_row;
	// matrix_col = (band_row + diag) + (band_col - k)
	//   band_row + diag puts you in the middle of the band (position k)

	band2matrix(band_row, band_col, diag, k, &tb.start2, &tb.start1);
	band2matrix(best_row, best_col, diag, k, &tb.end2, &tb.end1);
	tb.end2--;  // These are one too high because of the implied "X"
	tb.end1--;  // in the DP matrix.
	tb.gap_count = count;
	tb.length1 = length1;
	tb.length2 = length2;
	tb.score = band[best_row][best_col].score;
	tb.total_score = tb.gap_count+tb.mismatch_count;
	tb.quality = (float)(tb.gap_count+tb.mismatch_count) / (float) (best_row - band_row);

	return tb;
}

delta align_banded(const char * str1, const char * str2, int start1, int start2, int k)
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int band_row,band_col;
	int matrix_row, matrix_col;
	delta tb;
	tb.tb = 0;
	int lastrow = MIN(length1, length2)+(2*k);
	int width = 2*k+1;
	int last_simple_row, last_col;

	// Fix the start positions so that one is 0 and the other is positive.
	// Because we're finding the diagonal, we can subtract the same amount
	// from both.
	if (start1 < start2)
	{
		start2 -= start1;
		start1 = 0;
	}
	else
	{
		start1 -= start2;
		start2 = 0;
	}

	cell ** band = matrix_init(width, lastrow, INIT_PREFIX_SUFFIX);

	int diag = start1 - start2;
	int last_col_which = CHECK_DIAG | CHECK_LEFT;

	if (diag < k)
	{
		band_row = start_band_left(band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}
	else // (diag >= k)
	{
		band_row = start_band_upper(band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}

	last_simple_row = get_last_simple_row(diag, k, length1, length2);
	last_col = width;
	while (matrix_row <= MIN(length2, last_simple_row+width-1))
	{
		// The first one checks only up and diag, not left
		band[band_row][0] = new_score_banded(band, band_row, 0, matrix_row, matrix_col, str1, str2, CHECK_UP | CHECK_DIAG);
		matrix_col++;

		// All but the last check all three recursions
		for (band_col = 1; band_col < last_col-1; band_col++)
		{
			band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, CHECK_LEFT | CHECK_DIAG | CHECK_UP);
			matrix_col++;
		}

		// The last col only checks left and diag, not up.
		band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, last_col_which);
		
		// Update all the values.
		band_row++;
		matrix_row++;
		matrix_col -= last_col-2;

		if (matrix_row > last_simple_row)
		{
			last_col--;
			last_col_which = CHECK_UP | CHECK_DIAG | CHECK_LEFT;
		}
	}

	int best_row, best_col;

	choose_best_banded(band, &best_row, &best_col, length1, length2, k, band_row-1, last_col, width-last_col-1 );
	tb = align_traceback_banded(band, best_row, best_col, length1, length2, k, diag);

	// Free the banded matrix.
	int j;
	for (j=0; j<= lastrow; j++)
	{
		free(band[j]);
	}
	free(band);

	return tb;

}


cell ** matrix_init(int length1, int length2, int type)
{
	int i, j;

	// Create the list of rows, each itself an int *
	// Add 1 to make sure it has enough room for the
	// first row of 0's

	cell ** matrix = malloc((length2+1)*sizeof(cell *));
	for(j=0; j<=length2; j++)
	{
		matrix[j] = malloc((length1+1)*sizeof(cell));
	}

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
	} else if (type == INIT_SW) {

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


void matrix_free(cell ** matrix, int length2)
{
	int j;

	// Free each row
	for(j=0; j<=length2; j++)
	{
		free(matrix[j]);
	}

	// Free the list of rows.
	free(matrix);
}

static cell new_score(cell ** matrix, int i, int j, const char * str1, const char * str2)
{
	cell min;
	int incr;

	memset(&min,0,sizeof(min));

	min.score = INT_MAX;

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

	return min;
}

static cell new_score_gap_extensions(cell ** matrix, int i, int j, const char * str1, const char * str2)
{
	cell min;
	int incr;

	memset(&min,0,sizeof(min));
	min.score = INT_MAX;

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

	return min;
}

/*
Assumes we are trying to maximize the score rather than
minimize it.  Used for local alignment.
*/
static cell new_score_banded(cell ** band, int band_row, int band_col, int matrix_row, int matrix_col, const char * str1, const char * str2, int which)
{
	cell min, recurrence;
	int incr;

	memset(&min,0,sizeof(min));
	memset(&recurrence,0,sizeof(recurrence));

	min.score = INT_MAX;

	// Check if it's a gap (up).
	if (which & CHECK_UP)
	{
		recurrence = band[band_row-1][band_col+1];
		incr = (recurrence.tb == TB_UP) ? score_gap_extend : score_gap_open;
		if (recurrence.score+incr < min.score)
		{
			min.score = recurrence.score+incr;
			min.tb = TB_UP;
		}
	}

	// Check the left gap
	if (which & CHECK_LEFT)
	{
		recurrence = band[band_row][band_col-1];
		incr = (recurrence.tb == TB_LEFT) ? score_gap_extend : score_gap_open;
		if (recurrence.score+incr < min.score)
		{
			min.score = recurrence.score+incr;
			min.tb = TB_LEFT;
		}
	}
	
	// Check the diag.
	if (which & CHECK_DIAG)
	{
		recurrence = band[band_row-1][band_col];
		if (str1[matrix_col-1] == str2[matrix_row-1])
		{
			if ((recurrence.tb == TB_DIAG) && (str1[matrix_col-2] == str2[matrix_row-2]))
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
		if (recurrence.score+incr < min.score)
		{
			min.score = recurrence.score+incr;
			min.tb = TB_DIAG;
		}
	}

	return min;
}

static void choose_best(cell ** matrix, int * best_i, int * best_j, int length1, int length2, int min_align)
{
	int i, j;
	float quality;
	float min_qual = (float) length1 + (float)length2;


	// Find the best in the last column
	for (i=length1, j=min_align; j <= length2; j++)
	{
		quality = ((float) matrix[j][i].score) / (float)MIN(i, j);
		if (quality < min_qual)
		{
			min_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}

	// Find the best in the last row
	for (i=min_align, j=length2; i <= length1; i++)
	{
		quality = ((float) matrix[j][i].score) / (float)MIN(i, j);
		if (quality < min_qual)
		{
			min_qual = quality;
			*best_i = i;
			*best_j = j;
		}
	}
}

static delta align_traceback(cell ** matrix, int i, int j, int length1, int length2, int min_score)
{
	delta tb;
	int curr_i, curr_j, curr_gap_type, count;
	int first = 1, last_gap_type=0, count_since_last = 0;
	int temp_tb[MAX_STRING];
	int total_bases = 0;

	// The return array requires 4 pieces of info
	// 1. Whether string 1 or string 2 is the string for which the alignment is a prefix.
	// 2. Which position in the prefix string the alignment starts at
	// 3. Which position in the suffix string the alignment ends at.
	// 4. The positions of the gaps.
	memset(&tb,0,sizeof(tb));
	tb.end1 = i-1;
	tb.end2 = j-1;
	tb.mismatch_count = 0;

	curr_i = i;
	curr_j = j;
	curr_gap_type = 0;
	count = 0;

	while ((curr_i > 0) && (curr_j > 0) && (matrix[curr_j][curr_i].score > min_score))
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
			if ((matrix[curr_j][curr_i].score - matrix[curr_j-1][curr_i-1].score) == score_mismatch)
			{
				tb.mismatch_count++;
			}
			curr_i--;
			curr_j--;
		}

		count_since_last++;
		total_bases++;
	}

	// This is what is causing the last character to always be a gap.
	// What's up with that?
	if (count_since_last < total_bases) temp_tb[count++] = count_since_last * ( (last_gap_type == TB_LEFT) ? 1 : -1) ;

	int k;
	if (count > 0)
	{
		tb.tb = malloc((count)*sizeof(int));
		for (k=count-1; k >= 0; k--)
		{
		
			tb.tb[count-1-k] = temp_tb[k];
		}
	}
	else tb.tb = 0;
	tb.start1 = curr_i;
	tb.start2 = curr_j;
	tb.gap_count = count;
	tb.length1 = length1;
	tb.length2 = length2;
	tb.score = matrix[j][i].score;
	tb.total_score = tb.score + (length1 - i) + curr_i + (length2 - j) + curr_j;
	tb.quality = (float)(tb.gap_count + tb.mismatch_count) / (float) MIN(i, j);
	
	return tb;
}

// Find the maximum alignment length given the lengths and the start
// positions of the exact match. Assume the start position has already
// been corrected for distance.
int align_max(int length1, int length2, int start1, int start2)
{
	return MIN(start1, start2) + MIN(length1 - start1, length2 - start2);
}

void delta_print_local(FILE * file, const char * str1, const char * str2, delta tb, int line_width)
{
	int curr1, curr2, curr, a_curr1, a_curr2;
	char a_str1[MAX_STRING];
	char a_str2[MAX_STRING];
	char print_str1[line_width+1];
	char print_str2[line_width+1];

	curr = 0;
	curr1 = 0;
	curr2 = 0;
	a_curr1 = 0;
	a_curr2 = 0;

	// First, print the overhang between the two alignments.
	// The one with the higher start position hangs off the
	// front, as illustrated below:
	//        s1     e1
	// |--|----+------+----
	// |  |----+------+------
	//        s2     e2
	if (tb.start1 > tb.start2)
	{
		while (curr < (tb.start1 - tb.start2))	
		{
			a_str1[curr] = str1[curr1++];
			a_str2[curr] = ' ';
			curr++;
		}
	}
	else
	{
		while (curr < (tb.start2 - tb.start1))
		{
			a_str2[curr] = str2[curr2++];
			a_str1[curr] = ' ';
			curr++;
		}
	}

	// Now, we can print from the end of the overhang
	// until the beginning of the
	// alignment. This will just be a bunch of mismatches
	// probably.
	//         s1     e1
	//  --|----|------+----
	//    |----|------+------
	//         s2     e2
	while (curr1 < tb.start1)
	{
		a_str1[curr] = str1[curr1++];
		a_str2[curr] = str2[curr2++];
		curr++;
	}

	a_str1[curr] = '*';
	a_str2[curr] = '*';
	curr++;

	// Now, print the alignment. We go until we've
	// printed all of the gaps.
	//         s1     e1
	//  -------|----|-+----
	//    -----|----|-+------
	//         s2     e2
	int count_since_last_gap = 1, curr_gap = 0;
	while (curr_gap < tb.gap_count)
	{
		if (count_since_last_gap < ABS(tb.tb[curr_gap]))
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

	// We've printed all the gaps, so print until the end
	// of the alignment.
	int tmp_curr = curr;
	while (curr1 <= tb.end1)
	{
		a_str1[tmp_curr++] = str1[curr1++];
	}
	while (curr2 <= tb.end2)
	{
		a_str2[curr++] = str2[curr2++];
	}

	a_str1[curr] = '*';
	a_str2[curr] = '*';
	curr++;


	// We've printed all the gaps. Print the rest of each string.
	tmp_curr = curr;
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

	// Now actually print the aligned strings
	// to the specified file.
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


}

void delta_print_alignment( FILE * file, const char * str1, const char * str2, delta tb, int line_width )
{
	int curr1, curr2, curr, a_curr1, a_curr2;
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
		if (count_since_last_gap < ABS(tb.tb[curr_gap]))
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
}

void delta_free(delta tb)
{
	if (tb.tb) free(tb.tb);
}
