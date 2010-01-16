#include <stdlib.h>
#include <stdio.h>
#include <string.h>
//#include <cstring.h>
#include <ctype.h>
#include <limits.h>
#include "sequence_alignment.h"
#include "sand_align_macros.h"

#define TB_LEFT -1
#define TB_UP 1
#define TB_DIAG 0
#define TB_END 2

#define MAX_ID 100
#define MAX_METADATA 100

#define INIT_PREFIX_SUFFIX 0
#define INIT_SW 1

#define CHECK_UP   1
#define CHECK_DIAG 2
#define CHECK_LEFT 4

int score_mismatch = 1;
int score_match_open = 0;
int score_match_extend = 0;
int score_gap_open = 1;
int score_gap_extend = 1;

void print_matrix(FILE* file, cell ** matrix, const char * str1, int length1, const char * str2, int length2);
void print_banded_matrix(FILE * file, cell ** band, const char * str1, int length1, int start1, const char * str2, int length2, int start2, int k);
cell new_score(cell ** matrix, int i, int j, const char * str1, const char * str2);
cell new_score_gap_extensions(cell ** matrix, int i, int j, const char * str1, const char * str2);
cell new_score_maximize(cell ** matrix, int i, int j, const char * str1, const char * str2);
cell new_score_banded(cell ** band, int band_row, int band_col, int matrix_row, int matrix_col, const char * str1, const char * str2, int which);
void choose_best(cell ** matrix, int * i, int * j, int length1, int length2, int min_align);
delta generate_traceback(cell ** matrix, int i, int j, int length1, int length2, int min_score);
void convert_to_upper(char * str);
void chomp(char * str);
void process_string(char * str);
void seq_cat(seq * sequence, char * new_str);
cell ** init_matrix(int length1, int length2, int type);
void free_matrix(cell ** matrix, int length2);
int get_last_simple_row(int diag, int k, int length1, int length2);


delta local_align(const char * str1, const char * str2)
{
	int length1 = strlen(str1);
	int length2 = strlen(str2);
	int i,j;
	delta tb;
	int best_i, best_j, best_score;

#ifdef TRUNCATE
	length1 = length1/TRUNCATE;
	length2 = length2/TRUNCATE;
#endif

	//fprintf(stderr, "init matrix\n");
	cell ** matrix = init_matrix(length1, length2, INIT_PREFIX_SUFFIX);
	//fprintf(stderr, "filling out matrix\n");
	best_i = 0;
	best_j = 0;
	best_score = 0;
	for (i=1; i<=length1; i++)
	{
		for (j=1; j<=length2; j++)
		{
			//matrix[j][i] = new_score(matrix, i, j, str1, str2);
			matrix[j][i] = new_score_maximize(matrix, i, j, str1, str2);
			if (matrix[j][i].score < 0)
			{
				matrix[j][i].score = 0;
			}
			if (matrix[j][i].score >= best_score)
			{
				best_score = matrix[j][i].score;
				best_i = i;
				best_j = j;
			}
		}
	}


	tb = generate_traceback(matrix, best_i, best_j, length1, length2, 0);

	//print_matrix(stdout, matrix, str1, length1, str2, length2);
	//fprintf(stderr, "best row: %d, best column: %d\n", best_j, best_i);

	fflush(stdout);
	free_matrix(matrix, length2);

	return tb;


}


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

	tb = generate_traceback(matrix, length1, length2, length1, length2, INT_MIN);

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
			matrix[j][i] = new_score(matrix, i, j, str1, str2);
			//matrix[j][i] = new_score_gap_extensions(matrix, i, j, str1, str2);
		}
	}
	//print_matrix(stdout, matrix, str1, length1, str2, length2);

	int best_i, best_j;
	choose_best(matrix, &best_i, &best_j, length1, length2, min_align);

	//fprintf(stdout, "best: %d, %d\n", best_i, best_j);

	tb = generate_traceback(matrix, best_i, best_j, length1, length2, INT_MIN);

	//print_matrix(stdout, matrix, str1, length1, str2, length2);
	//fprintf(stderr, "best row: %d, best column: %d\n", best_j, best_i);

	//fflush(stdout);
	free_matrix(matrix, length2);

	return tb;
}

void band2matrix(int band_row, int band_col, int diag, int k, int * matrix_row, int * matrix_col)
{
	*matrix_row = (diag < -k) ? ((band_row) + (-diag-k)) : band_row;
	*matrix_col = (*matrix_row + diag) + (band_col - k);
}

int start_band_left(cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int band_row = 0;
	int matrix_row, first_full_row, band_first_col;
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
		//fprintf(stderr, "Calculating band[%d][%d] (x)\n", band_row, band_col);
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
		//fprintf(stderr, "Calculating band[%d][%d] (a)\n", band_row, band_col);
		band[band_row][band_col].score = 0;
		band[band_row][band_col].tb = TB_END;
		matrix_col++;

		// For all but the last column, look at all three recursions.
		for (band_col = band_first_col+1; band_col < (width-1); band_col++)
		{
			//fprintf(stderr, "Calculating band[%d][%d] (b)\n", band_row, band_col);
			band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, CHECK_UP | CHECK_LEFT | CHECK_DIAG);
			matrix_col++;
		}

		// Now, for the last column, just look at left and diag, don't
		// check up.
		//fprintf(stderr, "Calculating band[%d][%d] (c)\n", band_row, band_col);
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
int start_band_upper(cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
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

int get_last_simple_row(int diag, int k, int length1, int length2)
{
	// Figure out when the border of the band will hit the right
	// side of the matrix:
	int right_side_intersect_row = length1 - (diag+k);

	// If the right border of the band intersects with the right
	// side of the matrix below the last row, then just stop
	// at the last row. Otherwise stop where it intersects.
	//fprintf(stderr, "right_side_intersect_row: %d, length2: %d\n", right_side_intersect_row, length2);
	return MIN(right_side_intersect_row, length2);

}

void choose_best_banded(cell ** band, int * best_row_ret, int * best_col_ret, int length1, int length2, int k, int band_row, int cols_in_last_row, int rows_in_last_col)
{
	int band_col;
	int best_row, best_col, best_score;
	int i;

	best_score = INT_MAX;
	//fprintf(stderr, "cols_in_last_row: %d, rows_in_last_col: %d\n", cols_in_last_row, rows_in_last_col);

	// Now check the last row.
	for (band_col=cols_in_last_row; band_col>=0; band_col--)
	{
		//fprintf(stderr, "choose_best (last row): band_row: %d, band_col: %d, score: %d\n", band_row, band_col, band[band_row][band_col].score);
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
		//fprintf(stderr, "choose_best (last col): band_row: %d, band_col: %d, score: %d\n", band_row, band_col, band[band_row][band_col].score);
		if (band[band_row][band_col].score < best_score)
		{
			best_score = band[band_row][band_col].score;
			best_row = band_row;
			best_col = band_col;
		}
		//band_col--;
	}

	*best_row_ret = best_row;
	*best_col_ret = best_col;
}

delta generate_traceback_banded(cell ** band, int best_row, int best_col, int length1, int length2, int k, int diag)
{
	delta tb;
	int band_row, band_col, curr_tb_type, count;
	int count_since_last, last_gap_type;
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
	//fprintf(stderr, "band_row: %d, band_col: %d\nbest_row: %d, best_col: %d\n", band_row, band_col, best_row, best_col);
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

delta banded_prefix_suffix(const char * str1, const char * str2, int start1, int start2, int k)
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

#ifdef TRUNCATE
	length1 = length1/TRUNCATE;
	length2 = length2/TRUNCATE;
#endif

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

	//fprintf(stderr, "init matrix\n");
	cell ** band = init_matrix(width, lastrow, INIT_PREFIX_SUFFIX);
	//fprintf(stderr, "filling out matrix\n");
	//print_banded_matrix(stdout, band, str1, length1, start1, str2, length2, start2, k); return tb;

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
		//fprintf(stderr, "calculating band[%d][%d] (a)\n", band_row, 0);
		band[band_row][0] = new_score_banded(band, band_row, 0, matrix_row, matrix_col, str1, str2, CHECK_UP | CHECK_DIAG);
		matrix_col++;

		// All but the last check all three recursions
		for (band_col = 1; band_col < last_col-1; band_col++)
		{
			//fprintf(stderr, "calculating band[%d][%d] (b)\n", band_row, band_col);
			band[band_row][band_col] = new_score_banded(band, band_row, band_col, matrix_row, matrix_col, str1, str2, CHECK_LEFT | CHECK_DIAG | CHECK_UP);
			matrix_col++;
		}

		// The last col only checks left and diag, not up.
		//fprintf(stderr, "calculating band[%d][%d] (c)\n", band_row, band_col);
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

	//fprintf(stderr, "last_col: %d\n", last_col);

	choose_best_banded(band, &best_row, &best_col, length1, length2, k, band_row-1, last_col, width-last_col-1 );
	tb = generate_traceback_banded(band, best_row, best_col, length1, length2, k, diag);

	// Free the banded matrix.
	int j;
	for (j=0; j<= lastrow; j++)
	{
		free(band[j]);
	}
	free(band);
	//fprintf(stderr, "best_row: %d, best_col: %d\n", best_row, best_col);

	//print_banded_matrix(stdout, band, str1, length1, start1, str2, length2, start2, k);

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

/*
Assumes we are trying to maximize the score rather than
minimize it.  Used for local alignment.
*/
cell new_score_maximize(cell ** matrix, int i, int j, const char * str1, const char * str2)
{
	cell max;
	int incr;

	max.score = 0;

	// If this is a gap extension, don't increase the score.
	// If it's a gap open, increase the score.
	incr = (matrix[j-1][i].tb == TB_UP) ? score_gap_extend : score_gap_open;
	if (matrix[j-1][i].score+incr >= max.score)
	{
		max.score = matrix[j-1][i].score+incr;
		max.tb = TB_UP;
	}

	// If this is a gap extension, don't increase the score.
	// If it's a gap open, increase the score.
	incr = (matrix[j][i-1].tb == TB_LEFT) ? score_gap_extend : score_gap_open;
	if (matrix[j][i-1].score+incr >= max.score)
	{
		max.score = matrix[j][i-1].score+incr;
		max.tb = TB_LEFT;
	}

	if (str1[i-1] == str2[j-1])
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
	if (matrix[j-1][i-1].score + incr >= max.score)
	{
		max.score = matrix[j-1][i-1].score + incr;
		max.tb = TB_DIAG;
	}

	//printf("(%d, %d) %c %c = %d (%d, %d)\n", i, j, str1[i-1], str2[j-1], incr, max.score, max.tb);
	return max;
}


/*
Assumes we are trying to maximize the score rather than
minimize it.  Used for local alignment.
*/
cell new_score_banded(cell ** band, int band_row, int band_col, int matrix_row, int matrix_col, const char * str1, const char * str2, int which)
{
	cell min, recurrence;
	int incr;

	recurrence.score = 0;
	recurrence.tb = 0;
	min.score = INT_MAX;

	// Check if it's a gap (up).
	if (which & CHECK_UP)
	{
		recurrence = band[band_row-1][band_col+1];
		//fprintf(stderr, "  up score: %d\n", recurrence.score);
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
		//fprintf(stderr, "  left score: %d\n", recurrence.score);
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
		//fprintf(stderr, "  diag score: %d, matrix_col: %d, matrix_row: %d\n", recurrence.score, matrix_col, matrix_row);
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
		//fprintf(stderr, "  incr: %d\n", incr);
		if (recurrence.score+incr < min.score)
		{
			min.score = recurrence.score+incr;
			min.tb = TB_DIAG;
		}
	}

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
	//fprintf(stdout, "quality (%d, %d): %d / %d = %f\n", *best_i, *best_j, matrix[*best_j][*best_i].score, min(*best_i, *best_j), quality);

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
	//fprintf(stdout, "best quality (%d, %d): %d / %d = %f\n", *best_i, *best_j, matrix[*best_j][*best_i].score, min(*best_i, *best_j), min_qual);
}

delta generate_traceback(cell ** matrix, int i, int j, int length1, int length2, int min_score)
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
			/*if (matrix[curr_j][curr_i].score > matrix[curr_j-1][curr_i-1].score)
			{
				tb.mismatch_count++;
			}*/
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
	tb.quality = (float)(tb.gap_count + tb.mismatch_count) / (float) min(i, j);
	
	return tb;
}

// Find the maximum alignment length given the lengths and the start
// positions of the exact match. Assume the start position has already
// been corrected for distance.
int max_alignment_length(int length1, int length2, int start1, int start2)
{
	return MIN(start1, start2) + MIN(length1 - start1, length2 - start2);
}

void print_delta(FILE * file, delta tb, const char * id1, const char * id2)
{
	int i, start2, end2;
	if (tb.ori == 'N')
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

void print_local(FILE * file, const char * str1, const char * str2, delta tb, int line_width)
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

void print_alignment(FILE * file, const char * str1, const char * str2, delta tb, int line_width)
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
	for(i=0; i<length1; i++)
	{
		fprintf(file, "    %c | ", str1[i]);
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

int print_band_left(FILE * file, cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int i;
	int band_row = 0;
	int matrix_row;
	int first_full_row;
	int width = (2*k)+1;
	int band_col;
	int band_first_col;

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

	// Print any empty rows before the band actually starts.
	for (i=0; i<matrix_row; i++)
	{
		if (i == 0)
		{
			fprintf(file, "  X | \n");
		}
		else
		{
			fprintf(file, "  %c | \n", str2[i-1]);
		}
	}
	while (band_row < first_full_row)
	{
		if (matrix_row == 0)
		{
			fprintf(file, "  X | ");
		}
		else
		{
			fprintf(file, "  %c | ", str2[matrix_row-1]);
		}

		for (band_col = band_first_col; band_col < width; band_col++)
		{
			fprintf(file, "%c %3d | ", arrow(band, band_col, band_row), band[band_row][band_col].score);
			//fprintf(file, "%2d %3d| ", band_col, matrix_col);
			//fprintf(file, "%2d %3d| ", band_row, band_col);
		}
		fprintf(file, " %d %d\n", band_row, matrix_row);
		// Now, increment everything
		band_row++;
		matrix_row++;
		band_first_col--;
	}

	// We actually filled out the first full row, but don't tell
	// the rest of the code that. The reason is so that the rest
	// of the code doesn't have to care about making sure the
	// first column is set to 0.
	*matrix_row_ret = matrix_row;
	*matrix_col_ret = 1;
	return first_full_row;
	
}

int print_band_lower(FILE * file, cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int i;
	int band_row = 0;
	int matrix_row = start2 - k;
	int first_full_row = (2*k)+1;
	int width = (2*k)+1;

	// In the first row, the first (and only) column populated
	// is the last one, but this is actually the first column
	// in the DP matrix.
	int band_first_col = width-1;
	int band_col;

	for (i=0; i<matrix_row; i++)
	{
		if (i == 0)
		{
			fprintf(file, "  X | \n");
		}
		else
		{
			fprintf(file, "  %c | \n", str2[i-1]);
		}
	}
	while (band_row < first_full_row)
	{
		if (matrix_row == 0)
		{
			fprintf(file, "  X | ");
		}
		else
		{
			fprintf(file, "  %c | ", str2[matrix_row-1]);
		}

		for (band_col = band_first_col; band_col < width; band_col++)
		{
			fprintf(file, "%c %3d | ", arrow(band, band_col, band_row), band[band_row][band_col].score);
			//fprintf(file, "%2d %3d| ", band_col, matrix_col);
			//fprintf(file, "%2d %3d| ", band_row, band_col);
		}
		fprintf(file, " %d %d\n", band_row, matrix_row);
		// Now, increment everything
		band_row++;
		matrix_row++;
		band_first_col--;
	}

	// We actually filled out the first full row, but don't tell
	// the rest of the code that. The reason is so that the rest
	// of the code doesn't have to care about making sure the
	// first column is set to 0.
	*matrix_row_ret = matrix_row;
	*matrix_col_ret = 1;
	return first_full_row;
}

int print_band_low_corner(FILE * file, cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int band_row = 0;
	int matrix_row = 0;
	int first_full_row = start2+k+1;
	int width = (2*k)+1;
	int band_col;

	int band_first_col = width - (k-start2+1);

	while (band_row < first_full_row)
	{
		if (matrix_row == 0)
		{
			fprintf(file, "  X | ");
		}
		else
		{
			fprintf(file, "  %c | ", str2[matrix_row-1]);
		}

		// print out the row from 

		for (band_col = band_first_col; band_col < width; band_col++)
		{
			fprintf(file, "%c %3d | ", arrow(band, band_col, band_row), band[band_row][band_col].score);
			//fprintf(file, "%2d %3d| ", band_col, matrix_col);
			//fprintf(file, "%2d %3d| ", band_row, band_col);
		}
		fprintf(file, " %d %d\n", band_row, matrix_row);
		// Now, increment everything
		band_row++;
		matrix_row++;
		band_first_col--;

	}
	*matrix_row_ret = matrix_row;
	*matrix_col_ret = 1;
	return first_full_row;

}

int print_band_high_corner(FILE * file, cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
    return 0;
}

int print_band_upper(FILE * file, cell ** band, int k, const char * str1, int start1, const char * str2, int start2, int * matrix_row_ret, int * matrix_col_ret)
{
	int band_row = 0;
	int band_col;
	int i;
	int width = (2*k)+1;

	// Print everything before the row starts.
	fprintf(file, "  X | ");
	for (i=0; i < start1 - k; i++)
	{
		fprintf(file, "        ");
	}

	// Print the first row.
	for (band_col=0; band_col < width; band_col++)
	{
			fprintf(file, "%c %3d | ", arrow(band, band_col, band_row), band[band_row][band_col].score);
			//fprintf(file, "%2d %3d| ", band_col, start1-k+band_col);
			//fprintf(file, "%2d %3d| ", band_row, band_col);
	}
	fprintf(file, "\n");

	*matrix_row_ret = 1;
	*matrix_col_ret = start1 - k+1;
	return 1;
}

void print_banded_matrix(FILE * file, cell ** band, const char * str1, int length1, int start1, const char * str2, int length2, int start2, int k)
{
	int i;
	int diag = start1 - start2;
	int band_row, matrix_row, matrix_col, last_simple_row, band_col;
	int width = 2*k+1;
	int last_col;

	// Print the first row (str1)
	fprintf(file, "    |     X | ");
	for(i=0; i<length1; i++)
	{
		fprintf(file, "    %c | ", str1[i]);
	}
	fprintf(file, "\n");

	if (diag <= -k)
	{
		//band_row = print_band_lower(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
		band_row = print_band_left(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}
	else if (diag < 0)
	{
		//band_row = print_band_low_corner(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
		band_row = print_band_left(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}
	else if (diag < k) // assert: diag >= 0
	{
		//band_row = print_band_high_corner(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
		band_row = print_band_left(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}
	else // (diag >= k)
	{
		band_row = print_band_upper(file, band, k, str1, start1, str2, start2, &matrix_row, &matrix_col);
	}

	last_simple_row = get_last_simple_row(diag, k, length1, length2);
	last_col = width;
	fprintf(stderr, "last_simple_row: %d\n", last_simple_row);
	while (matrix_row <= MIN(length2, last_simple_row+width-1))
	{
		// First, print the character for this row.
		fprintf(file, "  %c | ", str2[matrix_row-1]);

		// Then, print blank space in the matrix up until
		// where the band starts.
		for (i=0; i<matrix_col; i++)
		{
			fprintf(file, "        ");
		}
		for (band_col = 0; band_col < last_col; band_col++)
		{
			fprintf(file, "%c %3d | ", arrow(band, band_col, band_row), band[band_row][band_col].score);
			//fprintf(file, "%2d %3d| ", band_row, band_col);
		}
		fprintf(file, " %d %d\n", band_row, matrix_row);

		band_row++;
		matrix_row++;
		matrix_col++;

		if (matrix_row > last_simple_row) last_col--;
	}

	// Print the rest of the rows, even if they're blank.
	for (i=matrix_row-1; i<length2; i++)
	{
		fprintf(file, "  %c |\n", str2[i]);
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
	int length;
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

size_t sprint_seq(char * buf, seq s)
{
	int res = 0;
	size_t size = 0;
	char * ins = buf;
	res = sprintf(ins, ">%s %d %d", s.id, s.length, s.length);
	ins += res;
	size += res;

	if (strlen(s.metadata) > 0)
	{
		res = sprintf(ins, " ");
		ins += res;
		size += res;
	}

	res = sprintf(ins, "%s\n%s\n", s.metadata, s.seq);
	ins += res;
	size += res;

	return size;
}

void print_sequence(FILE * file, seq s)
{
	fprintf(file, ">%s %d %d", s.id, s.length, s.length);
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

	// Get the first line of the file, compile the regexp.
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

	sscanf(line, ">%s %d %d %[^\n]\n", sequence.id, &bases, &bytes, sequence.metadata);

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

int sequence_count(FILE * file)
{
	int count = 0;
	char line[MAX_STRING];
	char id[MAX_STRING];
	int length, bytes;
	long int start_pos = ftell(file);

	while (!feof(file))
	{
		if (fgets(line, MAX_STRING, file) == 0) break;
		if ((line[0] == '>') && (line[1] != '>'))
		{
			count++;
			sscanf(line, ">%s %d %d", id, &length, &bytes);
			fseek(file, bytes+1, SEEK_CUR);
		}
	}
	fseek(file, start_pos, SEEK_SET);
	return count;
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




