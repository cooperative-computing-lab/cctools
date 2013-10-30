/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "overlap.h"
#include "macros.h"

void overlap_write_v7(FILE * file, struct alignment *aln, const char *id1, const char *id2)
{
	int ahg, bhg;

	// IDs of overlapping fragments.
	fprintf(file, "%s ", id1);
	fprintf(file, "%s ", id2);

	// Orientation
	fprintf(file, "%c ", aln->ori);

	// calculate overhangs assuming A is on the left
	ahg = aln->start1 + aln->start2;
	bhg = (aln->length2 - 1) - aln->end2;
	if(bhg == 0) {
		bhg = aln->end1 - (aln->length1 - 1);
	}

	if(aln->start2 <= aln->start1 && aln->end2 <= aln->end1) {	// A is on left or B is inside A


	} else if(aln->start1 <= aln->start2 && aln->end1 <= aln->end2) {	// B is on left or A is inside B

		// recalculate overhangs given that B is on right.  Main difference is these should be negative
		ahg = (aln->start2 + aln->start1) * -1;
		bhg = ((aln->length1 - 1) - aln->end1) * -1;
		if(bhg == 0) {
			bhg = (aln->end1 - (aln->length1 - 1)) * -1;
		}


	}
	// How much each piece hangs off the end. If A is on left (or B is contained), these are positive
	// If B is on the left, these are negative.
	fprintf(file, "%d ", ahg);
	fprintf(file, "%d ", bhg);

	double qual = aln->quality;
	qual = qual * 100;


	// Celera defines the quality score as (gaps + mismatches) / MIN(end1, end2)
	fprintf(file, "%.1f %.1f", qual, qual);

	fprintf(file, "\n");
}

void overlap_write_v5(FILE * file, struct alignment *aln, const char *id1, const char *id2)
{
	int ahg, bhg;
	int arh, brh;

	fprintf(file, "{OVL\n");

	// IDs of overlapping fragments.
	fprintf(file, "afr:%s\n", id1);
	fprintf(file, "bfr:%s\n", id2);

	// Orientation
	fprintf(file, "ori:%c\n", aln->ori);

	arh = aln->length1 - aln->end1;	// determine the right portions of sequences that 
	brh = aln->length2 - aln->end2;	// are not in the alignment

	// calculate overhangs assuming A is on the left
	ahg = aln->start1 + aln->start2;
	bhg = (aln->length2 - 1) - aln->end2;
	if(bhg == 0) {
		bhg = aln->end1 - (aln->length1 - 1);
	}

	if(aln->start2 <= aln->start1 && aln->end2 <= aln->end1) {	// A is on left or B is inside A

		if(arh >= brh)
			fprintf(file, "olt:C\n");	// b is contained in A (or the same as A)
		else
			fprintf(file, "olt:D\n");	// dovetail - suffix/prefix alignment

	} else if(aln->start1 <= aln->start2 && aln->end1 <= aln->end2) {	// B is on left or A is inside B

		// recalculate overhangs given that B is on right.  Main difference is these should be negative
		ahg = (aln->start2 + aln->start1) * -1;
		bhg = ((aln->length1 - 1) - aln->end1) * -1;
		if(bhg == 0) {
			bhg = (aln->end1 - (aln->length1 - 1)) * -1;
		}

		if(brh >= arh)
			fprintf(file, "olt:C\n");	// a is contained in B 
		else
			fprintf(file, "olt:D\n");	// dovetail - suffix/prefix alignment

	} else
		fprintf(file, "olt:D\n");	// Default to D to mimic Celera.

	// How much each piece hangs off the end. If A is on left (or B is contained), these are positive
	// If B is on the left, these are negative.
	fprintf(file, "ahg:%d\n", ahg);
	fprintf(file, "bhg:%d\n", bhg);

	// Celera defines the quality score as (gaps + mismatches) / MIN(end1, end2)
	fprintf(file, "qua:%f\n", aln->quality);

	// This is the length of the overlap
	fprintf(file, "mno:%d\n", MIN(aln->end1 - aln->start1, aln->end2 - aln->start2));
	fprintf(file, "mxo:%d\n", aln->score);

	// Polymorphism count.
	fprintf(file, "pct:0\n");	// Again, try to match Celera, where this is set to 0 and unchanged later

	fprintf(file, "}\n");
}

void overlap_write_begin(FILE * file)
{
	fprintf(file, "[\n");
}

void overlap_write_end(FILE * file)
{
	fprintf(file, "]\n");
}

/* vim: set noexpandtab tabstop=4: */
