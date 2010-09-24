
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "overlap.h"
#include "macros.h"

void overlap_write(FILE * file, delta tb, const char * id1, const char * id2)
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

	// If ahg and bhg are of opposite signs, then it's a containment.
	// If they are the same sign, it's a dovetail.
	if (ahg*bhg < 0) { olt = 'C'; }
	else { olt = 'D'; }
	fprintf(file, "olt:D\n");  // Always put D to mimic Celera more closely.

	// How much each piece hangs off the end. Not sure what to do
	// for containment overlaps, or really what this means at all.
	fprintf(file, "ahg:%d\n", ahg);
	fprintf(file, "bhg:%d\n", bhg);

	// Again, need to do more work to see how quality is computed.
	// For now just making something up.
	// As far as I can tell, Celera defines the quality score as
	// (gaps + mismatches) / MIN(end1, end2)
	fprintf(file, "qua:%f\n", tb.quality);

	// This is the length of the overlap
	fprintf(file, "mno:%d\n", MIN(tb.end1 - tb.start1, tb.end2 - tb.start2));
	fprintf(file, "mxo:%d\n", tb.score);

	// Polymorphism count.
	//fprintf(file, "pct:%d\n", tb.mismatch_count);
	fprintf(file, "pct:0\n");  // Again, try to match Celera

	fprintf(file, "}\n");
}

void overlap_write_begin( FILE * file )
{
    fprintf(file, "[\n");
}

void overlap_write_end( FILE * file )
{
    fprintf(file, "]\n");
}
