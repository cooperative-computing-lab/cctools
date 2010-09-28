/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>

#include "align.h"
#include "sequence_compression.h"
#include "overlap.h"

#include "debug.h"

static int min_align = 40;  // default SWAT minimal aligment length
static int min_qual_score = 25;  // default SWAT minimal match quality score
static float min_qual = 0.04;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Usage: %s [options] <file_name>\n", cmd);
	printf("The most common options are:\n");
	printf(" -m <integer>	SWAT minimal aligment length (default is %d).\n", min_align);
	printf(" -q <integer>	SWAT minimal match quality score (default is %d) -- [1/tb.quality].\n", min_qual_score);
	printf(" -d <flag>	Enable debugging for this subsystem.\n");
	printf(" -x         	Delete input file after completion.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

int main(int argc, char ** argv)
{
	FILE * input;
	struct seq *s1=0, *s2=0;
	char ori;
	char c;
	int fileindex;
	int del_input=0;

	while((c = getopt(argc, argv, "d:m:q:xvh")) != (char) -1) {
		switch (c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			exit(0);
			break;
		case 'h':
			show_help(argv[0]);
			exit(0);
			break;
		case 'm':
			min_align = atoi(optarg);
			break;
		case 'x':
			del_input = 1;
			break;
		case 'q':
			min_qual_score = atoi(optarg);
			break;
		}
	}

	if(min_qual_score!=0) {
		min_qual = 1 / (float)min_qual_score;
	}

	fileindex = optind;
	if ((argc - optind) == 1)
	{
		input = fopen(argv[fileindex], "r");
		if (!input)
		{
			fprintf(stderr, "ERROR: Could not open file %s for reading.\n", argv[fileindex]);
			exit(1);
		}
	} else {
		input = stdin;
	}

	struct cseq *c1, *c2;

	// outer loop: read first sequence in comparison list

	while((c1=cseq_read(input))) {
	  s1 = cseq_uncompress(c1);
	  cseq_free(c1);

	  overlap_write_begin(stdout);

	  // inner loop: read sequences until null (indicating end of list)
	  // then continue again with outer loop.  (two nulls to halt.)

	  while((c2=cseq_read(input))) {
		s2 = cseq_uncompress(c2);
		cseq_free(c2);

		delta tb;

		#ifdef DO_BANDED_ALIGNMENT
			int dir, start1, start2;

			if (sscanf(s2->metadata, "%d %d %d", &dir, &start1, &start2) != 3)
			{
				fprintf(stderr, "ERROR: Sequence %s (%s) did not provide enough information (direction and band start location)\n", s2->name, s2->metadata);
				exit(1);
			}
	
			if (dir == -1) {
				seq_reverse_complement(s2);
				ori = 'I';
			} else {
				ori = 'N';
			}

			// Find the number of errors allowable (the width of the band)
			// First, find the maximum length of the alignment, then get 4% (min_qual)
			// of that because it's the maximum amount of errors allowable.

			int max_alignment_l = align_max(s1->num_bases, s2->num_bases, start1, start2);
			int k = ceil(min_qual * max_alignment_l); 
			if(k >= max_alignment_l) k = max_alignment_l - 1;
			if(k <= 0) k = 1;

			tb = align_banded(s1->data, s2->data, start1, start2, k);
		#else
			if(atoi(s2->metadata) == -1) {
				seq_reverse_complement(s2);
				ori = 'I';
			} else {
				ori = 'N';
			}

			tb = align_prefix_suffix(s1->data,s2->data, min_align);
		#endif

		tb.ori = ori;

		if (tb.quality <= min_qual)
		{
			overlap_write(stdout, tb, s1->name, s2->name);
		}

		seq_free(s2);
		delta_free(tb);
	  }
	}

	fclose(input);
	overlap_write_end(stdout);

	if ((argc - optind) == 1 && del_input == 1)
	{
		remove(argv[fileindex]);
	}
	return 0;
}



