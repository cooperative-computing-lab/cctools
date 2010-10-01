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
#include "compressed_sequence.h"
#include "overlap.h"
#include "matrix.h"

#include "debug.h"

#ifndef DEFAULT_ALIGNMENT_TYPE
#define DEFAULT_ALIGNMENT_TYPE "ps"
#endif

static int band_width = 0;
static int min_align = 40;  // default SWAT minimal aligment length
static int min_qual_score = 25;  // default SWAT minimal match quality score
static float min_qual = 0.04;

static const char *output_format = "ovl";
static const char *align_type = DEFAULT_ALIGNMENT_TYPE;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Usage: %s [options] <file_name>\n", cmd);
	printf("The most common options are:\n");
	printf(" -a <type>      Alignment type: sw, ps, or banded. (default: %s)\n",align_type);
	printf(" -o <format>    Output format: ovl, align, or matrix. (default: %s)\n",output_format);
	printf(" -k <integer>   Width of band for banded alignment (default is 4%% of maximum alignment.).\n");
	printf(" -m <integer>	SWAT minimal aligment length (default: %d).\n", min_align);
	printf(" -q <integer>	SWAT minimal match quality score (default: %d) -- [1/tb.quality].\n", min_qual_score);
	printf(" -x         	Delete input file after completion.\n");
	printf(" -d <flag>	Enable debugging for this subsystem.\n");
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

	while((c = getopt(argc, argv, "a:o:k:m:q:xd:vh")) != (char) -1) {
		switch (c) {
		case 'a':
			align_type = optarg;
			break;
		case 'o':
			output_format = optarg;
			break;
		case 'k':
			band_width = atoi(optarg);
			break;
		case 'm':
			min_align = atoi(optarg);
			break;
		case 'q':
			min_qual_score = atoi(optarg);
			break;
		case 'x':
			del_input = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			exit(0);
			break;
		default:
		case 'h':
			show_help(argv[0]);
			exit(0);
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

	if(!strcmp(output_format,"ovl")) {
		overlap_write_begin(stdout);
	}

	// outer loop: read first sequence in comparison list

	while((c1=cseq_read(input))) {
	  s1 = cseq_uncompress(c1);
	  cseq_free(c1);

	  // inner loop: read sequences until null (indicating end of list)
	  // then continue again with outer loop.  (two nulls to halt.)

	  while((c2=cseq_read(input))) {
		s2 = cseq_uncompress(c2);
		cseq_free(c2);

		int dir, start1, start2;
		int metadata_valid = sscanf(s2->metadata, "%d %d %d", &dir, &start1, &start2);

		if(metadata_valid<1) {
			fprintf(stderr,"sequence %s did not indicate alignment direction.\n",s2->name);
			exit(1);
		}
	
		if (dir == -1) {
			seq_reverse_complement(s2);
			ori = 'I';
		} else {
			ori = 'N';
		}

		struct matrix *m = matrix_create(s1->num_bases,s2->num_bases);
		if(!m) {
			fprintf(stderr,"sand_align: out of memory when creating alignment matrix.\n");
			exit(1);
		}

		struct alignment *aln;

		if(!strcmp(align_type,"sw")) {

			aln = align_smith_waterman(m,s1->data,s2->data);

		} else if(!strcmp(align_type,"ps")) {

			aln = align_prefix_suffix(m,s1->data,s2->data, min_align);

		} else if(!strcmp(align_type,"banded")) {
			if(metadata_valid<3) {
				fprintf(stderr,"sequence %s did not indicate start positions for the banded alignment.\n",s2->name);
				exit(1);
			}

			if(band_width==0) {
				 // Find the number of errors allowable (the width of the band)
				 // First, find the maximum length of the alignment, then get 4% (min_qual)
				 // of that because it's the maximum amount of errors allowable.

				 int max_alignment_l = align_max(s1->num_bases, s2->num_bases, start1, start2);
				 band_width = ceil(min_qual * max_alignment_l); 
				 if(band_width >= max_alignment_l) band_width = max_alignment_l - 1;
				 if(band_width <= 0) band_width = 1;
			}

			aln = align_banded(m,s1->data, s2->data, start1, start2, band_width);
		} else {
			fprintf(stderr,"unknown alignment type: %s\n",align_type);
			exit(1);
		}

		aln->ori = ori;

		if(!strcmp(output_format,"ovl")) {
			if (aln->quality <= min_qual)
			{
				overlap_write(stdout, aln, s1->name, s2->name);
			}
		} else if(!strcmp(output_format,"matrix")) {
			printf("*** %s alignment of sequences %s and %s (quality %lf):\n\n",align_type,s1->name,s2->name,aln->quality);
			matrix_print(m,s1->data,s2->data);
		} else if(!strcmp(output_format,"align")) {
			printf("*** %s alignment of sequences %s and %s (quality %lf):\n\n",align_type,s1->name,s2->name,aln->quality);
			alignment_print(stdout,s1->data,s2->data,aln,80);
		} else {
			printf("unknown output formt '%s'\n",output_format);
			exit(1);
		}
		
		matrix_delete(m);
		seq_free(s2);
		alignment_delete(aln);
	  }
	  seq_free(s1);
	}

	fclose(input);

	if(!strcmp(output_format,"ovl")) {
		overlap_write_end(stdout);
	}

	if ((argc - optind) == 1 && del_input == 1)
	{
		remove(argv[fileindex]);
	}
	return 0;
}



