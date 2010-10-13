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

#include "macros.h"
#include "debug.h"

static int min_align = 0;
static double min_qual = 1.0;

static const char *output_format = "ovl";
static const char *align_type = "banded";

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
	printf(" -m <integer>	Minimum aligment length (default: %d).\n", min_align);
	printf(" -q <integer>	Minimum match quality (default: %.2lf)\n",min_qual);
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
		case 'm':
			min_align = atoi(optarg);
			break;
		case 'q':
			min_qual = atof(optarg);
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

	fileindex = optind;
	if ((argc - optind) == 1) {
		input = fopen(argv[fileindex], "r");
		if (!input) {
			fprintf(stderr, "sand_align_kernel: couldn't open %s: %s\n",argv[fileindex],strerror(errno));
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

		if(metadata_valid>=1 && dir==-1) {
			seq_reverse_complement(s2);
			ori = 'I';
		} else {
			ori = 'N';
		}

		struct matrix *m = matrix_create(s1->num_bases,s2->num_bases);
		if(!m) {
			fprintf(stderr,"sand_align_kernel: out of memory when creating alignment matrix.\n");
			exit(1);
		}

		struct alignment *aln;

		if(!strcmp(align_type,"sw")) {

			aln = align_smith_waterman(m,s1->data,s2->data);

		} else if(!strcmp(align_type,"ps")) {

			aln = align_prefix_suffix(m,s1->data,s2->data, min_align);

		} else if(!strcmp(align_type,"banded")) {
			if(metadata_valid<3) {
				fprintf(stderr,"sand_align_kernel: sequence %s did not indicate start positions for the banded alignment.\n",s2->name);
				exit(1);
			}

			/* The width of the band is proportional to the desired quality of the match. */

			int k = 2 + min_qual * MIN(s1->num_bases,s2->num_bases) / 2.0;
			if(k<5) k = 5;

			aln = align_banded(m,s1->data, s2->data, start1, start2, k);
		} else {
			fprintf(stderr,"unknown alignment type: %s\n",align_type);
			exit(1);
		}

		aln->ori = ori;

		if(aln->quality <= min_qual) {
			if(!strcmp(output_format,"ovl")) {
				overlap_write(stdout, aln, s1->name, s2->name);
			} else if(!strcmp(output_format,"matrix")) {
				printf("*** %s alignment of sequences %s and %s (quality %lf):\n\n",align_type,s1->name,s2->name,aln->quality);
				matrix_print(m,s1->data,s2->data);
			} else if(!strcmp(output_format,"align")) {
				printf("*** %s alignment of sequences %s and %s (quality %lf):\n\n",align_type,s1->name,s2->name,aln->quality);
				alignment_print(stdout,s1->data,s2->data,aln);
			} else {
				printf("unknown output format '%s'\n",output_format);
				exit(1);
			}
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



