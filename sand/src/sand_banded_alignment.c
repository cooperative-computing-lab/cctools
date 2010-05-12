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
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/time.h>
#include "sequence_alignment.h"
#include "sequence_compression.h"
#include "sand_align_macros.h"

#include "debug.h"

//#define MIN_ALIGN 40
int min_align = 40;  // default SWAT minimal aligment length
int min_qual_score = 25;  // default SWAT minimal match quality score
float min_qual = 0.04;

seq get_next_sequence_wrapper(FILE * input)
{
#ifdef COMPRESSION
	cseq c; c = get_next_cseq(input); return uncompress_seq(c);
#else
	return get_next_sequence(input);
#endif
}

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
	seq s1, s2;
	char ori;
	int dir, start1, start2, k, max_alignment_l;
	char c;		// holds command-line options
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
	if(min_qual_score!=0)
		min_qual = 1 / (float)min_qual_score;
	debug(D_DEBUG, "SWAT minimal alignment length: %d, minimal alignment quality score: %d\n", min_align, min_qual_score);

	fileindex = optind;

	if ((argc - optind) == 1)
	{
		input = fopen(argv[fileindex], "r");
		if (!input)
		{
			fprintf(stderr, "ERROR: Could not open file %s for reading.\n", argv[fileindex]);
			exit(1);
		}
	}
	else
	{
		input = stdin;
	}

	s1 = get_next_sequence_wrapper(input);

	print_OVL_envelope_start(stdout);

	while (!feof(input))
	{
#ifdef SPEEDTEST
		gettimeofday(&start_tv, NULL);
#endif
		s2 = get_next_sequence_wrapper(input);

		// If the sequence is null, we reached an end of file, and need to work
		// with a new one.
		if (s2.seq == 0)
		{
			free_seq(s1);
			free_seq(s2);
			s1 = get_next_sequence_wrapper(input);
			continue;
		}

		if (sscanf(s2.metadata, "%d %d %d", &dir, &start1, &start2) != 3)
		{
			fprintf(stderr, "ERROR: Sequence %s (%s) did not provide enough information (direction and band start location)\n", s2.id, s2.metadata);
			exit(1);
		}
		
		if (dir == -1) { revcomp(&s2); ori = 'I'; } //start2 = s2.length - start2; }
		else { ori = 'N'; }

		// Find the number of errors allowable (the width of the band)
		// First, find the maximum length of the alignment, then get 4% (min_qual)
		// of that because it's the maximum amount of errors allowable.
		max_alignment_l = max_alignment_length(s1.length, s2.length, start1, start2);
		k = ceil(min_qual * max_alignment_l); 
		if(k >= max_alignment_l) 
			k = max_alignment_l - 1; //to avoid segmentation faults if q_score=1 or less :-)
		if(k <= 0) k = 1; //to avoid segmentation faults if min_qual=0
		delta tb = banded_prefix_suffix(s1.seq, s2.seq, start1, start2, k);
		tb.ori = ori;
		//fprintf(stderr, "score: %d\n", tb.score);
	//print_alignment(stderr, s1.seq, s2.seq, tb, 80);

		//tb.ori = 'N';
		//revcomp(&s2);
		//delta tb_r = prefix_suffix_align(s1.seq, s2.seq, min_align);
		//tb_r.ori = 'I';

		// A lower score is better
		//if (tb.quality <= tb_r.quality)
		//{
		if (tb.quality <= min_qual)
		{
			print_OVL_message(stdout, tb, s1.id, s2.id);
		}
		//}
		//else
		//{
		//	print_OVL_message(stdout, tb_r, s1.id, s2.id);
		//}

		//print_delta(stdout, tb, s1.id, s2.id, 1);
		//print_OVL_message(stdout, tb, s1.id, s2.id);

		free_seq(s2);
		free_delta(tb);

#ifdef SPEEDTEST
		gettimeofday(&end_tv, NULL);
		time_diff = (end_tv.tv_sec+ (end_tv.tv_usec/1000000.0)) - (start_tv.tv_sec+ (start_tv.tv_usec/1000000.0));
		total_time += time_diff;
		count++;

		if (count % 1000 == 0)
		{
			fprintf(stderr, "%d\t%0.6f\n",count, total_time / (float) count);
		}
#endif
	}
#ifdef SPEEDTEST
	fprintf(stderr, "%d\t%0.6f\n",count, total_time/(float)count);
#endif

	free_seq(s1);
	fclose(input);
	print_OVL_envelope_end(stdout);
	//delete input file if told to do so
	if ((argc - optind) == 1 && del_input == 1)
	{
		remove(argv[fileindex]);
	}
	return 0;
}



