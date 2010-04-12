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
int min_align = 40;  // default SWAT minimal score

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
	printf(" -m <integer>   SWAT minimal score.\n");
	printf(" -d <flag>	Enable debugging for this subsystem.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

int main(int argc, char ** argv)
{
	FILE * input;
	seq s1, s2;
	char ori;
	char c;		// holds command-line options
	int fileindex;

	while((c = getopt(argc, argv, "d:m:vh")) != (char) -1) {
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
		}
	}
	debug(D_DEBUG, "SWAT minimal score: %d\n", min_align);

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

		//print_sequence(stdout, s1);
		//print_sequence(stdout, s2);
		if (atoi(s2.metadata) == -1) { revcomp(&s2); ori = 'I'; }
		else { ori = 'N'; }
		
		delta tb = prefix_suffix_align(s1.seq, s2.seq, min_align);
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
		if (tb.quality <= 0.04)
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
	return 0;
}



