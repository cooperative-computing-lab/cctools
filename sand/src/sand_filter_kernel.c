/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <math.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include "cctools.h"
#include "memory_info.h"
#include "debug.h"
#include "macros.h"

#include "sequence_filter.h"

static char *progname = "sand_filter_kernel";
static int num_seqs;
static int kmer_size = 22;
static int window_size = 22;
static unsigned long max_mem_kb = ULONG_MAX;

static char *repeat_filename = 0;
static char *sequence_filename = 0;
static char *second_sequence_filename = 0;
static char *output_filename = 0;

#define MEMORY_FOR_MERS(max_mem) (MIN((get_mem_avail()*0.95),(max_mem))-get_mem_usage())
#define DYNAMIC_RECTANGLE_SIZE(max_mem) (MEMORY_FOR_MERS((max_mem))/KB_PER_SEQUENCE)

static unsigned long get_mem_avail()
{
	UINT64_T total, avail;
	memory_info_get(&total, &avail);
	return (unsigned long) avail / 1024;
}

static unsigned long get_mem_usage()
{
	UINT64_T rss, total;
	memory_usage_get(&rss, &total);
	return rss / 1024;
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <sequences file> [second sequence file]\n", cmd);
	printf("where options are:\n");
	printf(" -s <size>      Size of \"rectangle\" for filtering. You can determine\n");
	printf("                the size dynamically by passing in d rather than a number.\n");
	printf(" -r <file>      A meryl file of repeat mers to be filtered out.\n");
	printf(" -k <number>    The k-mer size to use in candidate selection (default is 22).\n");
	printf(" -w <number>    The minimizer window size to use in candidate selection (default is 22).\n");
	printf(" -o <filename>  The output file. Default is stdout.\n");
	printf("                output file to indicate it has ended (default is nothing)\n");
	printf(" -d <subsys>    Enable debug messages for this subsystem.  Try 'd -all' to start .\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void get_options(int argc, char **argv, const char *progname)
{
	signed char c;

	while((c = getopt(argc, argv, "d:r:s:k:w:f:o:vh")) > -1) {
		switch (c) {
		case 'r':
			repeat_filename = optarg;
			break;
		case 's':
			if(*optarg == 'd') {
				rectangle_size = -1;
				max_mem_kb = strtol(optarg + 1, 0, 10);
				if(max_mem_kb <= 0)
					max_mem_kb = ULONG_MAX;
			} else
				rectangle_size = atoi(optarg);
			if(rectangle_size == 0) {
				fprintf(stderr, "Invalid rectangle size %s\n", optarg);
				exit(1);
			}
			break;
		case 'k':
			kmer_size = atoi(optarg);
			break;
		case 'w':
			window_size = atoi(optarg);
			break;
		case 'o':
			output_filename = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, progname);
			exit(0);
		case 'h':
			show_help(progname);
			exit(0);
		}
	}

	if(argc - optind == 1) {
		sequence_filename = argv[optind++];
	} else if(argc - optind == 2) {
		sequence_filename = argv[optind++];
		second_sequence_filename = argv[optind++];
	} else {
		show_help(progname);
		fprintf(stderr, "Incorrect number of arguments. Expected 1 or 2, got %d\n", argc - optind);
		exit(1);
	}

}

int main(int argc, char **argv)
{
	FILE *input;
	FILE *repeats = 0;
	FILE *output;

	int start_x, end_x, start_y, end_y;

	debug_config(progname);
	get_options(argc, argv, progname);

	cctools_version_debug(D_DEBUG, argv[0]);

	unsigned long start_mem, cand_mem, table_mem;

	input = fopen(sequence_filename, "r");
	if(!input) fatal("couldn't open %s: %s\n",sequence_filename,strerror(errno));

	if(repeat_filename) {
		repeats = fopen(repeat_filename, "r");
		if(!repeats) fatal("couldn't open %s: %s\n",repeat_filename,strerror(errno));
	}

	if(output_filename) {
		output = fopen(output_filename, "w");
	} else {
		output = stdout;
	}

	// Data is in the form:
	// >id metadata
	// data
	// >id metadata
	// data
	// >>
	// ...

	set_k(kmer_size);
	set_window_size(window_size);

	// If we only give one file, do an all vs. all
	// on them.
	if(!second_sequence_filename) {
		num_seqs = load_seqs(input);
		start_x = 0;
		end_x = num_seqs;
		start_y = 0;
		end_y = num_seqs;
	}
	// If we had two files, do not compare ones from
	// the same file to each other.
	else {
		FILE *input2 = fopen(second_sequence_filename, "r");
		if(!input2) {
			fprintf(stderr, "Could not open file %s for reading.\n", second_sequence_filename);
			exit(1);
		}
		num_seqs = load_seqs_two_files(input, &end_x, input2, &end_y);
		start_x = 0;
		start_y = end_x;
		debug(D_DEBUG,"First file contains %d sequences, stored from (%d,%d].\n", end_x, start_x, end_x);
		debug(D_DEBUG,"Second file contains %d sequences, stored from (%d,%d].\n", end_y-end_x, start_y, end_y);
	}
	fclose(input);

	debug(D_DEBUG,"Loaded %d sequences\n",num_seqs);

	init_cand_table(num_seqs * 5);
	init_mer_table(num_seqs * 5);

	if(repeats) {
		int repeat_count = init_repeat_mer_table(repeats, 2000000, 0);
		fclose(repeats);
		debug(D_DEBUG,"Loaded %d repeated mers\n", repeat_count);
	}

	if(rectangle_size == -1) {
		// Do get_mem_avail*0.95 to leave some memory for overhead
		rectangle_size = DYNAMIC_RECTANGLE_SIZE(max_mem_kb);
		debug(D_DEBUG,"Mem avail: %lu, rectangle size: %d\n",(unsigned long)MEMORY_FOR_MERS(max_mem_kb), rectangle_size);
	}

	int curr_start_x = start_x;
	int curr_start_y = start_y;

	candidate_t *output_list = 0;
	int num_in_list;

	while(curr_start_y < end_y) {
		while(curr_start_x < end_x) {
			if(start_x == start_y) {
				debug(D_DEBUG,"Loading mer table (%d,%d)\n", curr_rect_x, curr_rect_y);
			} else {
				debug(D_DEBUG,"Loading mer table for [%d,%d) and [%d,%d)\n",curr_start_x, MIN(curr_start_x + rectangle_size, end_x), curr_start_y, MIN(curr_start_y + rectangle_size, end_y));
			}

			start_mem = get_mem_usage();

			load_mer_table_subset(curr_start_x, MIN(curr_start_x + rectangle_size, end_x), curr_start_y, MIN(curr_start_y + rectangle_size, end_y), (curr_start_x == curr_start_y));

			table_mem = get_mem_usage();

			debug(D_DEBUG,"Finished loading, now generating candidates\n");
			debug(D_DEBUG,"Memory used: %lu\n", table_mem - start_mem);

			generate_candidates();
			cand_mem = get_mem_usage();

			debug(D_DEBUG,"Total candidates generated: %llu\n", (long long unsigned int) total_cand);
			debug(D_DEBUG,"Candidate memory used: %lu\n", cand_mem - table_mem);

			output_list = retrieve_candidates(&num_in_list);
			output_candidate_list(output, output_list, num_in_list);
			free(output_list);
			fflush(output);

			debug(D_DEBUG,"Now freeing\n");

			free_cand_table();
			free_mer_table();

			debug(D_DEBUG,"Successfully output and freed!\n");

			curr_rect_x++;
			curr_start_x += rectangle_size;
		}
		curr_rect_y++;
		curr_start_y += rectangle_size;
		curr_rect_x = curr_rect_y;
		if(start_y == 0) {
			curr_start_x = curr_start_y;
		} else {
			curr_start_x = start_x;
		}
	}

	fclose(output);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
