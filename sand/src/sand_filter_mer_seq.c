/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <sys/time.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

#include "sequence_alignment.h"
#include "sequence_compression.h"
#include "sand_align_macros.h"
#include "sequence_filter.h"


extern int rectangle_size;
extern int curr_rect_x;
extern int curr_rect_y;
extern int start_time;
extern int MER_TABLE_BUCKETS;
extern int CAND_TABLE_BUCKETS;
int num_seqs;
extern unsigned long total_cand;

static int kmer_size = 22;
static int window_size = 22;
static int output_format = CANDIDATE_FORMAT_LINE;
static int verbose_level = 0;
static char end_char = 0;
static unsigned long max_mem_kb = ULONG_MAX;

static char * repeat_filename = 0;
static char * sequence_filename = 0;
static char * second_sequence_filename = 0;
static char * output_filename = 0;

#define MEMORY_FOR_MERS(max_mem) (MIN((get_mem_avail()*0.95),(max_mem))-get_mem_usage())
#define DYNAMIC_RECTANGLE_SIZE(max_mem) (MEMORY_FOR_MERS((max_mem))/KB_PER_SEQUENCE)

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
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
	printf(" -b             Return output as binary (default is ASCII).\n");
	printf(" -f <character> The character that will be printed at the end of the file.\n");
	printf("                output file to indicate it has ended (default is nothing)\n");
	printf(" -d <number>    Set the verbose level for debugging.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void get_options(int argc, char ** argv, const char * progname)
{
	char c;

	while ((c = getopt(argc, argv, "d:r:s:bk:w:f:o:vh")) != (char) -1)
	{
		switch (c) {
		case 'r':
			repeat_filename = optarg;
			break;
		case 's':
			if (*optarg == 'd')
			{
				rectangle_size = -1;
				max_mem_kb = strtol(optarg+1, 0, 10);
				if (max_mem_kb <= 0) max_mem_kb = ULONG_MAX;
			}
			else rectangle_size = atoi(optarg);
			if (rectangle_size == 0)
			{
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
		case 'b':
			output_format = CANDIDATE_FORMAT_BINARY;
			break;
		case 'f':
			end_char = optarg[0];
			if (isalnum(end_char) || (end_char == '>') || (end_char < ' '))
			{
				fprintf(stderr, "End character (-f %c (%d)) must not be alphanumeric, cannot be '>',\ncannot be whitespace, and cannot be printable. Please choose a punctuation\ncharacter besides '>'.\n", end_char, (int) end_char);
				exit(1);  
			}
			break;
		case 'd':
			verbose_level = atoi(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
		case 'h':
			show_help(progname);
			exit(0);
		}
	}

	if (argc - optind == 1)
	{
		sequence_filename = argv[optind++];
	}
	else if (argc - optind == 2)
	{
		sequence_filename = argv[optind++];
		second_sequence_filename = argv[optind++];
	}
	else
	{
		show_help(progname);
		fprintf(stderr, "Incorrect number of arguments. Expected 1 or 2, got %d\n", argc - optind);
		exit(1);
	}

}

int main(int argc, char ** argv)
{
	FILE * input;
	FILE * repeats = 0;
	FILE * output;

	int start_x, end_x, start_y, end_y;

	get_options(argc, argv, "sand_filter_mer_seq");

	start_time = TIME;
	unsigned long start_mem, cand_mem, table_mem;

	input = fopen(sequence_filename, "r");
	if (!input)
	{
		fprintf(stderr, "ERROR: Could not open file %s for reading.\n", sequence_filename);
		exit(1);
	}

	if (repeat_filename)
	{
		repeats = fopen(repeat_filename, "r");
		if (!repeats)
		{
			fprintf(stderr, "ERROR: Could not open file %s for reading.\n", repeat_filename);
			exit(1);
		}
	}

	if (output_filename)
		output = fopen(output_filename, "w");
	else
		output = stdout;


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
	if (!second_sequence_filename)
	{
		num_seqs = load_seqs(input, 0);
		start_x = 0;
		end_x = num_seqs;
		start_y = 0;
		end_y = num_seqs;
	}
	// If we had two files, do not compare ones from
	// the same file to each other.
	else
	{
		FILE * input2 = fopen(second_sequence_filename, "r");
		if (!input2)
		{
			fprintf(stderr, "Could not open file %s for reading.\n", second_sequence_filename);
			exit(1);
		}
		num_seqs = load_seqs_two_files(input, 0, &end_x, input2, 0, &end_y);
		start_x = 0;
		start_y = end_x;
		if (verbose_level > -1) fprintf(stderr, "%6lds : First file contains %d sequences, stored from (%d,%d].\n", TIME, end_x, start_x, end_x);
		if (verbose_level > -1) fprintf(stderr, "%6lds : Second file contains %d sequences, stored from (%d,%d].\n", TIME, end_y, start_y, end_y);
	}
	fclose(input);
	if (verbose_level > -1) fprintf(stderr, "%6lds : Loaded %d sequences\n", TIME, num_seqs);
	//printf("Hit enter to continue.\n"); scanf("%*c");

	MER_TABLE_BUCKETS = num_seqs*5;
	CAND_TABLE_BUCKETS = num_seqs*5;
	init_cand_table();
	init_mer_table();
	//printf("Hit enter to continue.\n"); scanf("%*c");
	if (repeats)
	{
		int repeat_count = init_repeat_mer_table(repeats, 2000000, 0);
		fclose(repeats);
		if (verbose_level > -1) fprintf(stderr, "%6lds : Loaded %d repeated mers\n", TIME, repeat_count);
	}

	if (rectangle_size == -1)
	{
		// Do get_mem_avail*0.95 to leave some memory for overhead
		rectangle_size = DYNAMIC_RECTANGLE_SIZE(max_mem_kb);
		if (verbose_level > -1) fprintf(stderr, "%6lds : Mem avail: %lu, rectangle size: %d\n", TIME, (unsigned long) MEMORY_FOR_MERS(max_mem_kb), rectangle_size);
	}

	// Testing out mer generation.
	//test_mers();
	//exit(0);

	//curr_rect_x = 0;
	//curr_rect_y = 1;
	int curr_start_x = start_x;
	int curr_start_y = start_y;

	candidate_t * output_list = 0;
	int num_in_list;

	//while (curr_start_y < num_seqs)
	//{
	//	while (curr_start_x < num_seqs)
	//	{
	// MAIN LOOP
	while (curr_start_y < end_y)
	{
		while (curr_start_x < end_x)
		{
			if ((start_x == start_y) && (verbose_level > -1)) fprintf(stderr, "%6lds : Loading mer table (%d,%d)\n", TIME, curr_rect_x, curr_rect_y);
			else if (verbose_level > -1) fprintf(stderr, "%6lds : Loading mer table for [%d,%d) and [%d,%d)\n", TIME, curr_start_x, MIN(curr_start_x+rectangle_size, end_x), curr_start_y, MIN(curr_start_y+rectangle_size, end_y));
			//load_mer_table(0);
			start_mem = get_mem_usage();
			load_mer_table_subset(verbose_level, curr_start_x, MIN(curr_start_x+rectangle_size, end_x), curr_start_y, MIN(curr_start_y+rectangle_size, end_y), (curr_start_x == curr_start_y));
			table_mem = get_mem_usage();
			if (verbose_level > -1) fprintf(stderr, "%6lds : Finished loading, now generating candidates\n", TIME);
			if (verbose_level > -1) fprintf(stderr, "%6lds : Memory used: %lu\n", TIME, table_mem - start_mem);
			generate_candidates(verbose_level);
			cand_mem = get_mem_usage();
			if (verbose_level > -1) fprintf(stderr, "%6lds : Total candidates generated: %llu\n", TIME, (long long unsigned int) total_cand);
			output_list = retrieve_candidates(&num_in_list);
			output_candidate_list(output, output_list, num_in_list, output_format);
			//if (verbose_level > -1) fprintf(stderr, "%6lds : Memory used: %lu\n", TIME, cand_mem - table_mem);
			free(output_list);
			//output_candidates(output, output_format);
			fflush(output);
			if (verbose_level > -1) fprintf(stderr, "%6lds : Now freeing\n", TIME);
			free_cand_table();
			free_mer_table();
			if (verbose_level > -1) fprintf(stderr, "%6lds : Successfully output and freed!\n", TIME);
			curr_rect_x++;
			curr_start_x += rectangle_size;
		}
		curr_rect_y++;
		curr_start_y += rectangle_size;
		curr_rect_x = curr_rect_y;
		if (start_y == 0) { curr_start_x = curr_start_y; }
		else { curr_start_x = start_x; }
	}

	if (end_char > 0)
	{
		fprintf(output, "%c\n", end_char);
	}

	fclose(output);

	return 0;
}

