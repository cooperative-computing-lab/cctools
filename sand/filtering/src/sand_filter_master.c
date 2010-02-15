#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <ctype.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/wait.h>

#include "debug.h"
#include "work_queue.h"
#include "text_array.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "macros.h"
#include "sequence_compression.h"
#include "sequence_filter.h"
#include "int_hash.h"

#include <sys/resource.h>

enum filter_master_task_result {
	FILTER_MASTER_TASK_RESULT_SUCCESS = 0,
	FILTER_MASTER_TASK_RESULT_CHIRP_FAILED,
	FILTER_MASTER_TASK_RESULT_CHIRP_NOT_FOUND
};

#define CHECKPOINT_STATUS_NOT_YET_TRIED 0
#define CHECKPOINT_STATUS_SUCCESS 1
#define CHECKPOINT_STATUS_FAILED 2

// FUNCTIONS
static void get_options(int argc, char ** argv, const char * progname);
static void show_version(const char *cmd);
static void show_help(const char *cmd);
static void load_sequences(const char * file);
static void load_rectangles();
static void load_rectangles_to_files();
static void delete_rectangles();
static int create_and_submit_task_cached(struct work_queue * q, int curr_rect_x, int curr_rect_y);
static int handle_done_task(struct work_queue_task * t);
static int confirm_output(struct work_queue_task *t);
static void display_progress();
//static int rect_to_index(int x, int y);
//static void index_to_rect(int index, int * x, int * y);
static int convert_cand_binary_to_ascii(FILE * outfile, const char * fname);

// GLOBALS
static int port = 9090;
static int FILE_MAX_SIZE;
static int kmer_size = 22;
static int window_size = 22;
static int rectangle_size = 0;
static char end_char = '\0';
static int do_not_unlink = 0;

static unsigned long int cand_count = 0;

static cseq * sequences = 0;
static int num_seqs = 0;
static char ** rectangles = 0;
static int num_rectangles = 0;
static size_t * sizes = 0;
static size_t * rectangle_sizes = 0;
static char ** name_map = 0;
struct task_id
{
	int x;
	int y;
};
static int_hash * task_id_map = 0;

static struct work_queue * q = 0;

static const char * sequence_filename;
static const char * repeat_filename = 0;
static const char * filter_program_name = "filter_mer_seq";
static const char * wrapper_program_name = 0;
static const char * checkpoint_filename = 0;
static char filter_program_args[255];
static const char * outfilename;
static char * outdirname = 0;
static FILE * outfile;
static FILE * checkpoint_file = 0;

static short ** checkpoint = 0;

static time_t start_time = 0;
static time_t last_display_time = 0;
static time_t last_flush_time = 0;

static int total_submitted = 0;
static int total_processed = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

static int BINARY_OUTPUT = 0;


static void show_version(const char *cmd)
{
	//printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
	printf("%s version 0.1\n", cmd);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <sequences file> <outputdata>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on.\n");
	printf(" -s <size>      Size of \"rectangle\" for filtering.\n");
	printf(" -r <file>      A meryl file of repeat mers to be filtered out.\n");
	printf(" -k <number>    The k-mer size to use in candidate selection (default is 22).\n");
	printf(" -w <number>    The minimizer window size to use in candidate selection (default");
	printf("                is 22).\n");
	printf(" -b             Return output as binary (default is ASCII). Output\n");
	printf("                will be converted to ASCII and stored in <outputdata>\n");
	printf(" -u             If set, do not unlink temporary binary output files.\n");
	printf(" -c <file>      The file which contains checkpoint information. If it exists,\n");
	printf("                it will be used, otherwise it will be created.\n");
	printf("                will be converted to when the master finishes.\n");
	printf(" -a <file>      The wrapper to be passed to filter_mer_seq. Can technically\n");
	printf("                be anything, but generally should be run_exe.pl, which\n");
	printf("                replaces the repeat mer file with a chirp file.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -f <character> The character that will be printed at the end of the file.\n");
	printf("                output file to indicate it has ended (default is nothing)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

void load_sequences(const char * file)
{

	FILE * input = fopen(file, "r");
	int seq_count = 0;
	char * new_name;

	seq_count = sequence_count(input);
	sequences = malloc(seq_count*sizeof(cseq));
	sizes = malloc(seq_count*sizeof(size_t));
	name_map = malloc(seq_count*sizeof(char *));
	cseq c;

	while (!feof(input))
	{
		c = get_next_cseq(input);
		if (!c.metadata) continue;

		// Keep track of the name of this sequence.
		name_map[num_seqs] = c.ext_id;

		// Give this a new name.
		new_name = malloc(32*sizeof(char));
		sprintf(new_name, "%d", num_seqs);
		c.ext_id = new_name;

		sequences[num_seqs] = c;
		sizes[num_seqs] = cseq_size(c);

		num_seqs++;
	}

}

void load_rectangles_to_files()
{

	int curr_rect;
	num_rectangles = ceil((float)num_seqs / (float)rectangle_size);

	rectangle_sizes = malloc(num_rectangles*sizeof(size_t));
	
	int start, end, curr;
	size_t size;
	char tmpfilename[255];
	FILE * tmpfile;

	for (curr_rect = 0; curr_rect < num_rectangles; curr_rect++)
	{
		start = curr_rect * rectangle_size;
		end = MIN(start+rectangle_size, num_seqs);
		size = 0;

		// Get the size of this rectangle.
		for (curr = start; curr < end; curr++)
		{
			size += sizes[curr];
		}

		// Open a new file to which to print this rectangle.
		sprintf(tmpfilename, "%s/rect%03d.cfa", outdirname, curr_rect);
		tmpfile = fopen(tmpfilename, "w");

		// Copy the sequences into this rectangle.
		for (curr = start; curr < end; curr++)
		{			
			// Copy this sequence into the new file.
			print_cseq(tmpfile, sequences[curr]);

			// Free this sequence, it is no longer needed.
			free_cseq(sequences[curr]);
		}

		fclose(tmpfile);
		rectangle_sizes[curr_rect] = size;
	}

	// We no longer need the sequences array, and it has
	// all been freed anyway.
	free(sequences);

}

void delete_rectangles()
{
	char tmpfilename[255];
	int curr_rect;

	if (do_not_unlink) return;

	for (curr_rect = 0; curr_rect < num_rectangles; curr_rect++)
	{
		sprintf(tmpfilename, "%s/rect%03d.cfa", outdirname, curr_rect);
		unlink(tmpfilename);
	}

}

void load_rectangles()
{
	int curr_rect;
	num_rectangles = ceil((float)num_seqs / (float)rectangle_size);

	/*checkpoint = malloc(num_rectangles * sizeof(int *));
	for (i=0; i < num_rectangles; i++)
	{
		checkpoint[i] = calloc(num_rectangles,sizeof(int));
	}*/

	rectangles = malloc(num_rectangles*sizeof(char *));
	rectangle_sizes = malloc(num_rectangles*sizeof(size_t));
	int start, end, curr;
	size_t size;
	char * ins;

	for (curr_rect = 0; curr_rect < num_rectangles; curr_rect++)
	{
		start = curr_rect * rectangle_size;
		end = MIN(start+rectangle_size, num_seqs);
		size = 0;

		// Get the size of this rectangle.
		for (curr = start; curr < end; curr++)
		{
			size += sizes[curr];
		}

		// Allocate enough space for this rectangle.
		rectangles[curr_rect] = malloc(size+1);

		ins = rectangles[curr_rect];
		// Copy the sequences into this rectangle.
		for (curr = start; curr < end; curr++)
		{			
			// Copy this sequence into the rectangle.
			ins += sprint_cseq(ins, sequences[curr]);

			// Free this sequence, it is no longer needed.
			free_cseq(sequences[curr]);
		}

		rectangle_sizes[curr_rect] = size;
	}

	// We no longer need the sequences array, and it has
	// all been freed anyway.
	free(sequences);

}

static void init_checkpoint()
{
	int row;
	int x, y, status;

	// Initialize the checkpoint matrix.
	// Using calloc because it actually initializes to 0
	checkpoint = calloc(num_rectangles, sizeof(short *));
	for (row = 0; row < num_rectangles; row++)
	{
		checkpoint[row] = calloc(num_rectangles, sizeof(short));
	}

	// Open the file in a+ mode. This will allow us to read
	// and to write to the end, allowing for maximum robustness.
	if (checkpoint_filename)
	{
		checkpoint_file = fopen(checkpoint_filename, "a+");
		if (!checkpoint_file)
		{
			// If it can't be opened in append mode (because it doesn't exist)
			// open it in write mode so it will be created.
			checkpoint_file = fopen(checkpoint_filename, "w");
			if (!checkpoint_file)
			{
				printf("WARNING: Could not open checkpoint file %s for appending or writing. Checkpoint data will not be stored.\n", checkpoint_filename);
			}
			// No need to load it if it already exists, so quit here.
			return;
		}
	}

	// Now, if the file exists, read in the existing checkpoints.
	if (checkpoint_file)
	{
		while (fscanf(checkpoint_file, "%d %d %d\n", &y, &x, &status) == 3)
		{
			checkpoint[y][x] = status;
		}

	}
}

static void checkpoint_task(struct work_queue_task * t)
{
	if (!t) return; 
	if (!checkpoint_file) return;

	int x, y, new_status;

	// Get the rectangles this task belongs to by looking at the tag.
	sscanf(t->tag, "%d-%d", &y, &x);

	// Set the status.
	new_status = (t->result == 0) ? CHECKPOINT_STATUS_SUCCESS : CHECKPOINT_STATUS_FAILED;
	checkpoint[y][x] = new_status;

	// Print this new status out to the file.
	fprintf(checkpoint_file, "%d %d %d\n", y, x, new_status);
	fflush(checkpoint_file);
}

static int create_and_submit_task_cached(struct work_queue * q, int curr_rect_x, int curr_rect_y)
{
	struct work_queue_task * t;
	
	char rname_x[32];
	char rname_y[32];
	char cmd[255];
	char output_fname[255];
	char output_rname[255];
	char fname_x[255];
	char fname_y[255];
	char wrapper[255] = "";
	char tag[32];

	if (wrapper_program_name)
	{
		sprintf(wrapper, "./%s ", wrapper_program_name);
	}

	sprintf(tag, "%03d-%03d", curr_rect_y, curr_rect_x);

	// Create new arguments for the file by passing the two
	// filenames.
	sprintf(rname_x, "rect%03d.cfa", curr_rect_x);
	if (curr_rect_x != curr_rect_y)
	{
		sprintf(rname_y, "rect%03d.cfa", curr_rect_y);
	}
	else
	{
		sprintf(rname_y, "%s","");
	}
	if (BINARY_OUTPUT)
	{
		sprintf(output_rname, "rect%03d-%03d.bcand", curr_rect_y, curr_rect_x);
		sprintf(output_fname, "%s/%s", outdirname, output_rname);
		sprintf(cmd, "%s./%s %s -b -o %s %s %s 2>&1", wrapper, filter_program_name, filter_program_args, output_rname, rname_x, rname_y);
	}
	else
	{
		sprintf(cmd, "%s./%s %s %s %s", wrapper, filter_program_name, filter_program_args, rname_x, rname_y);
	}

	// Create the task.
	t = work_queue_task_create(cmd);

	// Specify the tag for this task. Used for identifying which
	// ones are done.
	work_queue_task_specify_tag(t, tag);

	// Send the executable, if it's not already there.
	work_queue_task_specify_input_file(t, filter_program_name, filter_program_name);

	// Send the wrapper program to make sure it can execute.
	if (wrapper_program_name) work_queue_task_specify_input_file(t, wrapper_program_name, wrapper_program_name);

	// Send the repeat file if we need it and it's not already there.
	if (repeat_filename && !wrapper_program_name) work_queue_task_specify_input_file(t, repeat_filename, repeat_filename);

	// Add the rectangle. Add it as staged, so if the worker
	// already has these sequences, there's no need to send them again.
	//work_queue_task_specify_input_buf(t, rectangles[curr_rect_x], rectangle_sizes[curr_rect_x], rname_x);
	//if (curr_rect_x != curr_rect_y) work_queue_task_specify_input_buf(t, rectangles[curr_rect_y], rectangle_sizes[curr_rect_y], rname_y);
	sprintf(fname_x, "%s/%s", outdirname, rname_x);
	work_queue_task_specify_input_file(t, fname_x, rname_x);
	if (curr_rect_x != curr_rect_y)
	{
		sprintf(fname_y, "%s/%s", outdirname, rname_y);
		work_queue_task_specify_input_file(t, fname_y, rname_y);
	}

	// Get the output file if it's in binary mode. If not in binary mode
	// it will just return a string into a buffer.
	if (BINARY_OUTPUT)
		work_queue_task_specify_output_file(t, output_rname, output_fname);

	// Submit the task
	work_queue_submit(q, t);
	total_submitted++;
	debug(D_DEBUG, "Submitted task for rectangle (%d, %d)\n", curr_rect_y, curr_rect_x);

	return 1;
}

static int confirm_output(struct work_queue_task *t)
{
	return 1;
}

static int handle_done_task(struct work_queue_task * t)
{
	if (!t) return 0;

	checkpoint_task(t);

	if (t->result == 0)
	{
		if (!BINARY_OUTPUT)
		{
			if (confirm_output(t))
			{
				//debug(D_DEBUG, "Completed task!\n%s\n%s\n", t->command, t->output);
				debug(D_DEBUG, "Completed rectangle %s: '%s'\n", t->tag, t->command_line);
				fputs(t->output, outfile);
				fflush(outfile);
				total_processed++;
				tasks_runtime += (t->finish_time - t->start_time);
				tasks_filetime += t->total_transfer_time;
				//printf("Total submitted: %6d, Total processed: %6d\n", total_submitted, total_processed);
			}
			else
			{
				fprintf(stderr, "Invalid output format from host %s on rectangle %s:\n%s", t->host, t->tag, t->output);
				return 0;
			}
		}
		else
		{
			char fname[255];
			// Deal with validating binary input later.
			debug(D_DEBUG, "Completed rectangle %s (binary output): '%s' Output: %s\n", t->tag, t->command_line, t->output);

			sprintf(fname, "%s/rect%s.bcand", outdirname, t->tag);
			unsigned long int start_line_in_outfile = cand_count;
			if (convert_cand_binary_to_ascii(outfile, fname))
			{
				// If we successfully converted, delete the file.
				if (!do_not_unlink)
				{
					if (unlink(fname) != 0)
					{
						debug(D_DEBUG, "File %s was successfully converted but could not be deleted.\n", fname);
					}
				}
			}
			debug(D_DEBUG, "Lines %lu - %lu", start_line_in_outfile, cand_count);
			total_processed++;
			tasks_runtime += (t->finish_time - t->start_time);
			tasks_filetime += t->total_transfer_time;
		}
	}
	else
	{
		if (t->result == 1)
		{
			fprintf(stderr, "Rectangle %s failed while sending input to host %s\n", t->tag, t->host);
			return 0;
		}
		else if (t->result == 2)
		{
			if (WEXITSTATUS(t->return_status) == FILTER_MASTER_TASK_RESULT_CHIRP_FAILED)
			{
				fprintf(stderr, "Worker was unable to find repeat file %s in chirp on host %s for rectangle %s.\n%s\n", repeat_filename, t->host, t->tag, t->output);
			}
			else if (WEXITSTATUS(t->return_status) == FILTER_MASTER_TASK_RESULT_CHIRP_NOT_FOUND)
			{
				fprintf(stderr, "Local file for repeat file %s in chirp did not exist on host %s for rectangle %s.\n%s\n", repeat_filename, t->host, t->tag, t->output);
			}
			else
			{
				fprintf(stderr, "Function returned non-zero exit status on host %s for rectangle %s (%d):\n%s", t->host, t->tag, WEXITSTATUS(t->return_status), t->output);
			}
			return 0;
			
		}
		else if (t->result == 3)
		{
			fprintf(stderr, "Rectangle %s failed to receive output files from host %s.\n", t->tag, t->host);
		}
	}

	work_queue_task_delete(t);
	return 1;
}

static int convert_cand_binary_to_ascii(FILE * outputfile, const char * fname)
{
	FILE * infile;
	long infile_size;
	candidate_t * buffer;
	size_t result;
	int count;

	infile = fopen(fname, "rb");
	if (!infile)
	{
		fprintf(stderr, "Could not open bcand file %s\n", fname);
		return 0;
	}

	// Obtain file size.
	fseek(infile, 0, SEEK_END);
	infile_size = ftell(infile);
	count = infile_size / sizeof(candidate_t);
	rewind(infile);

	// Allocate memory to contain the whole file.
	buffer = (candidate_t *) malloc(infile_size);
	if (!buffer)
	{
		fprintf(stderr, "Could not allocate memory for bcand file %s\n", fname);
		return 0;
	}

	// Copy the result into the buffer.
	result = fread(buffer, sizeof(candidate_t), count, infile);
	if (result*sizeof(candidate_t) != infile_size)
	{
		fprintf(stderr, "Read %lu bytes from the file, expected %lu\n", result, infile_size);
		return 0;
	}

	fclose(infile);

	int i;
	for (i=0; i < count; i++)
	{
		fprintf(outputfile, "%s\t%s\t%d\t%d\t%d\n", name_map[buffer[i].cand1], name_map[buffer[i].cand2], buffer[i].dir, buffer[i].loc1, buffer[i].loc2);
		cand_count++;
	}
	//fflush(outputfile);
	fsync(fileno(outputfile));

	free(buffer);

	return 1;
}

/*
static int rect_to_index(int x, int y)
{
	return ((x*num_rectangles) - ((x*(x+1))/2) + y);
}

static int index_to_rect(int index, int * x, int * y)
{
}
*/

static void display_progress()
{
	struct work_queue_stats info;
	time_t current = time(0);

	work_queue_get_stats(q, &info);

	//if (current == start_time) current++;

	//printf("%6ds | %4d %4d %4d | %6d %4d %4d %4d | %6d %6.02lf %6.02lf %8.02lf | %.02lf\n",
	printf("%6ds | %4d %4d %4d | %6d %4d %4d %4d | %6d %6.02lf %6.02lf %10lu\n",
		(int)(current - start_time),
		info.workers_init, info.workers_ready, info.workers_busy, 
		total_submitted, info.tasks_waiting, info.tasks_running, info.tasks_complete,
		total_processed, (tasks_runtime/1000000.0)/total_processed, (tasks_filetime/1000000.0)/total_processed, cand_count);
	last_display_time = current;
	if (current - last_flush_time >= 5) 
	{
		fflush(stdout);
		last_flush_time = current;
	}
}

int main(int argc, char ** argv)
{
	const char *progname = "filter_master";

	debug_config(progname);

	get_options(argc, argv, progname);

	outfile = fopen(outfilename, "a+");
	if (!outfile)
	{
		fprintf(stderr, "Unable to open output file %s for writing\n", outfilename);
		exit(1);
	}

	q = work_queue_create(port, time(0)+300);
	if (!q) {
		fprintf(stderr, "Creation of queue on port %d timed out.\n", port);
		exit(1);
	}

	// Load sequences.
	load_sequences(sequence_filename);
	//load_rectangles(sequence_filename);
	load_rectangles_to_files();
	task_id_map = int_hash_create(12, 0);

	// Load checkpointing info
	init_checkpoint();

	start_time = time(0);

	int curr_start_x = 0, curr_start_y = 0, curr_rect_x = 0, curr_rect_y = 0;
	struct work_queue_task * t;
	
	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %10s\n",
			"Time",
			"WI","WR","WB",
			"TS","TW","TR","TC",
			"TD","AR","AF",
			"Candidates");
	// MAIN LOOP
	while (curr_start_y < num_seqs)
	{
		while (work_queue_hungry(q))
		{
			if (checkpoint[curr_rect_y][curr_rect_x] != CHECKPOINT_STATUS_SUCCESS)
				create_and_submit_task_cached(q, curr_rect_x, curr_rect_y);

			if (time(0) != last_display_time) display_progress();

			// Increment the x rectangle
			curr_rect_x++;
			curr_start_x += rectangle_size;

			// If we've reached the end of a row, move to the
			// next row by incrementing the y rectangle.
			if (curr_start_x >= num_seqs)
			{
				curr_rect_y++;
				curr_start_y += rectangle_size;
				curr_rect_x = curr_rect_y;
				curr_start_x = curr_rect_x * rectangle_size;
			}

			if (curr_start_y >= num_seqs) break;
		}
		handle_done_task(work_queue_wait(q, 1));
		if (time(0) != last_display_time) display_progress();
	}

	// Once all tasks have been submitted, just wait
	// for them all to finish.
	
	// There's a "bug" with the workqueue that workers are
	// only added when work_queue_wait is called. This means
	// that until as many workers are busy as there are tasks,
	// we need to take shorter breaks.
	while (!work_queue_empty(q))
	{
		t = work_queue_wait(q, 1);
		handle_done_task(t);
		if (time(0) != last_display_time) display_progress();
	}

	display_progress();
	printf("Candidate Selection Complete! Candidates generated: %lu\n",cand_count);
	if (checkpoint_file)
		fclose(checkpoint_file);
	if (end_char)
		fprintf(outfile, "%c\n", end_char);
	fsync(fileno(outfile));
	fclose(outfile);
	work_queue_shut_down_workers(q, 0);
	work_queue_delete(q);
	delete_rectangles();
	int rmdir_result = rmdir(outdirname);
	if (rmdir_result == ENOTEMPTY)
	{
		fprintf(stderr, "Directory %s is not empty, please check results.\n", outdirname);
	}
	else if (rmdir_result != 0)
	{
		fprintf(stderr, "Deletion of directory %s failed.\n", outdirname);
	}
	return 0;
}

static void get_options(int argc, char ** argv, const char * progname)
{
	char c;
	char tmp[512];

	while ((c = getopt(argc, argv, "p:n:d:s:r:k:w:bc:o:f:a:uvh")) != (char) -1)
	{
		switch (c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'r':
			repeat_filename = optarg;
			break;
		case 's':
			rectangle_size = atoi(optarg);
			break;
		case 'k':
			kmer_size = atoi(optarg);
			break;
		case 'w':
			window_size = atoi(optarg);
			break;
		case 'b':
			BINARY_OUTPUT = 1;
			break;
		case 'c':
			checkpoint_filename = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'u':
			do_not_unlink = 1;
			break;
		case 'f':
			end_char = optarg[0];
			if (isalnum(end_char) || (end_char == '>') || (end_char < ' '))
			{
				fprintf(stderr, "End character (-f %c (%d)) must not be alphanumeric, cannot be '>',\ncannot be whitespace, and cannot be printable. Please choose a punctuation\ncharacter besides '>'.\n", end_char, (int) end_char);
				exit(1);  
			}
			break;
		case 'a':
			wrapper_program_name = optarg;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
		case 'h':
			show_help(progname);
			exit(0);
		}
	}

	if (argc - optind != 2)
	{
		show_help(progname);
		fprintf(stderr, "Wrong number of arguments, expected 2, got %d\n", argc - optind);
		exit(1);
	}

	sequence_filename = argv[optind++];
	outfilename = argv[optind++];

	outdirname = malloc(strlen(outfilename)+8);
	sprintf(outdirname, "%s.output", outfilename);
	struct stat st;
	if (stat(outdirname, &st) != 0)
	{
		if (mkdir(outdirname, S_IRWXU) != 0)
		{
			fprintf(stderr, "Unable to create directory %s\n", outdirname);
			exit(1);
		}
	}
	else
	{
		fprintf(stderr, "WARNING: Output directory %s/ already exists, you may want to delete or rename before running.\n", outdirname);
	}

	sprintf(filter_program_args, "-k %d -w %d -s d -d -1", kmer_size, window_size);
	if (repeat_filename)
	{
		sprintf(tmp, " -r %s", repeat_filename);
		strcat(filter_program_args, tmp);
	}

}
