/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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
#include "memory_info.h"
#include "macros.h"
#include "delete_dir.h"
#include "envtools.h"

#include "compressed_sequence.h"
#include "sequence_filter.h"

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
static void load_rectangles_to_files();
static void task_submit(struct work_queue * q, int curr_rect_x, int curr_rect_y);
static void task_complete(struct work_queue_task * t);
static void display_progress();

// GLOBALS
static int port = 9090;
static int kmer_size = 22;
static int window_size = 22;
static int do_not_unlink = 0;
static int do_not_cache = 0;
static int retry_max = 100;

static unsigned long int cand_count = 0;

static struct cseq ** sequences = 0;
static int num_seqs = 0;
static int num_rectangles = 0;
static size_t * rectangle_sizes = 0;

static struct work_queue * q = 0;

static const char *progname = "sand_filter_master";
static const char * sequence_filename;
static const char * repeat_filename = 0;
static const char * checkpoint_filename = 0;
static const char * filter_program_name = "sand_filter_kernel";
static char filter_program_args[255];
static char filter_program_path[255];
static const char * outfilename;
static char * outdirname = 0;
static FILE * outfile;
static FILE * checkpoint_file = 0;

static short ** checkpoint = 0;

static time_t start_time = 0;
static int total_submitted = 0;
static int total_retried = 0;
static int total_processed = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <sequences file> <outputdata>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on.\n");
	printf(" -s <size>      Size of \"rectangle\" for filtering.\n");
	printf(" -x             If specified, input files would be cached on the workers.\n");
	printf(" -r <file>      A meryl file of repeat mers to be filtered out.\n");
	printf(" -R <n>         Automatically retry failed jobs up to n times.\n");
	printf(" -k <number>    The k-mer size to use in candidate selection (default is 22).\n");
	printf(" -w <number>    The minimizer window size. (default is 22).\n");
	printf(" -u             If set, do not unlink temporary binary output files.\n");
	printf(" -c <file>      The file which contains checkpoint information. If it exists,\n");
	printf("                it will be used, otherwise it will be created.\n");
	printf("                will be converted to when the master finishes.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

void load_sequences(const char * filename)
{
	FILE * file = fopen(filename, "r");
	if(!file) fatal("couldn't open %s: %s\n",filename,strerror(errno));

	int alloc_size = 128;

	sequences = malloc(alloc_size*sizeof(struct cseq *));

	struct cseq *c;

	while((c = cseq_read(file))) {
		if(num_seqs>=alloc_size) {
			alloc_size *= 2;
			sequences = realloc(sequences,alloc_size * sizeof(struct cseq*));
		}
		sequences[num_seqs++] = c;
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

		for (curr = start; curr < end; curr++)
		{
			size += cseq_size(sequences[curr]);
		}

		sprintf(tmpfilename, "%s/rect%03d.cfa", outdirname, curr_rect);
		tmpfile = fopen(tmpfilename, "w");

		for (curr = start; curr < end; curr++)
		{			
			cseq_write(tmpfile, sequences[curr]);
			cseq_free(sequences[curr]);
		}

		fclose(tmpfile);
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

	checkpoint = calloc(num_rectangles, sizeof(short *));
	for (row = 0; row < num_rectangles; row++)
	{
		checkpoint[row] = calloc(num_rectangles, sizeof(short));
	}

	if (checkpoint_filename)
	{
		checkpoint_file = fopen(checkpoint_filename, "a+");
		if (!checkpoint_file)
		{
			checkpoint_file = fopen(checkpoint_filename, "w");
			if (!checkpoint_file) fatal("couldn't create %s: %s",checkpoint_filename,strerror(errno));
			return;
		}
	}

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

static void task_submit(struct work_queue * q, int curr_rect_x, int curr_rect_y)
{
	struct work_queue_task * t;
	
	char rname_x[32];
	char rname_y[32];
	char cmd[255];
	char fname_x[255];
	char fname_y[255];
	char tag[32];

	sprintf(tag, "%03d-%03d", curr_rect_y, curr_rect_x);

	sprintf(rname_x, "rect%03d.cfa", curr_rect_x);

	if (curr_rect_x != curr_rect_y) {
		sprintf(rname_y, "rect%03d.cfa", curr_rect_y);
	} else {
		sprintf(rname_y, "%s","");
	}

	sprintf(cmd, "./%s %s %s %s", filter_program_name, filter_program_args, rname_x, rname_y);

	int cache_flag = do_not_cache ? WORK_QUEUE_NOCACHE : WORK_QUEUE_CACHE;

	// Create the task.
	t = work_queue_task_create(cmd);

	// Specify the tag for this task. Used for identifying which
	// ones are done.
	work_queue_task_specify_tag(t, tag);

	// Send the executable, if it's not already there.
	work_queue_task_specify_file(t, filter_program_path, filter_program_name, WORK_QUEUE_INPUT, cache_flag);

	// Send the repeat file if we need it and it's not already there.
	if (repeat_filename) work_queue_task_specify_file(t, repeat_filename, repeat_filename, WORK_QUEUE_INPUT, cache_flag);

	// Add the rectangle. Add it as staged, so if the worker
	// already has these sequences, there's no need to send them again.
	sprintf(fname_x, "%s/%s", outdirname, rname_x);
	work_queue_task_specify_file( t, fname_x, rname_x, WORK_QUEUE_INPUT,cache_flag );
	if (curr_rect_x != curr_rect_y)
	{
		sprintf(fname_y, "%s/%s", outdirname, rname_y);
		work_queue_task_specify_file( t, fname_y, rname_y, WORK_QUEUE_INPUT, cache_flag );
	}

	work_queue_submit(q, t);
	total_submitted++;
	debug(D_DEBUG, "Submitted task for rectangle (%d, %d)\n", curr_rect_y, curr_rect_x);
}

static void task_complete( struct work_queue_task * t )
{
	checkpoint_task(t);

	if (t->result == 0) {
		debug(D_DEBUG, "task complete: %s: %s", t->tag, t->command_line);
		if(strlen(t->output) > 0){
			char * out = strdup(t->output);
			char * cand1 = malloc(sizeof(char) * 500);
			char * cand2 = malloc(sizeof(char) * 500);
			int dir, start1, start2;
			char * line = strtok(out, "\n");  
			int result = sscanf(line, "%s\t%s\t%d\t%d\t%d", cand1, cand2, &dir, &start1, &start2);
			while(result == 5)
			{
				cand_count++;
				line = strtok(NULL, "\n");
				if(line == NULL){break;} 
	 			result = sscanf(line, "%s\t%s\t%d\t%d\t%d", cand1, cand2, &dir, &start1, &start2);
			} 
			free(out);
			free(cand1);	
			free(cand2);
		}
		fputs(t->output, outfile);
		fflush(outfile);
		total_processed++;
		tasks_runtime += (t->finish_time - t->start_time);
		tasks_filetime += t->total_transfer_time;
		work_queue_task_delete(t);
	} else {
		debug(D_DEBUG, "task failed: %s: %s",t->tag,t->command_line);

		if(retry_max>total_retried) {
			debug(D_DEBUG,"retrying task %d/%d",total_retried,retry_max);
			total_retried++;
			work_queue_submit(q, t);
		} else {
			fprintf(stderr,"%s: giving up after retrying %d tasks.\n",progname,retry_max);
			exit(1);
		}
	}
}

static void display_progress()
{
	static time_t last_display_time = 0;
	struct work_queue_stats info;
	time_t current = time(0);

	if( (current - last_display_time) < 5 ) return;

	work_queue_get_stats(q, &info);

	printf("%6ds | %4d %4d %4d | %6d %4d %4d %4d | %6d %6.02lf %6.02lf %10lu\n",
		(int)(current - start_time),
		info.workers_init, info.workers_ready, info.workers_busy, 
		total_submitted, info.tasks_waiting, info.tasks_running, info.tasks_complete,
		total_processed, (tasks_runtime/1000000.0)/total_processed, (tasks_filetime/1000000.0)/total_processed, cand_count);

	fflush(stdout);

	last_display_time = current;
}

int main(int argc, char ** argv)
{
	debug_config(progname);

	get_options(argc, argv, progname);

	outfile = fopen(outfilename, "a+");
	if (!outfile) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,outfilename,strerror(errno));
		exit(1);
	}

	if(!find_executable(filter_program_name,"PATH",filter_program_path,sizeof(filter_program_path))) {
		fprintf(stderr,"%s: couldn't find %s in your PATH.\n",progname,filter_program_path);
		exit(1);	
	}

	q = work_queue_create(port);
	if (!q) {
		fprintf(stderr, "%s: couldn't listen on port %d: %s\n",progname,port,strerror(errno));
		exit(1);
	}

	load_sequences(sequence_filename);
	load_rectangles_to_files();

	init_checkpoint();

	start_time = time(0);

	int curr_start_x = 0, curr_start_y = 0, curr_rect_x = 0, curr_rect_y = 0;
	
	printf("%7s | %4s %4s %4s | %6s %4s %4s %4s | %6s %6s %6s %10s\n",
			"Time",
			"WI","WR","WB",
			"TS","TW","TR","TC",
			"TD","AR","AF",
			"Candidates");

	while (1) {
		while (work_queue_hungry(q)) {
			if (curr_start_y >= num_seqs) break;

			display_progress();

			if (checkpoint[curr_rect_y][curr_rect_x] != CHECKPOINT_STATUS_SUCCESS)
				task_submit(q, curr_rect_x, curr_rect_y);

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
		}

		if(work_queue_empty(q) && curr_start_y >= num_seqs) break;

		struct work_queue_task *t = work_queue_wait(q,5);
		if(t) task_complete(t);

		display_progress();
	}

	printf("%s: candidates generated: %lu\n",progname,cand_count);

	if (checkpoint_file) {
		fclose(checkpoint_file);
	}

	fprintf(outfile,"EOF\n");
	fclose(outfile);

	work_queue_delete(q);

	if(!do_not_unlink) delete_dir(outdirname);

	return 0;
}

static void get_options(int argc, char ** argv, const char * progname)
{
	char c;
	char tmp[512];

	while ((c = getopt(argc, argv, "p:n:d:s:r:R:k:w:c:o:uxvh")) != (char) -1)
	{
		switch (c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'r':
			repeat_filename = optarg;
			break;
		case 'R':
			retry_max = atoi(optarg);
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
		case 'c':
			checkpoint_filename = optarg;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'u':
			do_not_unlink = 1;
			break;
		case 'x':
			do_not_cache = 1;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
		default:
		case 'h':
			show_help(progname);
			exit(0);
		}
	}

	if (argc - optind != 2)
	{
		show_help(progname);
		exit(1);
	}

	sequence_filename = argv[optind++];
	outfilename = argv[optind++];

	outdirname = malloc(strlen(outfilename)+8);
	sprintf(outdirname, "%s.output", outfilename);

	if (mkdir(outdirname, S_IRWXU) != 0) {
		if(errno==EEXIST) {
			fprintf(stderr, "%s: directory %s already exists, you may want to delete or rename before running.\n",progname,outdirname);
		} else {
			fprintf(stderr, "%s: couldn't create %s: %s\n",progname,outdirname,strerror(errno));
			exit(1);
		}
	}

	sprintf(filter_program_args, "-k %d -w %d -s d", kmer_size, window_size);

	if (repeat_filename) {
		sprintf(tmp, " -r %s", repeat_filename);
		strcat(filter_program_args, tmp);
	}
}
