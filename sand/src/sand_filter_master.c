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

#include "cctools.h"
#include "debug.h"
#include "work_queue.h"
#include "work_queue_catalog.h"
#include "host_memory_info.h"
#include "macros.h"
#include "delete_dir.h"
#include "envtools.h"
#include "path.h"
#include "stringtools.h"
#include "getopt.h"
#include "getopt_aux.h"
#include "xxmalloc.h"

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
static void get_options(int argc, char **argv, const char *progname);
static void show_help(const char *cmd);
static void load_sequences(const char *file);
static size_t load_rectangle_to_file(int rect_id, struct cseq **sequences, int cseq_count);
static void task_submit(struct work_queue *q, int curr_rect_x, int curr_rect_y);
static void task_complete(struct work_queue_task *t);
static void display_progress();

static int port = WORK_QUEUE_DEFAULT_PORT;
static const char *port_file = 0;
static char *project = NULL;
static int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
static int priority = 0;

// By default, turn on fast abort option since we know each job is of very similar size (in terms of runtime).
// One can also set the fast_abort_multiplier by the '-f' option.
static int wq_option_fast_abort_multiplier = 10;

static int kmer_size = 22;
static int window_size = 22;
static int do_not_unlink = 0;
static int retry_max = 100;

static unsigned long int cand_count = 0;

static struct cseq **sequences = 0;
static int num_seqs = 0;
static int num_rectangles = 0;
static size_t *rectangle_sizes = 0;

static struct work_queue *q = 0;

static const char *progname = "sand_filter_master";
static const char *sequence_filename;
static const char *repeat_filename = 0;
static const char *checkpoint_filename = 0;
static const char *filter_program_name = "sand_filter_kernel";
static char filter_program_args[255];
static char filter_program_path[255];
static const char *outfilename;
static char *outdirname = 0;
static FILE *outfile;
static FILE *checkpoint_file = 0;

static short **checkpoint = 0;

static time_t start_time = 0;
static int total_submitted = 0;
static int total_retried = 0;
static int total_processed = 0;
static timestamp_t tasks_runtime = 0;
static timestamp_t tasks_filetime = 0;

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <sequences.cfa> <candidates.cand>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on. (default: %d)\n", port);
	printf(" -s <size>      Number of sequences in each filtering task. (default: %d)\n", rectangle_size);
	printf(" -r <file>      A meryl file of repeat mers to be filtered out.\n");
	printf(" -R <n>         Automatically retry failed jobs up to n times. (default: %d)\n", retry_max);
	printf(" -k <number>    The k-mer size to use in candidate selection (default is %d).\n", kmer_size);
	printf(" -w <number>    The minimizer window size. (default is %d).\n", window_size);
	printf(" -u             If set, do not unlink temporary binary output files.\n");
	printf(" -c <file>      Checkpoint filename; will be created if necessary.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -F <#>         Work Queue fast abort multiplier.     (default is 10.)\n");
	printf(" -a             Advertise the master information to a catalog server.\n");
	printf(" -N <project>   Set the project name to <project>\n");
	printf(" -P <integer>   Priority. Higher the value, higher the priority.\n");
	printf(" -C <catalog>   Set catalog server to <catalog>. Format: HOSTNAME:PORT\n");
	printf(" -Z <file>      Select port at random and write it out to this file.\n");
	printf(" -o <file>      Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

void load_sequences(const char *filename)
{
	FILE *file;
	int i, count, rect_id, rectangle_count;
	struct cseq *c;
	size_t size;

	rectangle_count = 256;
	rectangle_sizes = malloc(rectangle_count * sizeof(size_t));

	file = fopen(filename, "r");
	if(!file)
		fatal("couldn't open %s: %s\n", filename, strerror(errno));

	debug(D_DEBUG, "rectangle size: %d\n", rectangle_size);
	sequences = malloc(rectangle_size * sizeof(struct cseq *));
	if(!sequences)
		fatal("No enough memory to hold %d sequences. (%s) \n", rectangle_size, strerror(errno));


	count = 0;
	rect_id = 0;
	while(1) {
		c = cseq_read(file);
		if(!c) {
			if(count != rectangle_size && count > 0) {	// write the last rectangle to file
				size = load_rectangle_to_file(rect_id, sequences, count);
				if(!size)
					fatal("Failed to write rectangle %d to file. (%s)\n", rect_id, strerror(errno));
				rectangle_sizes[rect_id] = size;
				rect_id++;
				for(i = 0; i < count; i++)
					cseq_free(sequences[i]);
				debug(D_DEBUG, "Rectangle %d has been created.\n", rect_id - 1);
			}

			num_rectangles = rect_id;
			break;
		}
		sequences[count] = c;
		count++;
		num_seqs++;

		if(count == rectangle_size) {
			size = load_rectangle_to_file(rect_id, sequences, count);
			if(!size)
				fatal("Failed to write rectangle %d to file. (%s)\n", rect_id, strerror(errno));
			rectangle_sizes[rect_id] = size;
			rect_id++;
			if(rect_id == rectangle_count) {
				rectangle_count = rectangle_count * 2;
				rectangle_sizes = realloc(rectangle_sizes, rectangle_count * sizeof(size_t));
				if(!rectangle_sizes)
					fatal("Failed to allocate memory for holding rectangle sizes. Number of rectangles: %d. (%s)\n", rectangle_count, strerror(errno));
			}
			for(i = 0; i < count; i++)
				cseq_free(sequences[i]);
			count = 0;
			debug(D_DEBUG, "Rectangle %d has been created.\n", rect_id - 1);
		}
	}

	fclose(file);
	free(sequences);
}

size_t load_rectangle_to_file(int rect_id, struct cseq **sequences, int cseq_count)
{
	int i;
	size_t size;
	char tmpfilename[255];
	FILE *tmpfile;

	size = 0;
	sprintf(tmpfilename, "%s/rect%03d.cfa", outdirname, rect_id);
	tmpfile = fopen(tmpfilename, "w");
	if(!tmpfile)
		return 0;

	for(i = 0; i < cseq_count; i++) {
		cseq_write(tmpfile, sequences[i]);
		size += cseq_size(sequences[i]);
	}
	fclose(tmpfile);

	return size;
}

static void init_checkpoint()
{
	int row;
	int x, y, status;

	checkpoint = calloc(num_rectangles, sizeof(short *));
	for(row = 0; row < num_rectangles; row++) {
		checkpoint[row] = calloc(num_rectangles, sizeof(short));
	}

	if(checkpoint_filename) {
		checkpoint_file = fopen(checkpoint_filename, "a+");
		if(!checkpoint_file) {
			checkpoint_file = fopen(checkpoint_filename, "w");
			if(!checkpoint_file)
				fatal("couldn't create %s: %s", checkpoint_filename, strerror(errno));
			return;
		}
	}

	if(checkpoint_file) {
		while(fscanf(checkpoint_file, "%d %d %d\n", &y, &x, &status) == 3) {
			checkpoint[y][x] = status;
		}
	}
}

static void checkpoint_task(struct work_queue_task *t)
{
	if(!t)
		return;
	if(!checkpoint_file)
		return;

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

static void task_submit(struct work_queue *q, int curr_rect_x, int curr_rect_y)
{
	struct work_queue_task *t;

	char rname_x[32];
	char rname_y[32];
	char cmd[1024];
	char fname_x[255];
	char fname_y[255];
	char tag[32];

	sprintf(tag, "%03d-%03d", curr_rect_y, curr_rect_x);

	sprintf(rname_x, "rect%03d.cfa", curr_rect_x);

	if(curr_rect_x != curr_rect_y) {
		sprintf(rname_y, "rect%03d.cfa", curr_rect_y);
	} else {
		sprintf(rname_y, "%s", "");
	}

	sprintf(cmd, "./%s %s %s %s", filter_program_name, filter_program_args, rname_x, rname_y);

	// Create the task.
	t = work_queue_task_create(cmd);

	// Specify the tag for this task. Used for identifying which
	// ones are done.
	work_queue_task_specify_tag(t, tag);

	// Send the executable, if it's not already there.
	work_queue_task_specify_file(t, filter_program_path, filter_program_name, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);

	// Send the repeat file if we need it and it's not already there.
	if(repeat_filename)
		work_queue_task_specify_file(t, repeat_filename, path_basename(repeat_filename), WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);

	// Add the rectangle. Add it as staged, so if the worker
	// already has these sequences, there's no need to send them again.
	sprintf(fname_x, "%s/%s", outdirname, rname_x);
	work_queue_task_specify_file(t, fname_x, rname_x, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
	if(curr_rect_x != curr_rect_y) {
		sprintf(fname_y, "%s/%s", outdirname, rname_y);
		work_queue_task_specify_file(t, fname_y, rname_y, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
	}

	work_queue_submit(q, t);
	total_submitted++;
	debug(D_DEBUG, "Submitted task for rectangle (%d, %d)\n", curr_rect_y, curr_rect_x);
}

static void task_complete(struct work_queue_task *t)
{
	checkpoint_task(t);

	if(t->result == 0) {
		debug(D_DEBUG, "task complete: %s: %s", t->tag, t->command_line);
		if(strlen(t->output) > 0) {
			char *out = strdup(t->output);
			char *cand1 = malloc(sizeof(char) * 500);
			char *cand2 = malloc(sizeof(char) * 500);
			int dir, start1, start2;
			char *line = strtok(out, "\n");
			int result = sscanf(line, "%s\t%s\t%d\t%d\t%d", cand1, cand2, &dir, &start1, &start2);
			while(result == 5) {
				cand_count++;
				line = strtok(NULL, "\n");
				if(line == NULL) {
					break;
				}
				result = sscanf(line, "%s\t%s\t%d\t%d\t%d", cand1, cand2, &dir, &start1, &start2);
			}
			free(out);
			free(cand1);
			free(cand2);
		}
		fputs(t->output, outfile);
		fflush(outfile);
		total_processed++;
		tasks_runtime += (t->time_receive_output_finish - t->time_send_input_start);
		tasks_filetime += t->total_transfer_time;
		work_queue_task_delete(t);
	} else {
		debug(D_DEBUG, "task failed: %s: %s", t->tag, t->command_line);

		if(retry_max > total_retried) {
			debug(D_DEBUG, "retrying task %d/%d", total_retried, retry_max);
			total_retried++;
			work_queue_submit(q, t);
		} else {
			fprintf(stderr, "%s: giving up after retrying %d tasks.\n", progname, retry_max);
			exit(1);
		}
	}
}

static void display_progress()
{
	static int row_limit = 25;
	static int row_count = 0;
	static time_t last_display_time = 0;
	struct work_queue_stats info;
	time_t current = time(0);

	if((current - last_display_time) < 5)
		return;

	work_queue_get_stats(q, &info);

	if(row_count == 0) {
		printf(" Total | Workers   | Tasks                      Avg | Candidates\n");
		printf("  Time | Idle Busy | Submit Idle  Run   Done   Time | Found\n");
		row_count = row_limit;
	}

	double avg_time = total_processed > 0 ? (tasks_runtime / 1000000.0) / total_processed : 0;

	printf("%6d | %4d %4d | %6d %4d %4d %6d %6.02lf | %lu\n", (int) (current - start_time), info.workers_init + info.workers_ready, info.workers_busy, total_submitted, info.tasks_waiting, info.tasks_running, total_processed, avg_time, cand_count);

	fflush(stdout);
	row_count--;

	last_display_time = current;
}

int main(int argc, char **argv)
{
	debug_config(progname);

	get_options(argc, argv, progname);

	outfile = fopen(outfilename, "a+");
	if(!outfile) {
		fprintf(stderr, "%s: couldn't open %s: %s\n", progname, outfilename, strerror(errno));
		exit(1);
	}

	if(!find_executable(filter_program_name, "PATH", filter_program_path, sizeof(filter_program_path))) {
		fprintf(stderr, "%s: couldn't find %s in your PATH.\n", progname, filter_program_path);
		exit(1);
	}

	if(work_queue_master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && !project) {
		fprintf(stderr, "sand_filter: sand filter master running in catalog mode. Please use '-N' option to specify the name of this project.\n");
		fprintf(stderr, "sand_filter: Run \"%s -h\" for help with options.\n", argv[0]);
		return 1;
	}

	q = work_queue_create(port);
	if(!q) {
		fprintf(stderr, "%s: couldn't listen on port %d: %s\n", progname, port, strerror(errno));
		exit(1);
	}

	port = work_queue_port(q);

	if(port_file) {
		opts_write_port_file(port_file,port);
	}

	// advanced work queue options
	work_queue_specify_master_mode(q, work_queue_master_mode);
	work_queue_specify_name(q, project);
	work_queue_specify_priority(q, priority);
	work_queue_activate_fast_abort(q, wq_option_fast_abort_multiplier);

	load_sequences(sequence_filename);
	debug(D_DEBUG, "Sequence loaded.\n");

	init_checkpoint();

	start_time = time(0);

	int curr_start_x = 0, curr_start_y = 0, curr_rect_x = 0, curr_rect_y = 0;

	while(1) {
		while(work_queue_hungry(q)) {
			if(curr_start_y >= num_seqs)
				break;

			display_progress();

			if(checkpoint[curr_rect_y][curr_rect_x] != CHECKPOINT_STATUS_SUCCESS)
				task_submit(q, curr_rect_x, curr_rect_y);

			// Increment the x rectangle
			curr_rect_x++;
			curr_start_x += rectangle_size;

			// If we've reached the end of a row, move to the
			// next row by incrementing the y rectangle.
			if(curr_start_x >= num_seqs) {
				curr_rect_y++;
				curr_start_y += rectangle_size;
				curr_rect_x = curr_rect_y;
				curr_start_x = curr_rect_x * rectangle_size;
			}
		}

		if(work_queue_empty(q) && curr_start_y >= num_seqs)
			break;

		struct work_queue_task *t = work_queue_wait(q, 5);
		if(t)
			task_complete(t);

		display_progress();
	}

	printf("%s: candidates generated: %lu\n", progname, cand_count);

	if(checkpoint_file) {
		fclose(checkpoint_file);
	}

	fprintf(outfile, "EOF\n");
	fclose(outfile);

	work_queue_delete(q);

	if(!do_not_unlink)
		delete_dir(outdirname);

	return 0;
}

static void get_options(int argc, char **argv, const char *progname)
{
	signed char c;
	char tmp[512];

	while((c = getopt(argc, argv, "p:P:n:d:F:N:C:s:r:R:k:w:c:o:uxvhaZ:")) > -1) {
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
		case 'F':
			wq_option_fast_abort_multiplier = atof(optarg);
			break;
		case 'a':
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'N':
			free(project);
			project = xxstrdup(optarg);
			break;
		case 'P':
			priority = atoi(optarg);
			break;
		case 'C':
			setenv("CATALOG_HOST", optarg, 1);
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'u':
			do_not_unlink = 1;
			break;
		case 'Z':
			port_file = optarg;
			port = 0;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, progname);
			exit(0);
		default:
		case 'h':
			show_help(progname);
			exit(0);
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(argc - optind != 2) {
		show_help(progname);
		exit(1);
	}

	sequence_filename = argv[optind++];
	outfilename = argv[optind++];

	outdirname = malloc(strlen(outfilename) + 15);
	sprintf(outdirname, "%s.filter.tmp", outfilename);

	if(mkdir(outdirname, S_IRWXU) != 0) {
		if(errno == EEXIST) {
			fprintf(stderr, "%s: directory %s already exists, you may want to delete or rename before running.\n", progname, outdirname);
		} else {
			fprintf(stderr, "%s: couldn't create %s: %s\n", progname, outdirname, strerror(errno));
			exit(1);
		}
	}

	sprintf(filter_program_args, "-k %d -w %d -s d", kmer_size, window_size);

	if(repeat_filename) {
		sprintf(tmp, " -r %s", path_basename(repeat_filename));
		strcat(filter_program_args, tmp);
	}
}

/* vim: set noexpandtab tabstop=4: */
