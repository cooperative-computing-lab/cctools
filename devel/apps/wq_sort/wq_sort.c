/*
  Copyright (C) 2022 The University of Notre Dame
  This software is distributed under the GNU General Public License.
  See the file COPYING for details.
*/

/*
 * Distributed sort using Work Queue.
 */

#include "debug.h"
#include <work_queue.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include <limits.h>
#include <float.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#define LINE_SIZE 2048

//Defaults
#define PARTITION_DEFAULT 20
#define BW_DEFAULT 100 //BW in MBps
#define SAMPLE_SIZE_DEFAULT 2

#define PARTITION_COEFF_A_DEFAULT 195 //175
#define PARTITION_COEFF_B_DEFAULT 0.00005
#define MERGE_COEFF_A_DEFAULT 10
#define MERGE_COEFF_B_DEFAULT 435 //375
#define PER_RECORD_SORT_TIME_DEFAULT 0.000003

double partition_overhead_coefficient_a = PARTITION_COEFF_A_DEFAULT;
double partition_overhead_coefficient_b = PARTITION_COEFF_B_DEFAULT;
double merge_overhead_coefficient_a = MERGE_COEFF_A_DEFAULT;
double merge_overhead_coefficient_b = MERGE_COEFF_B_DEFAULT;
double per_record_sort_time = PER_RECORD_SORT_TIME_DEFAULT;
double bandwidth_bytes_per_sec = BW_DEFAULT * 1000000;

static int created_partitions = 0;
static int run_timing_code = 0;

unsigned long long get_total_lines(char *infile) {
	FILE *input_file = fopen(infile, "r");
	if (input_file == NULL) {
		fprintf (stderr, "Opening %s file failed: %s!\n", infile, strerror(errno));
		return 0;
	}

	unsigned long long line_count = 0;
	int ch;

	while ((ch=fgetc(input_file)) != EOF) {
		if (ch=='\n')
			++line_count;
	}
	fclose(input_file);

	return line_count;
}

//Returns the end byte offset for a given line number in file
off_t get_file_line_end_offset(FILE *fp, off_t start_offset, unsigned long long line_number) {
	unsigned long long line_count = 0;
	int ch;
	long end_offset = -1;

	if (fp == NULL)
		return -1;

	fseek(fp, start_offset, SEEK_SET);

	while ((ch=fgetc(fp)) != EOF && line_count < line_number) {
		if (ch == '\n')
			++line_count;
	}

	if(line_count == line_number) {
		end_offset = ftell(fp);
	}

	return (end_offset-2); //subtract two to rewind back to newline at end of line
}

int submit_task(struct work_queue *q, const char *command, const char *executable, const char *infile, off_t infile_offset_start, off_t infile_offset_end, const char *outfile) {
	struct work_queue_task *t;
	int taskid;

	char *infile_dup = strdup(infile); //basename() modifies its arguments. So we need to pass a duplicate.
	char *executable_dup = strdup(executable);
	char *outfile_dup = strdup(outfile);

	t = work_queue_task_create(command);
	if (!work_queue_task_specify_file_piece(t, infile, basename(infile_dup), infile_offset_start, infile_offset_end, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE)) {
		fprintf(stderr, "task_specify_file_piece() failed for %s: start offset %ld, end offset %ld.\n", infile, infile_offset_start, infile_offset_end);
		return 0;
	}
	if (!work_queue_task_specify_file(t, executable, basename(executable_dup), WORK_QUEUE_INPUT, WORK_QUEUE_CACHE)) {
		fprintf(stderr, "task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", executable);
		return 0;
	}
	if (!work_queue_task_specify_file(t, outfile, basename(outfile_dup), WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE)) {
		fprintf(stderr, "task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", outfile);
		return 0;
	}

	taskid = work_queue_submit(q, t);
	fprintf(stdout, "submitted task (id# %d): %s\n", taskid, t->command_line);

	free(infile_dup);
	free(executable_dup);
	free(outfile_dup);

	return taskid;
}

/* Partition the input file according to the number of partitions specified and
 * create tasks that sort each of these partitions.
 */
off_t partition_tasks(struct work_queue *q, char *executable, const char *executable_args, const char *infile, int infile_offset_start, const char *outfile_prefix, int partitions, unsigned long long records_to_partition) {
	char outfile[256], remote_infile[256], command[256];

	off_t file_offset_start = infile_offset_start;
	off_t file_offset_end;
	unsigned long long task_end_line = 0;
	unsigned long long lines_to_submit;
	FILE *infile_fs;

	struct timeval current;
	long long unsigned int partition_start_time, partition_end_time;
	double partition_time_secs = 0;

	if(run_timing_code){
		gettimeofday(&current, 0);
		partition_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
	}

	unsigned long long lines_per_task = (unsigned long long)ceil((double)records_to_partition/partitions);

	char *infile_dup = strdup(infile);
	strcpy(remote_infile, basename(infile_dup));
	free(infile_dup);

	infile_fs = fopen(infile, "r");
	if (infile_fs == NULL) {
		fprintf (stderr, "Opening %s file failed: %s!\n", infile, strerror(errno));
		return 0;
	}

	while(task_end_line < records_to_partition) {
		//we partition input into pieces by tracking the file offset of the lines in it.
		lines_to_submit = (records_to_partition - task_end_line) < lines_per_task ? (records_to_partition - task_end_line) : lines_per_task;
		task_end_line += lines_to_submit;
		file_offset_end = get_file_line_end_offset(infile_fs, file_offset_start, lines_to_submit);
		if (file_offset_end < 0) {
			fprintf (stderr, "End file offset for line %llu is:%ld\n", task_end_line, file_offset_end);
			return 0;
		}

		//create and submit tasks for sorting the pieces.
		sprintf(outfile, "%s.%d", outfile_prefix, created_partitions);
		if (executable_args){
			sprintf(command, "./%s %s %s > %s", basename(executable), executable_args, remote_infile, outfile);
		} else {
			sprintf(command, "./%s %s > %s", basename(executable), remote_infile, outfile);
		}

		if(!submit_task(q, command, executable, infile, file_offset_start, file_offset_end, outfile))
			return 0;

		created_partitions++;
		file_offset_start = file_offset_end + 1;
	}

	fclose(infile_fs);

	if(run_timing_code){
		gettimeofday(&current, 0);
		partition_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
		partition_time_secs += (partition_end_time - partition_start_time) / 1000000.0;
		fprintf(stderr, "Sample partition time is %f\n", partition_time_secs);

		//Coefficent B is expected to be really small and so we just use the default.
		fprintf(stderr, "Default partition coeff A: %f\n", partition_overhead_coefficient_a);
		partition_overhead_coefficient_a = partition_time_secs / (double) (records_to_partition/1000000000.0);
		fprintf(stderr, "Computed partition coeff A: %f\n", partition_overhead_coefficient_a);

	}

	return file_offset_start;
}

int get_file_line_value(FILE *fp) {
	char *line = (char*) malloc(sizeof(char) * LINE_SIZE);
	int line_value;

	if (!fgets(line, LINE_SIZE, fp)) {
			if(feof(fp)) {
				return -1;
			}
	}
	line_value = atoi(line);
	free(line);
	return line_value;
}

//compute min of array and also return the position of min.
int find_min(int *vals, int length, int *min_pos) {
	int i;
	int min = INT_MAX;
	for (i = 0; i < length; i++) {
		if(vals[i] >= 0 && vals[i] <= min) {
			min = vals[i];
			*min_pos = i;
		}
	}
	return min;
}

// Do k-way merge of the sorted outputs returned by tasks.
int merge_sorted_outputs(const char *outfile, const char *partition_file_prefix, int partitions) {
	FILE *outfile_fp;

	char partition_file[256];
	FILE **partition_file_fps;
	int *partition_file_line_vals;

	int min_pos, min_value;
	unsigned long long merged_records = 0;
	int merged_partitions = 0;
	int i;

	struct timeval current;
	long long unsigned int read_lines_start_time, read_lines_end_time;
	double read_lines_time_secs = 0;

	outfile_fp = fopen(outfile, "w");
	if(!outfile_fp) {
		fprintf(stderr, "Opening file %s failed: %s!\n", outfile, strerror(errno));
		return -1;
	}

	partition_file_line_vals = malloc(sizeof(int) * partitions);
	partition_file_fps = malloc(sizeof(FILE *) * partitions);

	if(run_timing_code){
		gettimeofday(&current, 0);
		read_lines_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
	}

	for(i = 0; i < partitions; i++) {
		sprintf(partition_file, "%s.%d", partition_file_prefix, i);
		partition_file_fps[i] = fopen(partition_file, "r");
		if(!partition_file_fps[i]) {
			fprintf(stderr, "Opening file %s failed: %s!\n", partition_file, strerror(errno));
			goto cleanup;
			return -1;
		}
	}

	//read the first lines of each output file into the array
	for(i = 0; i < partitions; i++) {
		partition_file_line_vals[i] = get_file_line_value(partition_file_fps[i]);
	}

	if(run_timing_code){
		gettimeofday(&current, 0);
		read_lines_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
		read_lines_time_secs += (read_lines_end_time - read_lines_start_time)/1000000.0;
	}

	//compute the minimum of array and load a new value from the contributing
	//file into the array index of the minimum.
	while (merged_partitions < partitions) {
		min_value = find_min(partition_file_line_vals, partitions, &min_pos);
		if(run_timing_code){
			gettimeofday(&current, 0);
			read_lines_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
		}

		fprintf(outfile_fp, "%d\n", min_value); //write current min value to output file

		partition_file_line_vals[min_pos] = get_file_line_value(partition_file_fps[min_pos]);

		if(run_timing_code){
			gettimeofday(&current, 0);
			read_lines_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
			read_lines_time_secs += (read_lines_end_time - read_lines_start_time)/1000000.0;
		}

		merged_records++;
		if (partition_file_line_vals[min_pos] < 0) {
			merged_partitions++;
		}
	}

  cleanup:
	for(i = 0; i < partitions; i++) {
		fclose(partition_file_fps[i]);
		sprintf(partition_file, "%s.%d", partition_file_prefix, i);
		unlink(partition_file);
	}
	free(partition_file_line_vals);
	free(partition_file_fps);
	fclose(outfile_fp);

	if(run_timing_code){
		//Coefficent A is expected to be really small and so we just use the default.
		fprintf(stderr, "Merged records: %lld, file read time:%f\n", merged_records, read_lines_time_secs);
		fprintf(stderr, "Default merge coeff B: %f\n", merge_overhead_coefficient_b);
		merge_overhead_coefficient_b = read_lines_time_secs / (double) (merged_records / 1000000000.0);
		fprintf(stderr, "Computed merge coeff B: %f\n", merge_overhead_coefficient_b);
	}

	return 1;
}

double wait_partition_tasks(struct work_queue *q, int timeout, char *task_times_file) {
	struct work_queue_task *t;
	FILE *task_times_fp = NULL;

	double task_execution_times = 0;
	int64_t total_transfered_bytes = 0;
	time_t total_transfer_time = 0;

	if(task_times_file) {
		task_times_fp = fopen("wq_sort.tasktimes", "w");
		if (!task_times_fp) {
			fprintf(stderr, "Opening of wq_sort.tasktimes file failed!\n");
		}
	}

	while(!work_queue_empty(q)) {
		t = work_queue_wait(q, timeout);
		if(t) {
			fprintf(stdout, "Task (taskid# %d) complete in %llu: %s (return code %d)\n", t->taskid, (long long unsigned) t->cmd_execution_time, t->command_line, t->return_status);

			total_transfered_bytes += t->total_bytes_transferred;
			total_transfer_time += t->total_transfer_time;
			fprintf(stderr, "Total bytes sent %" PRId64 " in %llu s\n", total_transfered_bytes, (long long unsigned) total_transfer_time);
			fprintf(stderr, "Default bandwidth (Bps): %f\n", bandwidth_bytes_per_sec);
			bandwidth_bytes_per_sec  = total_transfered_bytes / (total_transfer_time/1000000.0);
			fprintf(stderr, "Measured bandwidth (Bps): %f\n", bandwidth_bytes_per_sec);

			task_execution_times += t->cmd_execution_time/1000000.00;

			if(task_times_fp) {
				fprintf(task_times_fp, "%d: %llu\n", t->taskid, (long long unsigned) t->cmd_execution_time);
			}

			work_queue_task_delete(t);
		}
	}

	if(task_times_fp)
		fclose(task_times_fp);

	return task_execution_times;
}

// Sample the execution environment.
off_t sample_run(struct work_queue *q, char *executable, const char *executable_args, const char *infile, int infile_offset_start, const char *partition_file_prefix, const char *outfile, int partitions, unsigned long long records_to_sort) {

	double sample_task_runtimes = 0;

	fprintf(stdout, "Sampling the execution environment with %d partitions!\n", partitions);

	//turn on timing code to compute the model coefficients if the sample size is large enough (100million).
	if(records_to_sort >= 100000000)
		run_timing_code = 1;

	off_t partition_offset_end = partition_tasks(q, executable, executable_args, infile, infile_offset_start, partition_file_prefix, partitions, records_to_sort);

	sample_task_runtimes = wait_partition_tasks(q, 5, NULL);
	fprintf(stderr, "Sample task times: %f\n", sample_task_runtimes);
	fprintf(stderr, "Default per record sort time: %f\n", per_record_sort_time);
	per_record_sort_time = sample_task_runtimes/(double)records_to_sort;
	fprintf(stderr, "Computed per record sort time: %f\n", per_record_sort_time);

	merge_sorted_outputs(outfile, partition_file_prefix, partitions);

	run_timing_code = 0; //turn off timing code.

	created_partitions = 1;	 //we merge the sample partitions to 1.
	return partition_offset_end;
}


double* sort_estimate_runtime(char *input_file, char *executable, unsigned long long records, int resources, int tasks) {
	//Model: T(n,k,r) = [T_part + T_merge] + [(t*n)/k * ceil(k/r)] + [(d_n + (d_r * r))/BW_Bps]

	double partition_overhead;
	double merge_overhead;
	double parallel_execution_time;
	double transfer_overhead;
	double total_execution_time;
	double *estimated_times;

	double records_in_billion;
	long long record_bytes = 0;
	long long sw_bytes = 0;

	struct stat stat_buf;

	//resources that exceed the number of tasks aren't going to be used.
	if(resources > tasks) {
		resources = tasks;
	}

	if(!stat(input_file, &stat_buf)){
		record_bytes = stat_buf.st_size;
	}

	if(!stat(executable, &stat_buf)){
		sw_bytes = stat_buf.st_size;
	}

	records_in_billion = records/1000000000.0;

	//we transfer the records twice - for input and output.
	transfer_overhead = ((double)((2*record_bytes) + (sw_bytes * resources))) / bandwidth_bytes_per_sec;

	parallel_execution_time = (records * per_record_sort_time) / tasks;
	parallel_execution_time *= ceil((double)tasks/(double)resources);

	/* Model of partition is based on the partitioning done in partition_tasks():
	 * Its asymptotic runtime is O(n+m) where n is number of records and m is number of partitions.
	 * Its actual runtime is modeled as: (a*n + b*m). The values of a and b are found by sampling.
	 */
	partition_overhead = (partition_overhead_coefficient_a * records_in_billion) + (partition_overhead_coefficient_b * tasks);

	/* Model of merge is based on the running time of merge_sorted_outputs():
	 * Its asymptotic runtime is O(n*m) where n is number of records and m is number of partitions.
	 * Its actual runtime is modeled as: (a*n*m + b*n). The values of a and b are found sampling.
	 */
	merge_overhead = (merge_overhead_coefficient_a * records_in_billion * tasks) + (merge_overhead_coefficient_b * records_in_billion);

	total_execution_time = partition_overhead + merge_overhead + parallel_execution_time + transfer_overhead;

	estimated_times = (double *)malloc(sizeof(double) * 5);
	if (estimated_times == NULL) {
		fprintf (stderr, "Allocating memory for estimated_times failed!\n");
		return NULL;
	}
	estimated_times[0] = total_execution_time;
	estimated_times[1] = partition_overhead;
	estimated_times[2] = merge_overhead;
	estimated_times[3] = parallel_execution_time;
	estimated_times[4] = transfer_overhead;
	return estimated_times;
}

int get_optimal_runtimes(char *input_file, char *executable, int resources, unsigned long long records, double *optimal_times) {
	double *estimated_times;
	double optimal_execution_time = -1;
	double execution_time = -1;
	int optimal_partitions;
	int i;
	for (i = 1; i <= 5*resources; i++) {
		estimated_times = sort_estimate_runtime(input_file, executable, records, resources, i);
		execution_time = estimated_times[0];
		if (optimal_execution_time < 0 || execution_time < optimal_execution_time) {
			optimal_execution_time = optimal_times[0] = execution_time;
			optimal_partitions = i;
			optimal_times[1] = estimated_times[1];
			optimal_times[2] = estimated_times[2];
			optimal_times[3] = estimated_times[3];
			optimal_times[4] = estimated_times[4];
		}
		free(estimated_times);
	}
	return optimal_partitions;
}

static void show_help(const char *cmd) {
	fprintf(stdout, "Use: %s [options] <sort program> <infile>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Specify a project name for the Work Queue manager. (default = none)\n", "-N <string>");
	fprintf(stdout, " %-30s Specify the number of partitions to create of the input data. (default = 20)\n", "-k <int>");
	fprintf(stdout, " %-30s Specify the output file name for the sorted records. (default = <infile>.sorted)\n", "-o <string>");
	fprintf(stdout, " %-30s Automatically determine the optimal partition size. (default = 20)\n", "-A");
	fprintf(stdout, " %-30s Empirically estimate the model coefficients by sampling the execution environment. (default = off)\n", "-S");
	fprintf(stdout, " %-30s Specify the number of sample partitions. (default = %d)\n", "-s <int>", SAMPLE_SIZE_DEFAULT);
	fprintf(stdout, " %-30s Specify the arguments for the sort program.\n", "-p <string>");
	fprintf(stdout, " %-30s Estimate and print the optimal number of partitions for different resource sizes and exit.\n", "-M");
	fprintf(stdout, " %-30s Specify the number of records in the input file.(default=auto).\n", "-L <int>");
	fprintf(stdout, " %-30s Specify the keepalive interval for WQ.(default=300).\n", "-I <int>");
	fprintf(stdout, " %-30s Specify the keepalive timeout for WQ.(default=30).\n", "-T <int>");
	fprintf(stdout, " %-30s Estimate and print the runtime for specified partition and exit.\n", "-R <int>");
	fprintf(stdout, " %-30s Set the estimated bandwidth (in MBps) to workers for estimating optimal paritions. (default=%d)\n", "-B <int>", BW_DEFAULT);
	fprintf(stdout, " %-30s Show this help screen\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	struct work_queue *q;
	int port = 0; //pick an arbitrary port
	int c;

	char *sort_arguments = NULL;
	const char *proj_name = NULL;
	char *outfile= NULL;
	int auto_partition = 0;
	int sample_env = 0;
	int print_runtime_estimates = 0;
	int estimate_partition= 0;
	struct timeval current;
	long long unsigned int execn_start_time, execn_time, workload_runtime;
	int keepalive_interval = 300;
	int keepalive_timeout = 30;

	unsigned long long records = 0;
	int partitions = PARTITION_DEFAULT;
	int sample_size = SAMPLE_SIZE_DEFAULT;

	gettimeofday(&current, 0);
	execn_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;

	debug_flags_set("all");
	if(argc < 3) {
		show_help(argv[0]);
		return 0;
	}

	while((c = getopt(argc, argv, "N:k:o:ASs:p:MR:L:I:T:B:h")) != (char) -1) {
		switch (c) {
		case 'N':
			proj_name = strdup(optarg);
			break;
		case 'k':
			partitions = atoi(optarg);
			break;
		case 'o':
			outfile = strdup(optarg);
			break;
		case 'A':
			auto_partition = 1;
			break;
		case 's':
			sample_size = atoi(optarg);
			break;
		case 'S':
			sample_env = 1;
			break;
		case 'p':
			sort_arguments = strdup(optarg);
			break;
		case 'M':
			print_runtime_estimates = 1;
			break;
		case 'R':
			estimate_partition = atoi(optarg);
			break;
		case 'L':
			records = atoll(optarg);
			break;
		case 'I':
			keepalive_interval = atoi(optarg);
			break;
		case 'T':
			keepalive_timeout = atoi(optarg);
			break;
		case 'B':
			bandwidth_bytes_per_sec = atoi(optarg) * 1000000;
			break;
		case 'h':
			show_help(argv[0]);
			return 0;
		default:
			show_help(argv[0]);
			return -1;
		}
	}

	char sort_executable[256], infile[256];
	off_t last_partition_offset_end = 0;
	int optimal_partitions, optimal_resources, current_optimal_partitions;
	double current_optimal_time = DBL_MAX;
	double optimal_times[5];
	int sample_partition_offset_end = 0;
	int i;

	sprintf(sort_executable, "%s", argv[optind]);
	sprintf(infile, "%s", argv[optind+1]);

	if(!outfile){
		char *infile_dup = strdup(infile);
		outfile = (char *) malloc((strlen(infile)+8)*sizeof(char));
		sprintf(outfile, "%s.sorted", basename(infile_dup));
		free(infile_dup);
	}

	if(records == 0) {
		records = get_total_lines(infile);
		fprintf(stdout, "Input file %s has %llu records to sort\n", infile, records);
		if(records == 0) {
			fprintf(stderr, "Error in reading records. Quitting...\n");
			return 0;
		}
	}

	if(estimate_partition) {
		double *estimated_runtimes = (double *)malloc(sizeof(double) * 5);
		for (i = 1; i <= 2*estimate_partition; i++) {
			estimated_runtimes = sort_estimate_runtime(infile, sort_executable, records, i, estimate_partition);
			if(estimated_runtimes[0] < current_optimal_time) {
				current_optimal_time = estimated_runtimes[0];
				optimal_times[0] = estimated_runtimes[0];
				optimal_times[1] = estimated_runtimes[1];
				optimal_times[2] = estimated_runtimes[2];
				optimal_times[3] = estimated_runtimes[3];
				optimal_times[4] = estimated_runtimes[4];
				optimal_resources = i;
			}
		}
		fprintf(stdout, "For partition %d: %d %f %f %f %f %f\n", estimate_partition, optimal_resources, optimal_times[0], optimal_times[1], optimal_times[2], optimal_times[3], optimal_times[4]);
		free(estimated_runtimes);
		return 1;
	}

	if(print_runtime_estimates) {
		fprintf(stdout, "Resources \t Partitions \t Runtime \t Part time \t Merge time \t Task time \t Transfer time\n");
		for (i = 1; i <= 100; i++) {
			optimal_partitions = get_optimal_runtimes(infile, sort_executable, i, records, optimal_times);
			fprintf(stdout, "%d \t \t %d \t %f \t %f \t %f \t %f \t %f\n", i, optimal_partitions, optimal_times[0], optimal_times[1], optimal_times[2], optimal_times[3], optimal_times[4]);
		}
		return 1;
	}

	q = work_queue_create(port);
	if(!q) {
		fprintf(stderr, "couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}

	fprintf(stdout, "listening on port %d...\n", work_queue_port(q));

	if(proj_name){
		work_queue_specify_manager_mode(q, WORK_QUEUE_MANAGER_MODE_CATALOG);
		work_queue_specify_name(q, proj_name);
	}
	work_queue_specify_keepalive_interval(q, keepalive_interval);
	work_queue_specify_keepalive_timeout(q, keepalive_timeout);

	free((void *)proj_name);

	fprintf(stdout, "%s will be run to sort contents of %s\n", sort_executable, infile);

	long long unsigned int sample_start_time, sample_end_time, sample_time;
	if(sample_env) {
		gettimeofday(&current, 0);
		sample_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
		int sample_record_size = (5*records)/100; //sample size is 5% of the total records

		char *sample_partition_file_prefix = (char *) malloc((strlen(outfile)+8) * sizeof(char));
		sprintf(sample_partition_file_prefix, "%s.sample", outfile);

		char *sample_outfile = (char *) malloc((strlen(outfile)+3) * sizeof(char));
		sprintf(sample_outfile, "%s.0", outfile);

		sample_partition_offset_end = sample_run(q, sort_executable, sort_arguments, infile, 0, sample_partition_file_prefix, sample_outfile, sample_size, sample_record_size);

		records -= sample_record_size;

		free(sample_partition_file_prefix);
		free(sample_outfile);
		gettimeofday(&current, 0);
		sample_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
		sample_time = sample_end_time - sample_start_time;
		fprintf(stdout, "Sampling time is %llu\n", sample_time);
	}

	if(auto_partition) {
		fprintf(stdout, "Determining optimal partition size for %s\n", infile);
		for (i = 1; i <= 100; i++) {
			current_optimal_partitions = get_optimal_runtimes(infile, sort_executable, i, records, optimal_times);
			if (optimal_times[0] < current_optimal_time) {
				current_optimal_time = optimal_times[0];
				optimal_partitions = current_optimal_partitions;
				optimal_resources = i;
			}
		}
		fprintf(stdout, "Optimal partition size is %d that runs the workload in %f\n", optimal_partitions, current_optimal_time);
		fprintf(stdout, "--> Please allocate %d resources for running this workload in a cost-efficient manner.\n", optimal_resources);
		partitions = optimal_partitions;
	}

	long long unsigned int part_start_time, part_end_time, part_time;
	gettimeofday(&current, 0);
	part_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;

	last_partition_offset_end = partition_tasks(q, sort_executable, sort_arguments, infile, 0+sample_partition_offset_end, outfile, partitions, records);
	if(last_partition_offset_end <= 0) {
		fprintf(stderr, "Partitioning failed. Quitting...\n");
		return 0;
	}

	gettimeofday(&current, 0);
	part_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
	part_time = part_end_time - part_start_time;
	fprintf(stdout, "Partition time is %llu\n", part_time);

	free(sort_arguments);

	fprintf(stdout, "Waiting for tasks to complete...\n");
	long long unsigned int parallel_start_time, parallel_end_time, parallel_time;
	gettimeofday(&current, 0);
	parallel_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;

	char *record_task_times_file = (char *)malloc((strlen(outfile)+11) * sizeof(char));
	sprintf(record_task_times_file, "%s.tasktimes", outfile);
	wait_partition_tasks(q, 5, record_task_times_file);
	free(record_task_times_file);

	gettimeofday(&current, 0);
	parallel_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
	parallel_time = parallel_end_time - parallel_start_time;
	fprintf(stdout, "Parallel execution time is %llu\n", parallel_time);

	long long unsigned int merge_start_time, merge_end_time, merge_time;
	gettimeofday(&current, 0);
	merge_start_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;

	merge_sorted_outputs(outfile, outfile, created_partitions);

	gettimeofday(&current, 0);
	merge_end_time = ((long long unsigned int) current.tv_sec) * 1000000 + current.tv_usec;
	merge_time = merge_end_time - merge_start_time;
	fprintf(stdout, "Merge time is %llu\n", merge_time);

	fprintf(stdout, "Sorting complete. Output is at: %s!\n", outfile);

	execn_time = merge_end_time - execn_start_time;
	workload_runtime = merge_end_time - part_start_time;
	fprintf(stdout, "Workload execn time is %llu\n", workload_runtime);
	fprintf(stdout, "Total execn time is %llu\n", execn_time);

	FILE *time_file = fopen("wq_sort.times", "w");
	if (time_file) {
		fprintf(time_file, "Partition time: %llu\n", part_time);
		fprintf(time_file, "Parallel time: %llu\n", parallel_time);
		fprintf(time_file, "Merge time: %llu\n", merge_time);
		if(sample_env)
			fprintf(time_file, "Sampling time: %llu\n", sample_time);
		fprintf(time_file, "Workload execution time: %llu\n", workload_runtime);
		fprintf(time_file, "Total execution time: %llu\n", execn_time);
	}
	fclose(time_file);

	work_queue_delete(q);

	free(outfile);
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
