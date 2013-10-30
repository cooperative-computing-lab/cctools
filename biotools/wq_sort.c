/*
  Copyright (C) 2013- The University of Notre Dame
  This software is distributed under the GNU General Public License.
  See the file COPYING for details.
*/

/* 
  Distributed sort using Work Queue.
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

#define LINE_SIZE 2048

static int partitions = 20;

int get_total_lines(char *infile) {
	FILE *input_file = fopen(infile, "r");
	int line_count = 0; 
	int ch;
	
	while ((ch=fgetc(input_file)) != EOF) {
		if (ch=='\n') 
	        ++line_count;
	}
	fclose(input_file);

	return line_count;
}

//Returns the end byte offset for a given line number in file
off_t get_file_line_end_offset(char *filename, int line_number) {
	FILE *fs = fopen(filename, "r");
	int line_count = 0; 
	int ch;
	long end_offset = -1;
	
	while ((ch=fgetc(fs)) != EOF && line_count < line_number) {
		if (ch == '\n') 
	        ++line_count;
	}

	if(line_count == line_number) {
		end_offset = ftell(fs);	
	}
	
	fclose(fs);
	return (end_offset-2); //subtract two to rewind back to newline at end of line	
}

/* Partition the input file according to the number of partitions specified and
 * create tasks that sort each of these partitions.
 */
int submit_tasks(struct work_queue *q, char *executable, char *executable_args, char *infile, char *outfile_prefix) {
	char outfile[256], remote_infile[256], remote_executable[256], command[256];
	struct work_queue_task *t;
	int taskid;
	int task_count = 0;	
	
	long prev_file_offset_end;
	int task_end_line = 0;
	long file_offset_end = -1;
	int lines_to_submit;

	int number_lines = get_total_lines(infile);
	int lines_per_task = (int)ceil((double)number_lines/partitions); 
	
	char *executable_dup = strdup(executable);

	if (strchr(executable, '/')) {
		strcpy(remote_executable, basename(executable_dup));
	} else {
		strcpy(remote_executable, executable);
	}
	if (strchr(infile, '/')) {
		strcpy(remote_infile, basename(infile));
	} else {
		strcpy(remote_infile, infile);
	}

	free(executable_dup);
	
	while(task_end_line < number_lines) {
		//we partition input into pieces by tracking the file offset of the lines in it.
		prev_file_offset_end = file_offset_end;	
		lines_to_submit = (number_lines - task_end_line) < lines_per_task ? (number_lines - task_end_line) : lines_per_task;	
		task_end_line += lines_to_submit;
		file_offset_end = get_file_line_end_offset(infile, task_end_line);		
	
		//create and submit tasks for sorting the pieces.
		sprintf(outfile, "%s.%d", outfile_prefix, task_count);
		if (executable_args){	
			sprintf(command, "./%s %s %s > %s", executable, executable_args, remote_infile, outfile);
		} else {
			sprintf(command, "./%s %s > %s", executable, remote_infile, outfile);
		}

		t = work_queue_task_create(command);
		if (!work_queue_task_specify_file_piece(t, infile, remote_infile, (off_t)prev_file_offset_end+1, (off_t)file_offset_end, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE)) {
			printf("task_specify_file_piece() failed for %s: check if arguments are null or remote name is an absolute path.\n", infile);
			return 0;	
		}
		if (!work_queue_task_specify_file(t, executable, remote_executable, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE)) {
			printf("task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", executable);
			return 0;	
		}
		if (!work_queue_task_specify_file(t, outfile, outfile, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE)) {
			printf("task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", outfile);
			return 0;	
		}
	
		taskid = work_queue_submit(q, t);
		printf("submitted task (id# %d): %s\n", taskid, t->command_line);
		task_count++;
	}
	
	return task_count;
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
int merge_sorted_outputs(char *outfile_prefix, int number_files) {
	char outfile[256], merged_output_file[256];
	int *outfile_line_vals;	
	FILE **outfile_ptrs;
	FILE *merged_output_fp;
	int min_pos, min_value;
	int processed_output_files = 0;
	int i;	

	sprintf(merged_output_file, "%s", outfile_prefix);	
	merged_output_fp = fopen(merged_output_file, "w");
	if(!merged_output_fp) {
		fprintf(stderr, "Opening file %s failed: %s!\n", merged_output_file, strerror(errno));
		return -1;	
	}	
	
	outfile_line_vals = malloc(sizeof(int) * number_files);
	outfile_ptrs = malloc(sizeof(FILE *) * number_files);

	for(i = 0; i < number_files; i++) {
		sprintf(outfile, "%s.%d", outfile_prefix, i);	
		outfile_ptrs[i] = fopen(outfile, "r");
		if(!outfile_ptrs[i]) {
			fprintf(stderr, "Opening file %s failed: %s!\n", outfile, strerror(errno));
			goto cleanup;	
			return -1;	
		}
	}

	//read the first lines of each output file into the array
	for(i = 0; i < number_files; i++) {
		outfile_line_vals[i] = get_file_line_value(outfile_ptrs[i]);
	}

	//compute the minimum of array and load a new value from the contributing
	//file into the array index of the minimum.
	while (processed_output_files < number_files) {
		min_value = find_min(outfile_line_vals, number_files, &min_pos);
		fprintf(merged_output_fp, "%d\n", min_value); //write current min value to merged output file
		outfile_line_vals[min_pos] = get_file_line_value(outfile_ptrs[min_pos]);
		if (outfile_line_vals[min_pos] < 0) {	
			processed_output_files++;	
		}
	}

  cleanup:
	for(i = 0; i < number_files; i++) {
		fclose(outfile_ptrs[i]);
	}
	free(outfile_line_vals);	
	free(outfile_ptrs);	
	fclose(merged_output_fp);	
	
	return 1;
}

static void show_help(const char *cmd)
{
    fprintf(stdout, "Use: %s [options] <sort program> <file 1>\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Specify a project name for the Work Queue master. (default = none)\n", "-N");
	fprintf(stdout, " %-30s Specify the number of partitions to create of the input data. (default = 20)\n", "-k");
	fprintf(stdout, " %-30s Specify the arguments for the sort executable.\n", "-p");
	fprintf(stdout, " %-30s Show this help screen\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	struct work_queue *q;
	int port = 9100;
	int c;

	char *sort_arguments = NULL;
	const char *proj_name = NULL;

	debug_flags_set("all");
	if(argc < 3) {
		show_help(argv[0]);
		return 0;
	}
		
	while((c = getopt(argc, argv, "N:k:p:h")) != (char) -1) {
		switch (c) {
		case 'N':
			proj_name = strdup(optarg);
			break;
		case 'k':
			partitions = atoi(optarg);
			break;
		case 'p':
			sort_arguments = strdup(optarg);
			break;
		case 'h':
			show_help(argv[0]);
			return 0;
		default:
			show_help(argv[0]);
			return -1;
		}
	}

	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}

	printf("listening on port %d...\n", work_queue_port(q));
	
	if(proj_name){
		work_queue_specify_master_mode(q, WORK_QUEUE_MASTER_MODE_CATALOG);	
		work_queue_specify_name(q, proj_name);
	}

	free((void *)proj_name);

	char sort_executable[256]; 
	char infile[256], outfile_prefix[256]; 
	struct work_queue_task *t;	
	int number_tasks = 0;
	char *infile_temp = NULL;

	sprintf(sort_executable, "%s", argv[optind]);
		
	sprintf(infile, "%s", argv[optind+1]);

	infile_temp = strdup(infile);		
	if (strchr(infile, '/')) {
		strcpy(outfile_prefix, basename(infile_temp));
	} else {
		strcpy(outfile_prefix, infile_temp);
	}
	sprintf(outfile_prefix, "%s.sorted", outfile_prefix);
	free(infile_temp);
	
	printf("%s will be run to sort contents of %s\n", sort_executable, infile);
	number_tasks = submit_tasks(q, sort_executable, sort_arguments, infile, outfile_prefix);
	free(sort_arguments);
	if (number_tasks <= 0) {
		printf("No tasks were submitted.\n");
		return 1;
	}
	
	printf("Waiting for tasks to complete...\n");
	while(!work_queue_empty(q)) {
		t = work_queue_wait(q, 5);
		if(t) {
			printf("Task (taskid# %d) complete: %s (return code %d)\n", t->taskid, t->command_line, t->return_status);
			work_queue_task_delete(t);
		}
	}
	merge_sorted_outputs(outfile_prefix, number_tasks);	
	printf("Sorting complete. Ouput is at: %s!\n", outfile_prefix);

	work_queue_delete(q);
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
