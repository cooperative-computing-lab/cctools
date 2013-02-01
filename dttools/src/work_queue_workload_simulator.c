/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This program is a very simple example of how to use the Work Queue.
It accepts a list of files on the command line.
Each file is compressed with gzip and returned to the user.
*/

#include "work_queue.h"

#include "cctools.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "itable.h"
#include "list.h"
#include "get_line.h"
#include "get_canonical_path.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


static FILE *logfile = NULL;
static struct list *created_files;

struct task_series {
	int submit_time;
	int input_size;
	int execution_time;
	int output_size;	
	int num_of_tasks;
};



struct list *get_workload_specs(const char *path) {
	
	struct list *specs = list_create();
	if(!specs) {
		fprintf(stderr, "Cannot allocate memory for creating list!\n");
		return NULL;
	}

	// Read in new configuration from file
	FILE *fp;

	fp = fopen(path, "r");
	if(!fp) {
		fprintf(stderr, "Failed to open workload specification file at %s.\n", path);
		return NULL;
	}

	char *line;
	int line_count = 0;
	while((line = get_line(fp))) {
		line_count++;

		string_chomp(line);
		if(string_isspace(line)) { // skip empty lines
			continue;
		}
		if(line[0] == '#') { // skip comment lines
			continue;
		}
		
		int submit, input, exe, output, num;
		if(sscanf(line, "%d %d %d %d %d", &submit, &input, &exe, &output, &num) == 5) {
			if(submit < 0 || input <=0 || exe <=0 || output <=0 || num <=0) {
				fprintf(stderr, "Other than the submit_time field, every other field should be greater than 0.\n");
				goto fail;
			}

			struct task_series *ts = (struct task_series *)xxmalloc(sizeof(struct task_series));
			ts->submit_time = submit;
			ts->input_size = input;
			ts->execution_time = exe;
			ts->output_size = output;
			ts->num_of_tasks = num;

			list_push_priority(specs, ts, ts->submit_time);
		} else {
			fprintf(stderr, "Line %d is invalid: %s\n", line_count, line);
			goto fail;
		}
	}
	fclose(fp);
	return specs;

fail:
	// delete list
	list_free(specs);
	list_delete(specs);
	fclose(fp);
	return NULL;
}

int submit_task_series(struct work_queue *q, struct task_series *ts, int series_id) {
	char input_file[128], output_file[128], command[256];
	char gen_input_cmd[256];

	sprintf(input_file, "input-%d", series_id);
	list_push_tail(created_files, xxstrdup(input_file));
	sprintf(gen_input_cmd, "dd if=/dev/zero of=%s bs=1M count=%d", input_file, ts->input_size);
	system(gen_input_cmd);

	// submit tasks to the queue
	int i;
	for(i = 0; i < ts->num_of_tasks; i++) {
		sprintf(output_file, "output-%d-%d", series_id, i);
		list_push_tail(created_files, xxstrdup(output_file));
		sprintf(command, "dd if=/dev/zero of=%s bs=1M count=%d; sleep %d", output_file, ts->output_size, ts->execution_time);

		struct work_queue_task *t = work_queue_task_create(command);
		if (!work_queue_task_specify_file(t, input_file, input_file, WORK_QUEUE_INPUT, WORK_QUEUE_CACHE)) {
			printf("task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", input_file);
			return 0; 	
		}
		if (!work_queue_task_specify_file(t, output_file, output_file, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE)) {
			printf("task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", output_file);
			return 0; 	
		}	
		int taskid = work_queue_submit(q, t);

		printf("submitted task (id# %d): %s\n", taskid, t->command_line);
	}
	return 1; // success
}

void log_work_queue_status(struct work_queue *q) {
	struct work_queue_stats s;
	work_queue_get_stats(q, &s);

	fprintf(logfile, "QUEUE %llu %d %d %d %d %d %d %d %d %d %d %lld %lld %.2f %.2f %d %d %d\n", timestamp_get(), s.workers_init, s.workers_ready, s.workers_busy, s.tasks_running, s.tasks_waiting, s.tasks_complete, s.total_tasks_dispatched, s.total_tasks_complete, s.total_workers_joined, s.total_workers_removed, s.total_bytes_sent, s.total_bytes_received, s.efficiency, s.idle_percentage, s.capacity, s.avg_capacity, s.total_workers_connected);

	fflush(logfile);
	fsync(fileno(logfile));
}

void wait_for_task(struct work_queue *q, int timeout) {
	struct work_queue_task *t = work_queue_wait(q,timeout);
	if(t) {
		printf("task (id# %d) complete: %s (return code %d)\n", t->taskid, t->command_line, t->return_status);
		work_queue_task_delete(t);
		log_work_queue_status(q);
	}
}

void remove_created_files() {
	char *filename;
	int i = 0;
	list_first_item(created_files);
	while((filename = (char *)list_next_item(created_files))) {
		if(unlink(filename) == 0) {
			printf("File removed: %s\n", filename);
			i++;
		}
	}
	printf("%d created files are removed\n", i);
	list_free(created_files);
	list_delete(created_files);
}

int main(int argc, char *argv[])
{
	struct work_queue *q;
	int port = WORK_QUEUE_DEFAULT_PORT;

	if(argc != 4) {
		printf("Usage: work_queue_workload_simulator <workload_spec> <logfile> <proj_name> \n");
		exit(1);
	}

	struct list *specs = get_workload_specs(argv[1]);
	if(!specs) {
		printf("Failed to load a non-empty workload specification.\n");
		exit(1);
	}

	created_files = list_create(); 
	if(!created_files) {
		printf("Failed to allocate memory for a list to store created files.\n");
		exit(1);
	}

	// open log file
	logfile = fopen(argv[2], "a");
	if(!logfile) {
		printf("Couldn't open logfile %s: %s\n", argv[2], strerror(errno));
		exit(1);
	}

	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		goto fail;
		exit(1);
	}

	printf("listening on port %d...\n", work_queue_port(q));

	// specifying the right modes
	work_queue_specify_master_mode(q, WORK_QUEUE_MASTER_MODE_CATALOG);
	work_queue_specify_name(q, argv[3]);
	work_queue_specify_estimate_capacity_on(q, 1); // report capacity on

	int time_elapsed = 0; // in seconds 
	int series_id = 0;
	time_t start_time = time(0);
	log_work_queue_status(q);
	while(1) {
		struct task_series *ts = (struct task_series *)list_peek_tail(specs);
		if(!ts) {
			while(!work_queue_empty(q)) { // wait until all tasks to finish
				wait_for_task(q, 5);
			}
			break;
		} else {
			time_elapsed = time(0) - start_time;
			int time_until_next_submit = ts->submit_time - time_elapsed;
			if(time_until_next_submit <=0) {
				list_pop_tail(specs);
				printf("time elapsed: %d seconds\n", time_elapsed);
				if(!submit_task_series(q, ts, series_id)) {
					// failed to submit tasks
					fprintf(stderr, "Failed to submit tasks.\n");
					goto fail;
				}
				free(ts);
				series_id++;
			} else {
				time_t stoptime = start_time + ts->submit_time;
				while(!work_queue_empty(q)) {
					int timeout = stoptime - time(0);
					if(timeout > 0) {
						wait_for_task(q, timeout);
					} else {
						break;
					}
				}
				time_t current_time = time(0);
				if(current_time < stoptime) {
					sleep(stoptime - current_time);
				}
			}
		}
	}

	printf("all tasks complete!\n");
	work_queue_delete(q);
	remove_created_files();
	fclose(logfile);

	return 0;

fail:
	remove_created_files();
	fclose(logfile);
	exit(1);
}
