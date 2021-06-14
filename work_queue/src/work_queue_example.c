// Work queue example using wsummary

#include "work_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

// displays data summary of worker resources
void display_work_queue_worker_summary(struct work_queue_wsummary *worker_data, char *sortby, int length)
{
	convert_wsummary_to_log_scale(worker_data, &length);
	for (int i = 0; i < length; i++)
	{
		printf( "There %3s %3d %7s with %2d %5s, %6dmb memory, at least %8dmb disk space, and %2d %4s\n",
				(worker_data[i].count == 1) ? "is" : "are",
				worker_data[i].count, (worker_data[i].count == 1) ? "worker" : "workers",
				worker_data[i].cores, (worker_data[i].cores == 1) ? "core" : "cores",
				worker_data[i].memory, worker_data[i].disk,
				worker_data[i].gpus, (worker_data[i].gpus == 1) ? "gpu" : "gpus");
	}
}

int main(int argc, char *argv[])
{
	struct work_queue *q;
	struct work_queue_task *t;
	int taskid;
	int i;
	char *sleep_path; 

	sleep_path = "/bin/sleep";
	if(access(sleep_path, X_OK | R_OK) != 0) {
		sleep_path = "/usr/bin/sleep";
		if(access(sleep_path, X_OK | R_OK) != 0) {
			fprintf(stderr, "sleep was not found. Please modify the sleep_path variable accordingly. To determine the location of sleep, from the terminal type: which sleep (usual locations are /bin/sleep and /usr/bin/sleep)\n");
			exit(1);
		}
	}

	/* We create the tasks queue using the default port. If this port is
	 * already been used by another program, you can try changing the argument
	 * to work_queue_create to 0 to use an available port.  */
	q = work_queue_create(WORK_QUEUE_DEFAULT_PORT);
	if(!q) {
		printf("couldn't create queue: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", work_queue_port(q));

	// This example program creates 500 tasks where each task has the worker sleep for 20 seconds and then return
	for(i = 1; i < 500; i++) {
		char command[128];
		sprintf(command, "./sleep 20");

		t = work_queue_task_create(command);

		work_queue_task_specify_file(t, sleep_path, "sleep", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
		taskid = work_queue_submit(q, t);

		printf("submitted task (id# %d): %s\n", taskid, t->command_line);
	}

	printf("waiting for tasks to complete...\n");
	
	while(!work_queue_empty(q)) {
		struct work_queue_wsummary data[500];
		int data_length = work_queue_worker_summmary(q, data, 500);
		display_work_queue_worker_summary(data, "count", data_length);
		printf("\n");
		t = work_queue_wait(q, 1);

		if(t) {
			printf("task (id# %d) complete: %s (return code %d)\n", t->taskid, t->command_line, t->return_status);
			if(t->return_status != 0)
			{
				/* The task failed. Error handling (e.g., resubmit with new parameters) here. */
			}

			work_queue_task_delete(t);
		}

	}

	printf("all tasks complete!\n");

	work_queue_delete(q);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
