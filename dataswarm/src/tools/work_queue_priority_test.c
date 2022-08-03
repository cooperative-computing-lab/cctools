/*
 * Copyright (C) 2008- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

/*
 * This program is a very simple example of how to use the Work Queue.
 * It accepts a list of files on the command line.
 * Each file is compressed with gzip and returned to the user.
 * */

#include "work_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
	struct work_queue *q;
	struct work_queue_task *t;
	int port = WORK_QUEUE_DEFAULT_PORT;
	int taskid;
	int i;

	if(argc < 2) {
		printf("work_queue_priority_test priority_task_1 priority_task_2 ...\n");
		return 0;
	}

	/* We create the tasks queue using the default port. If this port is
	 * already been used by another program, you can try setting port = 0 to
	 * use an available port.  */
	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", work_queue_port(q));

	/* We create and dispatch a task for each priority given in the argument list */
	for(i = 1; i < argc; i++) {

		char infile[256], outfile[256], command[1024];

		sprintf(outfile, "test/priority/%s", argv[i]);
		sprintf(infile, "/bin/sleep");

		/* Note that we write ./gzip here, to guarantee that the gzip version
		 * we are using is the one being sent to the workers. */
		sprintf(command, "./sleep 1 && date +'%%s' > %s", outfile);

		t = work_queue_task_create(command);

		work_queue_task_specify_file(t, infile, "sleep", WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
		work_queue_task_specify_file(t, outfile, outfile, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
		work_queue_task_specify_tag(t, argv[i]);

		work_queue_task_specify_priority(t, atof(argv[i]));

		/* Once all files has been specified, we are ready to submit the task to the queue. */
		taskid = work_queue_submit(q, t);

		printf("submitted task (id# %d): %s\n", taskid, t->command_line);
	}

	printf("waiting for tasks to complete...\n");

	while(!work_queue_empty(q)) {

		/* work_queue_wait waits at most 5 seconds for some task to return. */
		t = work_queue_wait(q, 5);

		if(t) {
			printf("%s task completed\n", t->tag);

			work_queue_task_delete(t);
		}

	}

	work_queue_delete(q);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
