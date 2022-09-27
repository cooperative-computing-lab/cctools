/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example program shows the behavior of the DS_WATCH flag.

If a task produces output to a file incrementally as it runs,
it can be helpful to see that output piece by piece as it
is produced. By simply adding the DS_WATCH flag to the output
of the program, dataswarm will periodically check for output
and return it to the manager while each task runs.  When the
task completes, any remaining output is fetched.

This example runs several instances of the task named
ds_example_watch_trickle.sh, which gradually produces output
every few seconds.  While running the manager program, open
up another terminal, and observe that files output.0, output.1,
etc are gradually produced throughout the run.
*/

#include "dataswarm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
	struct ds_manager *m;
	struct ds_task *t;

	m = ds_create(DS_DEFAULT_PORT);
	if(!m) {
		printf("Couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("Listening on port %d...\n", ds_port(m));

	ds_specify_debug_log(m,"manager.log");

	int i;
	for(i=0;i<10;i++) {
		char output[256];
		sprintf(output,"output.%d",i);
		t = ds_task_create("./ds_example_watch_trickle.sh > output");
		ds_task_specify_file(t, "ds_example_watch_trickle.sh", "ds_example_watch_trickle.sh", DS_INPUT, DS_CACHE );
		ds_task_specify_file(t, output, "output", DS_OUTPUT, DS_WATCH );
		ds_task_specify_cores(t,1);
		ds_submit(m, t);
	}

	printf("Waiting for tasks to complete...\n");

	while(!ds_empty(m)) {
		t = ds_wait(m, 5);
		if(t) {
			ds_result_t r = ds_task_get_result(t);
                        int id = ds_task_get_taskid(t);

			if(r==DS_RESULT_SUCCESS) {
				printf("Task %d complete: %s\n",id,ds_task_get_command(t));
                        } else {
                                printf("Task %d failed: %s\n",id,ds_result_string(r));
                        }

                        ds_task_delete(t);
		}
	}

	printf("All tasks complete!\n");

	ds_delete(m);

	return 0;
}
