/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example program shows the behavior of the VINE_WATCH flag.

If a task produces output to a file incrementally as it runs,
it can be helpful to see that output piece by piece as it
is produced. By simply adding the VINE_WATCH flag to the output
of the program, taskvine will periodically check for output
and return it to the manager while each task runs.  When the
task completes, any remaining output is fetched.

This example runs several instances of the task named
vine_example_watch_trickle.sh, which gradually produces output
every few seconds.  While running the manager program, open
up another terminal, and observe that files output.0, output.1,
etc are gradually produced throughout the run.
*/

#include "taskvine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;

	//runtime logs will be written to vine_example_watch_info/%Y-%m-%dT%H:%M:%S
	vine_set_runtime_info_path("vine_example_watch_info");

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("Couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("Listening on port %d...\n", vine_port(m));

	int i;
	for(i=0;i<10;i++) {
		char output[256];
		sprintf(output,"output.%d",i);
		t = vine_task_create("./vine_example_watch_trickle.sh > output");
		vine_task_add_input_file(t, "vine_example_watch_trickle.sh", "vine_example_watch_trickle.sh", VINE_CACHE );
		vine_task_add_output_file(t, output, "output", VINE_WATCH );
		vine_task_set_cores(t,1);
		vine_submit(m, t);
	}

	printf("Waiting for tasks to complete...\n");

	while(!vine_empty(m)) {
		t = vine_wait(m, 5);
		if(t) {
			vine_result_t r = vine_task_get_result(t);
                        int id = vine_task_get_id(t);

			if(r==VINE_RESULT_SUCCESS) {
				printf("Task %d complete: %s\n",id,vine_task_get_command(t));
                        } else {
                                printf("Task %d failed: %s\n",id,vine_result_string(r));
                        }

                        vine_task_delete(t);
		}
	}

	printf("All tasks complete!\n");

	vine_delete(m);

	return 0;
}
