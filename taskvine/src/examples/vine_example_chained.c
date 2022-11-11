/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
An example of using file chaining to pull in dependencies of dependencies.
*/

#include "taskvine.h"

/* This is a temporary hack to access functions not yet in the public API. */

#include "vine_file.h"
#include "vine_task.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#define CCTOOLS_URL "http://ccl.cse.nd.edu/software/files/cctools-7.4.14-source.tar.gz"

int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;
	int i;

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	vine_enable_debug_log(m,"manager.log");
	vine_set_scheduler(m,VINE_SCHEDULE_FILES);

	for(i=0;i<10;i++) {
			  
		struct vine_task *minitask = vine_task_create("tar xvzf cctools.tar.gz");
		vine_task_add_input_url(minitask,CCTOOLS_URL,"cctools.tar.gz",VINE_CACHE);
		vine_task_add_output_file(minitask,"cctools","cctools-7.4.14-source",VINE_CACHE);

		struct vine_file *file = vine_file_mini_task(minitask);
		
		struct vine_task *task = vine_task_create("ls -lR cctools");
		vine_task_add_input(task,file,"cctools",VINE_CACHE);

		int task_id = vine_submit(m, task);

		printf("submitted task (id# %d): %s\n", task_id, vine_task_get_command(task) );
	}

	printf("waiting for tasks to complete...\n");

	while(!vine_empty(m)) {
		t  = vine_wait(m, 5);
		if(t) {
			vine_result_t r = vine_task_get_result(t);
			int id = vine_task_get_id(t);

			if(r==VINE_RESULT_SUCCESS) {
				printf("task %d output: %s\n",id,vine_task_get_stdout(t));
			} else {
				printf("task %d failed: %s\n",id,vine_result_string(r));
			}
			vine_task_delete(t);
		}
	}

	printf("all tasks complete!\n");

	vine_delete(m);

	return 0;
}
