/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
An example of a task using a minitask (vine_file_unponcho) to unpack a dependency before using it.
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
	int i;

    //runtime logs will be written to vine_example_unponcho_worker_info/%Y-%m-%dT%H:%M:%S
	vine_set_runtime_info_path("vine_example_unponcho_worker_info");

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	for(i=0;i<5;i++) {

		struct vine_task *task = vine_task_create("./poncho_package_run -d -e package python python_example.py");
		struct vine_file *infile = vine_file_unponcho(vine_file_local("package.tar.gz"));
		vine_task_add_input(task, infile, "package",VINE_CACHE);		
        	vine_task_add_input(task, vine_file_local("poncho_package_run"), "poncho_package_run", VINE_CACHE);
        	vine_task_add_input(task, vine_file_local("python_example.py"), "python_example.py", VINE_CACHE);
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
