/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
An example of a task using a minitask (vine_file_untgz) to unpack a dependency before using it.
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

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	vine_enable_debug_log(m,"manager.log");
	vine_set_scheduler(m,VINE_SCHEDULE_FILES);
	vine_set_name(m, "bslydelg_test");

	for(i=0;i<5;i++) {

		struct vine_file *infile = vine_file_unponcho(vine_file_local("package.tar.gz"));
		struct vine_task *task = vine_task_create("./poncho_package_run -d -e package python python_example.py");
        	vine_task_add_input(task, vine_file_local("poncho_package_run"), "poncho_package_run", VINE_CACHE);
        	vine_task_add_input(task, vine_file_local("python_example.py"), "python_example.py", VINE_CACHE);
		vine_task_add_input(task,infile,"package",VINE_CACHE);		
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
