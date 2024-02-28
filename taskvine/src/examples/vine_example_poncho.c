/*
An example of a task using a minitask (vine_declare_poncho) to unpack a dependency before using it.
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

	struct vine_file *script = vine_declare_file(m, "script_example_for_poncho.py", VINE_CACHE_LEVEL_WORKFLOW, 0);

	struct vine_file *tarball = vine_declare_file(m, "package.tar.gz", VINE_CACHE_LEVEL_WORKFLOW, 0);
	struct vine_file *package = vine_declare_poncho(m, tarball, VINE_CACHE_LEVEL_WORKFLOW, 0);

	for(i=0;i<5;i++) {

		struct vine_task *task = vine_task_create("python my_script.py");
		vine_task_add_poncho_package(task, package);

		vine_task_add_input(task, script, "my_script.py", 0);

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

/* vim: set noexpandtab tabstop=4: */
