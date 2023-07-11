/*
This example program produces a mosaic of images, each one transformed
with a different amount of swirl.

It demonstrates several features of taskvine:

- Each task consumes remote data accessed via url, cached and shared
among all tasks on that machine.

- Each task uses the "convert" program, which may or may not be installed
on remote machines.  To make the tasks portable, the program "/usr/bin/convert"
is packaged up into a self-contained archive "convert.sfx" which contains
the executable and all of its dynamic dependencies.  This allows the
use of arbitrary workers without regard to their software environment.

- Output is sent to temporary files on workers, which are then consumed
by the final "montage" task, regardless of location.
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

	printf("Checking that /usr/bin/convert is installed...\n");
	int r = access("/usr/bin/convert",X_OK);
	if(r!=0) {
		printf("%s: /usr/bin/convert is not installed: this won't work at all.\n",argv[0]);
		return 1;
	}

	printf("Converting /usr/bin/convert into convert.sfx...\n");
	r = system("starch -x /usr/bin/convert -c convert convert.sfx");
	if(r!=0) {
		printf("%s: failed to run starch, is it in your PATH?\n",argv[0]);
		return 1;
	}
	
	printf("Converting /usr/bin/montage into montage.sfx...\n");
	r = system("starch -x /usr/bin/montage -c montage montage.sfx");
	if(r!=0) {
		printf("%s: failed to run starch, is it in your PATH?\n",argv[0]);
		return 1;
	}
	
	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("Couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("Listening on port %d...\n", vine_port(m));

	vine_enable_peer_transfers(m);

	struct vine_file *convert = vine_declare_file(m, "convert.sfx", VINE_CACHE);
	struct vine_file *image = vine_declare_url(m, "https://upload.wikimedia.org/wikipedia/commons/7/74/A-Cat.jpg", VINE_CACHE);

	struct vine_file *temp_file[36];

	int i;
	for(i=0; i<36; i++) {
		char outfile[256];
		char command[1024];

		sprintf(outfile, "%d.cat.jpg",i);
		sprintf(command, "./convert.sfx -swirl %d cat.jpg %d.cat.jpg", i*10, i);

		temp_file[i] = vine_declare_temp(m);

		t = vine_task_create(command);

		vine_task_add_input(t,convert,"convert.sfx",0);
		vine_task_add_input(t,image,"cat.jpg",0);
		vine_task_add_output(t,temp_file[i],outfile,0);

		vine_task_set_cores(t,1);

		int task_id = vine_submit(m, t);

		printf("Submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );
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

	printf("Combining images into mosaic.jpg...\n");

	t = vine_task_create("./montage.sfx `ls *.cat.jpg | sort -n` -tile 6x6 -geometry 128x128+0+0 mosaic.jpg");
	for(i=0;i<36;i++) {
		char filename[256];
		sprintf(filename,"%d.cat.jpg",i);
		vine_task_add_input(t,temp_file[i],filename,0);
	}

	vine_task_add_input(t,vine_declare_file(m,"montage.sfx",VINE_CACHE),"montage.sfx",0);
	vine_task_add_output(t,vine_declare_file(m,"mosaic.jpg",VINE_CACHE_NEVER),"mosaic.jpg",0);

	int task_id = vine_submit(m,t);
	printf("Submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );

	printf("Waiting for tasks to complete...\n");
	t = vine_wait(m,VINE_WAIT_FOREVER);

	printf("All tasks complete!\n");

	vine_task_delete(t);
	vine_delete(m);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
