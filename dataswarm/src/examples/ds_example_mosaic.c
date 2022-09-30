/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example program produces a mosaic of images, each one transformed
with a different amount of swirl.

It demonstrates several features of dataswarm:

- Each task consumes remote data accessed via url, cached and shared
among all tasks on that machine.

- Each task uses the "convert" program, which may or may not be installed
on remote machines.  To make the tasks portable, the program "/usr/bin/convert"
is packaged up into a self-contained archive "convert.sfx" which contains
the executable and all of its dynamic dependencies.  This allows the
use of arbitrary workers without regard to their software environment.
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
	
	m = ds_create(DS_DEFAULT_PORT);
	if(!m) {
		printf("Couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("Listening on port %d...\n", ds_port(m));

	ds_specify_debug_log(m,"manager.log");

	int i;
	for(i = 0; i < 360; i+=10) {
		char outfile[256];
		char command[1024];

		sprintf(outfile, "%d.cat.jpg",i);
		sprintf(command, "./convert.sfx -swirl %d cat.jpg %d.cat.jpg", i, i);

		t = ds_task_create(command);
		ds_task_specify_input_file(t, "convert.sfx", "convert.sfx", DS_CACHE);
		ds_task_specify_input_url(t,"https://upload.wikimedia.org/wikipedia/commons/7/74/A-Cat.jpg", "cat.jpg", DS_CACHE );
		ds_task_specify_output_file(t,outfile,outfile,DS_NOCACHE);

		ds_task_specify_cores(t,1);

		int taskid = ds_submit(m, t);

		printf("Submitted task (id# %d): %s\n", taskid, ds_task_get_command(t) );
	}

	printf("Waiting for tasks to complete...\n");

	while(!ds_empty(m)) {
		t = ds_wait(m, 5);
		if(t) {
			ds_result_t result = ds_task_get_result(t);
                        int id = ds_task_get_taskid(t);

			if(result==DS_RESULT_SUCCESS) {
				printf("Task %d complete: %s\n",id,ds_task_get_command(t));
                        } else {
                                printf("Task %d failed: %s\n",id,ds_result_string(r));
                        }

                        ds_task_delete(t);
		}
	}

	printf("All tasks complete!\n");

	ds_delete(m);

	printf("Combining images into mosaic.jpg...\n");
	system("montage `ls *.cat.jpg | sort -n` -tile 6x6 -geometry 128x128+0+0 mosaic.jpg");

	printf("Deleting intermediate images...\n");
	for(i=0;i<360;i+=10) {
		char filename[256];
		sprintf(filename,"%d.cat.jpg",i);
		unlink(filename);
	}

	return 0;
}
