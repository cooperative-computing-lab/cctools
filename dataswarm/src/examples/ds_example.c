/*
 * Copyright (C) 2022- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

/*
 * This program is a very simple example of how to use the Data Swarm.
 * It accepts a list of files on the command line.
 * Each file is compressed with gzip and returned to the user.
 * */

#include "dataswarm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
	struct ds_manager *q;
	struct ds_task *t;
	int taskid;
	int i;
	char *gzip_path;

	if(argc < 2) {
		printf("ds_example <file1> [file2] [file3] ...\n");
		printf("Each file given on the command line will be compressed using a remote worker.\n");
		return 0;
	}

	/*
	   Usually, we can execute the gzip utility by simply typing its name at a
	   terminal. However, this is not enough for dataswarm; we have to specify
	   precisely which files need to be transmitted to the workers. We record
	   the location of gzip in 'gzip_path', which is usually found in /bin/gzip
	   or /usr/bin/gzip. We use the 'access' function (from unistd.h standard C
	   library), and test the path for execution (X_OK) and reading (R_OK)
	   permissions.
	 */
	gzip_path = "/bin/gzip";
	if(access(gzip_path, X_OK | R_OK) != 0) {
		gzip_path = "/usr/bin/gzip";
		if(access(gzip_path, X_OK | R_OK) != 0) {
			fprintf(stderr, "gzip was not found. Please modify the gzip_path variable accordingly. To determine the location of gzip, from the terminal type: which gzip (usual locations are /bin/gzip and /usr/bin/gzip)\n");
			exit(1);
		}
	}

	/* We create the tasks queue using the default port. If this port is
	 * already been used by another program, you can try changing the argument
	 * to ds_create to 0 to use an available port.  */
	q = ds_create(DS_DEFAULT_PORT);
	if(!q) {
		printf("couldn't create queue: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", ds_port(q));

	/* We create and dispatch a task for each filename given in the argument list */
	for(i = 1; i < argc; i++) {

		char infile[256], outfile[256], command[1024];

		sprintf(infile, "%s", argv[i]);
		sprintf(outfile, "%s.gz", argv[i]);

		/* Note that we write ./gzip here, to guarantee that the gzip version
		 * we are using is the one being sent to the workers. */
		sprintf(command, "./gzip < %s > %s", infile, outfile);

		t = ds_task_create(command);

		/* gzip is the same across all tasks, so we can cache it in the
		 * workers. Note that when specifying a file, we have to name its local
		 * name (e.g. gzip_path), and its remote name (e.g. "gzip"). Unlike the
		 * following line, more often than not these are the same. */
		ds_task_specify_file(t, gzip_path, "gzip", DS_INPUT, DS_CACHE);

		/* files to be compressed are different across all tasks, so we do not
		 * cache them. This is, of course, application specific. Sometimes you
		 * may want to cache an output file if is the input of a later task.*/
		ds_task_specify_file(t, infile, infile, DS_INPUT, DS_NOCACHE);
		ds_task_specify_file(t, outfile, outfile, DS_OUTPUT, DS_NOCACHE);

		/* Once all files has been specified, we are ready to submit the task to the queue. */
		taskid = ds_submit(q, t);

		printf("submitted task (id# %d): %s\n", taskid, ds_task_get_command(t) );
}

	printf("waiting for tasks to complete...\n");

	while(!ds_empty(q)) {

		/* Application specific code goes here ... */

		/* ds_wait waits at most 5 seconds for some task to return. */
		t = ds_wait(q, 5);

		if(t) {
			printf("task (id# %d) complete: %s (return code %d)\n",
				ds_task_get_taskid(t),
				ds_task_get_command(t),
				ds_task_get_result(t) );

			if( ds_task_get_result(t) != 0)
			{
				/* The task failed. Error handling (e.g., resubmit with new parameters) here. */
			}

			ds_task_delete(t);
		}

		/* Application specific code goes here ... */
	}

	printf("all tasks complete!\n");

	ds_delete(q);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
