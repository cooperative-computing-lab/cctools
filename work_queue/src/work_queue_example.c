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

#include <work_queue.h>
#include <debug.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/param.h>

/*
  Usually, we can execute the gzip utility by simply typing its name at a
  terminal. However, this is not enough for work queue; we have to specify
  precisely which files need to be transmitted to the workers. To this extent,
  the following function finds the location of an executable, using the value
  of the PATH variable.
*/
int find_executable(const char *executable, char *location);

int main(int argc, char *argv[])
{
	struct work_queue *q;
	struct work_queue_task *t;
	char executable[PATH_MAX]; 
	int port = WORK_QUEUE_DEFAULT_PORT;
	int taskid;
	int i;

	if(argc < 2) {
		printf("work_queue_example <executable> <file1> [file2] [file3] ...\n");
		printf("Each file given on the command line will be compressed using a remote worker.\n");
		return 0;
	}

	debug_flags_set("all");

	find_executable("gzip", executable);

	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}

	printf("listening on port %d...\n", work_queue_port(q));

	for(i = 1; i < argc; i++) {

		char infile[256], outfile[256], command[256];

		sprintf(infile, "%s", argv[i]);
		sprintf(outfile, "%s.gz", argv[i]);
		sprintf(command, "./gzip < %s > %s", infile, outfile);

		t = work_queue_task_create(command);
		work_queue_task_specify_file(t, executable, "gzip", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE); 
		work_queue_task_specify_file(t, infile, infile, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
		work_queue_task_specify_file(t, outfile, outfile, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE); 
		
		taskid = work_queue_submit(q, t);

		printf("submitted task (id# %d): %s\n", taskid, t->command_line);
	}

	printf("waiting for tasks to complete...\n");

	while(!work_queue_empty(q)) {

		t = work_queue_wait(q, 5);
		if(t) {
			printf("task (id# %d) complete: %s (return code %d)\n", t->taskid, t->command_line, t->return_status);
			work_queue_task_delete(t);
		}
	}

	printf("all tasks complete!\n");

	work_queue_delete(q);

	return 0;
}

int find_executable(const char *executable, char *location) {
	char *paths_org;
	char *paths;
	char p[PATH_MAX];
	char *d;

	paths_org = getenv("PATH");
	if(!paths_org)
		return 0;

	paths = malloc(strlen(paths_org) + 1);
	strcpy(paths, paths_org);
	while((d = strsep(&paths, ":"))) {
		if(*d == '\0')
		{
			d = ".";
		}

		snprintf(p, PATH_MAX, "%s/%s", d, executable);

		if(access(p, X_OK) == 0)
		{
			strcpy(location, p);
			free(paths);
			return 1;
		}
	}

	free(paths);
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
