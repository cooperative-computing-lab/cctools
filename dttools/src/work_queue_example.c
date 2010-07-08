
/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This program is a very simple example of how to use the Work Queue.
It accepts a list of files on the command line.
Each file is compressed with gzip and returned to the user.
*/

#include "work_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int main( int argc, char *argv[] ) 
{
	struct work_queue *q;
	struct work_queue_task *t;
	int port = WORK_QUEUE_DEFAULT_PORT;
	int i;

	if(argc<2) {
		printf("work_queue_example <file1> [file2] [file3] ...\n");
		printf("Each file given on the command line will be compressed using a remote worker.\n");
		return 0;
	}

	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n",port,strerror(errno));
		return 1;
	}

	printf("listening on port %d...\n",port);

	for(i=1;i<argc;i++) {

		char infile[256], outfile[256], command[256];

		sprintf(infile,"%s",argv[i]);
		sprintf(outfile,"%s.gz",argv[i]);
		sprintf(command,"/usr/bin/gzip < %s > %s",infile,outfile);

		t = work_queue_task_create(command);
		//work_queue_task_specify_input_file_do_not_cache(t,infile,infile);
		//work_queue_task_specify_output_file_do_not_cache(t,outfile,outfile);
		work_queue_task_specify_input_file(t,infile,infile);
		work_queue_task_specify_output_file(t,outfile,outfile);
		work_queue_submit(q,t);

		printf("submitted task: %s\n",t->command_line);
	}

	printf("waiting for tasks to complete...\n");

	while(!work_queue_empty(q)) {

		t = work_queue_wait(q,5);
		if(t) {
			printf("task complete: %s (return code %d)\n",t->command_line,t->return_status);
			work_queue_task_delete(t);
		}
	}

	printf("all tasks complete!\n");

	work_queue_delete(q);

	return 0;
}

