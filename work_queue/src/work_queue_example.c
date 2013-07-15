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

#include <work_queue.h>
#include <debug.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int main(int argc, char *argv[])
{
	struct work_queue *q;
	struct work_queue_task *t;
	int port = WORK_QUEUE_DEFAULT_PORT;
	int taskid;
	int i, iterations;
	char *url_base, *filename;

	if(argc < 3) {
                printf("work_queue_example <executable> <url1> [url2] [url3] ...\n");
                printf("Each url given on the command line will be downloaded and compressed using a remote worker.\n");
                return 0;
        }

	url_base = argv[1];
	filename = argv[2];
	iterations = atoi(argv[3]);

	debug_flags_set("all");

	q = work_queue_create(port);
	if(!q) {
		printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}

	work_queue_tune(q, "short-timeout", 60);
        work_queue_specify_name(q,"numbtest");
        work_queue_specify_master_mode(q,WORK_QUEUE_MASTER_MODE_CATALOG);

	printf("listening on port %d...\n", work_queue_port(q));

	for(i = 0; i < iterations; i++) {
		char url[256], outfile[256], command[256];
                char* infile;

                sprintf(url, "%s/%s", url_base, filename);
                sprintf(outfile, "%s.gz", filename);                    
                sprintf(command, "./gzip < %s > %s", filename, outfile);

		t = work_queue_task_create(command);	

	       if (!work_queue_task_specify_file(t, "/usr/bin/gzip", "gzip", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE)) {
                        printf("task_specify_URL() failed for /usr/bin/gzip: check if arguments are null or remote anme is an absolute path.\n");
                        return 1;
                }
                if (!work_queue_task_specify_file(t, outfile, outfile, WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE)) {
                        printf("task_specify_file() failed for %s: check if arguments are null or remote name is an absolute path.\n", outfile);
                        return 1;
                }
                if (!work_queue_task_specify_url(t, url, filename, WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE)) {
                        printf("task_specify_url() failed for %s: check if arguments are null or remote name is an absolute path.\n", url);
                        return 1;
                }

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
