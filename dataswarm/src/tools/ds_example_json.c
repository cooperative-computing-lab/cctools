/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_json.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

char *dataswarm = "{ \"name\" : \"json_example_ds\" , \"port\" : 1234 }";

int main(int argc, char *argv[])
{

	struct ds_manager *q;
	int taskid;
	char *task, *t;
	size_t len = 0;
	ssize_t read;

	if(argc < 2) {
		printf("./example <tasks_json>\n");
		return 0;
	}

	q = ds_json_create(dataswarm);
	if(!q) {
		return 1;
	}

	/* read from tasks file and create a task for each line */
	char *filename = argv[1];
	FILE *fp = fopen(filename, "r");
	if(!fp) {
		printf("cannot open file: %s\n", filename);
		return 1;
	}

	while((read = getline(&task, &len, fp)) != -1) {

		taskid = ds_json_submit(q, task);

		if(taskid < 0) {
			return 1;
		}

		printf("submitted task (id# %d)\n", taskid);

	}

	fclose(fp);

	printf("waiting for tasks to complete...\n");

	while(!ds_empty(q)) {

		t = ds_json_wait(q, 5);
        if(t) {
            printf("%s\n", t);
        }
	}

	printf("all tasks complete!\n");

	ds_delete(q);

	return 0;

}
