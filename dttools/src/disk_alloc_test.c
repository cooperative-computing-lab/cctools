/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "disk_alloc.h"
#include "stringtools.h"

int disk_del(int i, char *arg_loc) {

	char *test_loc = (char *) malloc(sizeof(char) * 20);

	while(i > -1) {

		sprintf(test_loc, "%s%d/", arg_loc, i);

		printf("\nAttempting delete of %s.\n", test_loc);

		while(disk_alloc_delete(test_loc) < 0) {
			printf("couldn't delete %s, still trying...\n",test_loc);
			sleep(1);
		}

		printf("Disk allocation cleaned and removed.\n");
		i--;
	}

	free(test_loc);

	return 0;
}

int main(int argc, char **argv) {

	int i, j, result, fail_flag = 0;
	char *arg_loc = argv[1];
	char *arg_size = argv[2];
	char *fs = argv[3];
	int64_t size = string_metric_parse(arg_size) / 1024;
	char *test_loc = (char *) malloc(sizeof(char) * 200);

	for(j = 0; j < 3; j++) {

		i = 0;

		printf("Beginning run #%d.\n", j + 1);
		while(i < 10) {

			sprintf(test_loc, "%s%d/", arg_loc, i);

			result = disk_alloc_create(test_loc, fs, size);

			if(result < 0) {

				printf("Disk allocation failed.\n");
				fail_flag = 1;
				i--;
				break;
			}

			printf("Disk allocation successful.\n");
			i++;
		}

		if(fail_flag == 0) {
			i--;
		}

		disk_del(i, arg_loc);

		printf("\n\nRun #%d complete.\n\n", (j + 1));
		sleep(1);
	}

	free(test_loc);

	return 0;
}
