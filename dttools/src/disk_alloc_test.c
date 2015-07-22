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

int disk_del(int i, int k[10], char *arg_loc) {

	char *test_loc = (char *) malloc(sizeof(char) * 20);

	while(i > -1) {

		sprintf(test_loc, "%s%d/", arg_loc, i);

		printf("\nAttempting delete of %s on device: %d.\n", test_loc, k[i]);

		while(disk_alloc_delete(test_loc, k[i]) < 0) {
			printf("couldn't delete %s, still trying...\n",test_loc);
			sleep(1);
		}

		i--;
	}

	free(test_loc);

	return 0;
}

int main(int argc, char **argv) {

	int i, j, result, fail_flag = 0;
	int k[10];
	char *arg_loc = argv[1];
	char *test_loc = (char *) malloc(sizeof(char) * 20);

	for(j = 0; j < 100; j++) {

		i = 0;

		while(i < 10) {

			sprintf(test_loc, "%s%d/", arg_loc, i);

			result = disk_alloc_create(test_loc, "300m");

			if(result < 0) {

				fail_flag = 1;
				i--;
				break;
			}

			k[i] = result;
			i++;
		}

		if(fail_flag == 0) {
			i--;
		}

		disk_del(i, k, arg_loc);

		printf("\n\nRun #%d complete.\n\n", (j + 1));
		sleep(2);
	}

	free(test_loc);

	return 0;
}
