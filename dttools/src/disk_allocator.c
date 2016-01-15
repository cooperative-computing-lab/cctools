/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>

#include "disk_alloc.h"
#include "stringtools.h"

int main(int argc, char *argv[]) {

	char *func = argv[1];
	char *loc = argv[2];
	char *fs = argv[3];
	int result;

	if(strstr(func, "create") != NULL) {

		int64_t size = (string_metric_parse(argv[3]) / 1024);
		result = disk_alloc_create(loc, fs, size);

		if(result != 0) {

			printf("Could not create allocation.\n");
			return -1;
		}

		printf("Allocation complete.\n");
		return 0;
	}

	else if(strstr(func, "delete") != NULL) {

		result = disk_alloc_delete(loc);

		if(result != 0) {

			printf("Could not delete allocation.\n");
			return -1;
		}

		printf("Deallocation complete.\n");
		return 0;
	}

	else {

		printf("Invalid parameters defined.\n");
		return -1;
	}
}
