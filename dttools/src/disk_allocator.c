/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>
#include "disk_alloc.h"
#include "stringtools.h"
#include "cctools.h"

static void show_help(const char *cmd) {

	fprintf(stdout, "Use: %s [options] <create|delete> <target directory> <size (i.e. 100MB)> <filesystem>\n", cmd);
	fprintf(stdout, "Where options are:\n");
	fprintf(stdout, " %-30s This message\n", "-h,--help=<flag>");
	fprintf(stdout, " %-30s Version\n", "-v,--version");
	fprintf(stdout, "\n");
	return;
}

int main(int argc, char *argv[]) {

	int c;
	static const struct option long_options[] = {
		{"help", no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{0,0,0,0}
	};

	while((c = getopt_long(argc, argv, "h:v", long_options, NULL)) > -1) {
		switch (c) {
		case 'h':
			show_help(argv[0]);
			exit(0);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
			break;
		default:
			show_help(argv[0]);
			exit(1);
			break;
		}
	}

	if(strstr(argv[1], "create") != NULL && argc < 5) {
		fprintf(stdout, "Too few arguments given for loop device creation. Needs: create <target directory> <size (i.e. 100MB)> <filesystem>\n");
		return 1;
	}
	else if(strstr(argv[1], "delete") != NULL && argc < 3) {
		fprintf(stdout, "Too few arguments given for loop device deletion. Needs: delete <target directory>\n");
		return 1;
	}
	else if(argc > 5) {
		printf("%d\n", argc);
		fprintf(stdout, "Too many arguments given.\n");
		return 1;
	}

	char *func = argv[1];
	char *loc = argv[2];
	char *fs = argv[4];
	int result;

	if(strstr(func, "create") != NULL) {

		int64_t size = (string_metric_parse(argv[3]) / 1024);
		result = disk_alloc_create(loc, fs, size);

		if(result != 0) {

			printf("Could not create allocation.\n");
			return 1;
		}

		printf("Allocation complete.\n");
		return 0;
	}

	else if(strstr(func, "delete") != NULL) {

		result = disk_alloc_delete(loc);

		if(result != 0) {

			printf("Could not delete allocation.\n");
			return 1;
		}

		printf("Deallocation complete.\n");
		return 0;
	}

	else {

		printf("Invalid parameters defined.\n");
		return 1;
	}
}
