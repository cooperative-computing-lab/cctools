/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_matrix.h"

#include "auth_all.h"
#include "debug.h"
#include "timestamp.h"
#include "macros.h"
#include "random.h"

#include <time.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/wait.h>

int main(int argc, char *argv[])
{
	auth_register_byname("hostname");

	debug_config(argv[0]);
	random_init();

	if(argc != 7) {
		printf("use: %s <host> <path> <width> <height> <nhosts> <ops>\n", argv[0]);
		return -1;
	}

	int i;
	timestamp_t start, stop;
	const char *host = argv[1];
	const char *path = argv[2];
	int width = atoi(argv[3]);
	int height = atoi(argv[4]);
	int nhosts = atoi(argv[5]);
	int randlimit = atoi(argv[6]);
	time_t stoptime = time(0) + 3600;

	double *data = malloc(width * 8);

	struct chirp_matrix *matrix;

	matrix = chirp_matrix_open(host, path, stoptime);
	if(matrix) {
		if(chirp_matrix_width(matrix) == width && chirp_matrix_height(matrix) == height && chirp_matrix_nhosts(matrix) == nhosts) {
			/* ok, continue */
		} else {
			chirp_matrix_close(matrix, stoptime);
			chirp_matrix_delete(host, path, stoptime);
			matrix = 0;
		}
	}

	if(!matrix) {
		matrix = chirp_matrix_create(host, path, width, height, sizeof(double), nhosts, stoptime);
		if(!matrix) {
			printf("couldn't create matrix: %s\n", strerror(errno));
			return 1;
		}

		start = timestamp_get();
		for(i = 0; i < height; i++) {
			chirp_matrix_set_row(matrix, i, data, stoptime);
		}
		chirp_matrix_fsync(matrix, stoptime);
		stop = timestamp_get();
		printf("init      %8.0lf cells/sec\n", 1000000.0 * (height * width) / (stop - start));
		sleep(1);
	}

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_get_row(matrix, rand() % height, data, stoptime);
	}
	stop = timestamp_get();
	printf("rowread   %8.0lf cells/sec\n", 1000000.0 * (randlimit * width) / (stop - start));

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_set_row(matrix, rand() % height, data, stoptime);
	}
	chirp_matrix_fsync(matrix, stoptime);
	stop = timestamp_get();
	printf("rowwrite  %8.0lf cells/sec\n", 1000000.0 * (randlimit * width) / (stop - start));

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_get_col(matrix, rand() % width, data, stoptime);
	}
	stop = timestamp_get();
	printf("colread   %8.0lf cells/sec\n", 1000000.0 * (randlimit * height) / (stop - start));

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_set_col(matrix, rand() % width, data, stoptime);
	}
	chirp_matrix_fsync(matrix, stoptime);
	stop = timestamp_get();
	printf("colwrite  %8.0lf cells/sec\n", 1000000.0 * (randlimit * height) / (stop - start));

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_get(matrix, rand() % width, rand() % height, data, stoptime);
	}
	stop = timestamp_get();
	printf("cellread  %8.0lf cells/sec\n", 1000000.0 * randlimit / (stop - start));

	/*--------------------------------------------------------------------*/

	start = timestamp_get();
	for(i = 0; i < randlimit; i++) {
		chirp_matrix_set(matrix, rand() % width, rand() % height, data, stoptime);
	}
	chirp_matrix_fsync(matrix, stoptime);
	stop = timestamp_get();
	printf("cellwrite %8.0lf cells/sec\n", 1000000.0 * randlimit / (stop - start));

	/*-------------------------------------------------------------------*/


	return 0;
}

/* vim: set noexpandtab tabstop=8: */
