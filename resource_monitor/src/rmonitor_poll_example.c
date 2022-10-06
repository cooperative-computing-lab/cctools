/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "rmonitor_poll.h"
#include "rmsummary.h"

int main(int argc, char **argv) {
	sleep(2);

	struct rmsummary *resources = rmonitor_measure_process(getpid());

	fprintf(stdout, "command: %s, ",
			resources->command);

	fprintf(stdout, "wall time used (s): %3.0f, ", resources->wall_time);

	fprintf(stdout, "total memory used (MB): %f, ", resources->memory + resources->swap_memory);

	fprintf(stdout, "total cores used: %f\n", resources->cores);

	fprintf(stdout, "\n\njson output:\n");
	rmsummary_print(stdout, resources, /* pprint */ 1, /* extra fields */ 0);

	rmsummary_delete(resources);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
