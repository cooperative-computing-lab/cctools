/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#if defined(CCTOOLS_OPSYS_DARWIN)

#include <unistd.h>
#include <stdlib.h>
#include <sys/sysctl.h>
#include <sys/types.h>

void load_average_get(double *avg)
{
	avg[0] = avg[1] = avg[2] = 0;
	getloadavg(avg, 3);
}

int load_average_get_cpus()
{
	int n;
	size_t size = sizeof(n);
	if(sysctlbyname("hw.physicalcpu", &n, &size, 0, 0) == 0) {
		return n;
	} else {
		return 1;
	}
}

#elif defined(CCTOOLS_OPSYS_LINUX)

#include <stdio.h>
#include <stdlib.h>

#include "stringtools.h"
#include "string_set.h"

void load_average_get(double *avg)
{
	FILE *f;
	avg[0] = avg[1] = avg[2] = 0;
	f = fopen("/proc/loadavg", "r");
	if(f) {
		fscanf(f, "%lf %lf %lf", &avg[0], &avg[1], &avg[2]);
		fclose(f);
	}
}

int load_average_get_cpus()
{
	struct string_set *cores;
	cores = string_set_create(0, 0);

	for (int i = 0; ; i++) {
		char *p = string_format("/sys/devices/system/cpu/cpu%u/topology/thread_siblings", i);
		FILE *f = fopen(p, "r");
		free(p);
		if (!f) break;

		char line[1024];
		int rc = fscanf(f, "%1023s", line);
		fclose(f);
		if (rc != 1) break;

		string_set_push(cores, line);
	}

	int cpus = string_set_size(cores);
	string_set_delete(cores);
	if (cpus < 1) {
		cpus = 1;
		fprintf(stderr, "Unable to detect CPUs, falling back to 1\n");
	}
	return cpus;
}

#else

void load_average_get(double *avg)
{
	avg[0] = avg[1] = avg[2] = 0;
}

int load_average_get_cpus()
{
	return 1;
}

#endif

/* vim: set noexpandtab tabstop=8: */
