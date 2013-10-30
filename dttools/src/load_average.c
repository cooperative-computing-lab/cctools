/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#if defined(CCTOOLS_OPSYS_SUNOS)

#include <sys/loadavg.h>
#include <unistd.h>

void load_average_get(double *avg)
{
	avg[0] = avg[1] = avg[2] = 0;
	getloadavg(avg, 3);
}

int load_average_get_cpus()
{
	long n = sysconf(_SC_NPROCESSORS_ONLN);
	if(n >= 1) {
		return n;
	} else {
		return 1;
	}
}

#elif defined(CCTOOLS_OPSYS_DARWIN)

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
	if(sysctlbyname("hw.ncpu", &n, &size, 0, 0) == 0) {
		return n;
	} else {
		return 1;
	}
}

#elif defined(CCTOOLS_OPSYS_LINUX)

#include <stdio.h>

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
	FILE *f;

	f = fopen("/proc/cpuinfo", "r");
	if(f) {
		char line[1024];
		int ncpus = 0;
		while(fgets(line, sizeof(line), f)) {
			sscanf(line, "processor : %d", &ncpus);
		}
		fclose(f);
		return ncpus + 1;
	} else {
		return 1;
	}
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

/* vim: set noexpandtab tabstop=4: */
