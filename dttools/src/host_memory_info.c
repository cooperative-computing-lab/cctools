/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "host_memory_info.h"

#if defined(CCTOOLS_OPSYS_LINUX)

#include <unistd.h>

int host_memory_info_get(uint64_t * avail, uint64_t * total)
{
	*total = getpagesize() * (uint64_t) sysconf(_SC_PHYS_PAGES);
	*avail = getpagesize() * (uint64_t) sysconf(_SC_AVPHYS_PAGES);
	return 1;
}

#elif defined(CCTOOLS_OPSYS_DARWIN)

#include <sys/types.h>
#include <sys/sysctl.h>

int host_memory_info_get(uint64_t * avail, uint64_t * total)
{
	unsigned x = 0;
	size_t s = sizeof(x);
	sysctlbyname("hw.physmem", &x, &s, 0, 0);
	*avail = *total = x;
	return 1;
}

#else

int host_memory_info_get(uint64_t * avail, uint64_t * total)
{
	*total = 0;
	*avail = 0;
	return 1;
}

#endif

#include <sys/resource.h>
#include <stdio.h>
#include <unistd.h>

int host_memory_usage_get(uint64_t * rssp, uint64_t * totalp)
{
#ifdef CCTOOLS_OPSYS_LINUX
	/*
	   Linux has getrusage, but it doesn't remote memory status,
	   so we must load it from the proc filesystem instead.
	 */

	unsigned long total, rss, shared, text, libs, data, dirty;

	FILE *file = fopen("/proc/self/statm", "r");
	if(!file)
		return 0;

	fscanf(file, "%lu %lu %lu %lu %lu %lu %lu", &total, &rss, &shared, &text, &libs, &data, &dirty);

	fclose(file);

	*rssp = (uint64_t) rss *getpagesize();
	*totalp = (uint64_t) total *getpagesize();

	return 1;
#else
	struct rusage ru;
	int result = getrusage(RUSAGE_SELF, &ru);
	if(result >= 0) {
		*rssp = (uint64_t) ru.ru_ixrss * getpagesize();
		*totalp = (uint64_t) ru.ru_ixrss * getpagesize();
		return 1;
	} else {
		return 0;
	}
#endif
}

/* vim: set noexpandtab tabstop=4: */
