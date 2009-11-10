/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "memory_info.h"

#if defined(CCTOOLS_OPSYS_LINUX) || defined(CCTOOLS_OPSYS_SUNOS)

#include <unistd.h>

int memory_info_get( UINT64_T *avail, UINT64_T *total )
{
	*total = getpagesize() * (UINT64_T) sysconf(_SC_PHYS_PAGES);
	*avail = getpagesize() * (UINT64_T) sysconf(_SC_AVPHYS_PAGES);
	return 1;
}

#elif defined(CCTOOLS_OPSYS_DARWIN)

#include <sys/types.h>
#include <sys/sysctl.h>

int memory_info_get( UINT64_T *avail, UINT64_T *total )
{
	unsigned x=0;
	size_t s = sizeof(x);
	sysctlbyname("hw.physmem",&x,&s,0,0);
	*avail = *total = x;
	return 1;
}

#else

int memory_info_get( UINT64_T *avail, UINT64_T *total )
{
	*total = 0;
	*avail = 0;
	return 1;
}

#endif
