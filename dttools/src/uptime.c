/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "uptime.h"
#include "debug.h"

#if defined(CCTOOLS_OPSYS_DARWIN)
#include <sys/sysctl.h>
#include <time.h>
#elif defined(CCTOOLS_OPSYS_LINUX)
#include <sys/sysinfo.h>
#endif

int uptime_get()
{
	int uptime;

#if defined(CCTOOLS_OPSYS_DARWIN)
	struct timeval boottime;
	size_t len = sizeof(boottime);
	int mib[2] = { CTL_KERN, KERN_BOOTTIME };
	if(sysctl(mib, 2, &boottime, &len, NULL, 0) < 0) {
		uptime = -1;
	}
	time_t bsec = boottime.tv_sec;
	time_t csec = time(NULL);

	uptime = difftime(csec, bsec);
#elif defined(CCTOOLS_OPSYS_LINUX)
	struct sysinfo info;
	sysinfo(&info);
	uptime = info.uptime;
#else
	/*
	   Note that this is implemented as a text warning, since
	   system uptime detection is only used a few limited
	   cases and then only as a debugging tool.
	 */
	static int did_warning = 0;
	if(!did_warning) {
		debug(D_NOTICE, "uptime not implemented (yet) on this operating system");
		did_warning = 1;
	}
	uptime = 0;
#endif

	return uptime;
}

/* vim: set noexpandtab tabstop=8: */
