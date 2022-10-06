/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_time.h"

#include <string.h>
#include <errno.h>

pfs_time_mode_t pfs_time_mode = PFS_TIME_MODE_NORMAL;

/*
For time stop and time warp modes, this is the emulated
time, which begins at midnight, Monday, January 1st, 2001 UTC
In time-warp mode, this is incremented by .01s at every request.
*/

static struct timespec emulated_time = { 978307200,0 };

time_t pfs_emulate_time( time_t *t )
{
	struct timespec ts;
	pfs_emulate_clock_gettime(CLOCK_REALTIME,&ts);

	if(t) *t = ts.tv_sec;

	return ts.tv_sec;
}

int pfs_emulate_gettimeofday( struct timeval *tv, struct timezone *tz )
{
	struct timespec ts;
	pfs_emulate_clock_gettime(CLOCK_REALTIME,&ts);

	if(tv) {
		tv->tv_sec = ts.tv_sec;
		tv->tv_usec = ts.tv_nsec/1000;
	}

	if(tz) {
		memset(tz,0,sizeof(*tz));
	}

	return 0;
}

int pfs_emulate_clock_gettime( clockid_t clockid, struct timespec *ts )
{
       	switch(pfs_time_mode) {
		case PFS_TIME_MODE_NORMAL:
			return clock_gettime(clockid,ts);
			break;
		case PFS_TIME_MODE_STOP:
			*ts = emulated_time;
			return 0;
			break;
		case PFS_TIME_MODE_WARP:
			*ts = emulated_time;
			emulated_time.tv_nsec += 10000000;
			if(emulated_time.tv_nsec >= 1000000000) {
				emulated_time.tv_nsec -= 1000000000;
				emulated_time.tv_sec++;
			}
			return 0;
			break;
	}

	errno = ENOSYS;
	return -1;
}


