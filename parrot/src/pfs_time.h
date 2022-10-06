/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_TIME_H
#define PFS_TIME_H

#include <time.h>
#include <sys/time.h>

typedef enum {
	PFS_TIME_MODE_NORMAL,
	PFS_TIME_MODE_STOP,
	PFS_TIME_MODE_WARP
} pfs_time_mode_t;

extern pfs_time_mode_t pfs_time_mode;

time_t pfs_emulate_time( time_t *t );
int    pfs_emulate_gettimeofday( struct timeval *tv, struct timezone *tz );
int    pfs_emulate_clock_gettime( clockid_t clockid, struct timespec *ts );

#endif
