/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "sleeptools.h"
#include <unistd.h>
#include <sys/time.h>

void sleep_until(time_t stoptime)
{
	struct timeval tv;

	while(1) {
		time_t current = time(0);
		if(current >= stoptime)
			break;
		tv.tv_sec = stoptime - current;
		tv.tv_usec = 0;
		select(0, 0, 0, 0, &tv);
	}
}

void sleep_for(time_t interval)
{
	sleep_until(time(0) + interval);
}

/* vim: set noexpandtab tabstop=8: */
