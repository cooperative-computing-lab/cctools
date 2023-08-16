/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "timestamp.h"

#include <sys/time.h>
#include <sys/select.h>
#include <sys/stat.h>

time_t timestamp_file(const char *filename)
{
	struct stat buf;
	if(stat(filename, &buf) == 0) {
		return buf.st_mtime;
	} else {
		return 0;
	}
}

timestamp_t timestamp_get()
{
	struct timeval current;
	timestamp_t stamp;

	gettimeofday(&current, 0);
	stamp = ((timestamp_t) current.tv_sec) * 1000000 + current.tv_usec;

	return stamp;
}

int timestamp_fmt(char *buf, size_t size, const char *fmt, timestamp_t ts)
{
	time_t tv_sec;
	struct tm *tp;

	if(buf == NULL) return 0;

	tv_sec = ts / 1000000;

#if defined (_XOPEN_SOURCE) || defined (_BSD_SOURCE) || defined (_SVID_SOURCE) || defined (_POSIX_SOURCE) || _POSIX_C_SOURCE >= 1
	struct tm t;
	tp = localtime_r(&tv_sec, &t);
#else
	tp = localtime(&tv_sec);
#endif

	if(tp != NULL) {
		return strftime(buf, size, fmt, tp);
	}

	return 0;
}

void timestamp_sleep(timestamp_t interval)
{
	struct timeval t;

	t.tv_sec = interval / 1000000;
	t.tv_usec = interval % 1000000;

	select(0, 0, 0, 0, &t);
}

/* vim: set noexpandtab tabstop=8: */
