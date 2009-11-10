/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "timestamp.h"

#include <sys/time.h>
#include <sys/select.h>
#include <sys/stat.h>

time_t timestamp_file( const char *filename )
{
	struct stat buf;
	if(stat(filename,&buf)==0) {
		return buf.st_mtime;
	} else {
		return 0;
	}
}

timestamp_t timestamp_get()
{
	struct timeval current;
	timestamp_t stamp;

	gettimeofday(&current,0);
	stamp = ((timestamp_t)current.tv_sec)*1000000 + current.tv_usec;

	return stamp;
}

void timestamp_sleep( timestamp_t interval )
{
	struct timeval t;

	t.tv_sec = interval / 1000000;
	t.tv_usec = interval % 1000000;

	select(0,0,0,0,&t);
}
