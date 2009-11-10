/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TIMESTAMP_H
#define TIMESTAMP_H

#include "int_sizes.h"
#include <time.h>

typedef UINT64_T timestamp_t;
#define TIMESTAMP_FORMAT UINT64_FORMAT

time_t timestamp_file( const char *file );
timestamp_t timestamp_get();
void timestamp_sleep( timestamp_t interval );

#endif

