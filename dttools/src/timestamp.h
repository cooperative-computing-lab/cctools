/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TIMESTAMP_H
#define TIMESTAMP_H

/** @file timestamp.h
Portable routines for high resolution timing.
*/

#include "int_sizes.h"
#include <time.h>

/** A type to hold the current time, in microseconds since January 1st, 1970. */
typedef UINT64_T timestamp_t;

#define TIMESTAMP_FORMAT UINT64_FORMAT

/** Get the current time.
@return The current time, in microseconds since January 1st, 1970.
*/

timestamp_t timestamp_get();

/** Sleep for a specified time.
@param interval The number of microseconds to sleep for.
*/

void timestamp_sleep( timestamp_t interval );

/** Get the last modified time of a file.
@param file The path of the file to examine.
@return The modification time, in seconds since January 1st, 1970.
*/

time_t timestamp_file( const char *file );

#endif

