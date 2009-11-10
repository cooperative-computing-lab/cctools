/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SLEEPTOOLS_H
#define SLEEPTOOLS_H

#include <time.h>

/** @file sleeptools.h 
Sleep for a specified amount of time.
Note that the standard Unix sleep() may be interrupted for a number of reasons.
These routines will retry sleep() until the desired condition is reached.
*/

/** Sleep until a specific time.
@param stoptime The absolute time to wait for.
*/
void sleep_until( time_t stoptime );

/** Sleep for a specific interval of time.
@param interval The number of seconds to wait.
*/
void sleep_for( time_t interval );

#endif
