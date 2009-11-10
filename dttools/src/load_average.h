/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LOAD_AVERAGE_H
#define LOAD_AVERAGE_H

/** @file load_average.h Get the current load and number of CPUs. */

/** Get the current load average.
Uses different techniques on each platform to get the current 1, 5, and 15 minute load average.
@param avg A pointer to an array of three doubles.  avg[0] gets the one minute load average, avg[1] gets the five minute load average, and avg[2] gets the 15 minute load average.
*/

void load_average_get( double *avg );

/** Get the number of CPU cores.
This function returns the number of CPU cores that the operating system assumes are actually available for use.  Due to hyperthreading, BIOS settings, operating system configuration, and so forth, this may not actually be the physical number of CPU cores.  A more accurate description might be the number of programs that the operating system can efficiently run at once.
@return The number of CPU cores.
*/

int  load_average_get_cpus();

#endif
