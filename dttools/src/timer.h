/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TIMER_H
#define TIMER_H

/** @file timer.h Simple timer library.*/

void timer_init(int, const char *[]);
void timer_destroy();
void timer_start(int);
void timer_stop(int);
void timer_reset(int);
double timer_elapsed_time(int);
double timer_average_time(int);
void timer_print_summary(int);

#endif
