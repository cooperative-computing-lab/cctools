/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "timer.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/time.h>

int NumberOfTimers = 0;
const char **TimerStrings = NULL;

struct timeval *StartTime = NULL;
struct timeval *EndTime = NULL;

double *ElapsedTime = NULL;
int *TimedRuns = NULL;

void timer_init(int timers, const char *timer_strings[])
{
	StartTime = malloc(sizeof(struct timeval) * timers);
	EndTime = malloc(sizeof(struct timeval) * timers);

	ElapsedTime = malloc(sizeof(double) * timers);
	TimedRuns = malloc(sizeof(int) * timers);

	memset(ElapsedTime, 0, sizeof(double) * timers);
	memset(TimedRuns, 0, sizeof(int) * timers);

	NumberOfTimers = timers;
	TimerStrings = timer_strings;
}

#define FREE_AND_NULL(s) if (s) { free (s); (s) = NULL; }

void timer_destroy()
{
	FREE_AND_NULL(StartTime);
	FREE_AND_NULL(EndTime);
	FREE_AND_NULL(ElapsedTime);
	FREE_AND_NULL(TimedRuns);
}

void timer_start(int i)
{
	gettimeofday(&StartTime[i], NULL);
}

void timer_stop(int i)
{
	gettimeofday(&EndTime[i], NULL);

	ElapsedTime[i] += (double) (EndTime[i].tv_sec - StartTime[i].tv_sec) + (double) (EndTime[i].tv_usec - StartTime[i].tv_usec) / 1000000.0;
	TimedRuns[i]++;
}

void timer_reset(int i)
{
	ElapsedTime[i] = 0.0;
	TimedRuns[i] = 0;
}

double timer_elapsed_time(int i)
{
	return (ElapsedTime[i]);
}

double timer_average_time(int i)
{
	return (ElapsedTime[i] / TimedRuns[i]);
}

void timer_print_summary(int print_all)
{
	int i;
	for(i = 0; i < NumberOfTimers; i++)
		if(TimedRuns[i] > 0 || print_all)
			printf("%s = average(%2.6lf), total(%2.6lf), runs(%d)\n", TimerStrings[i], timer_average_time(i), ElapsedTime[i], TimedRuns[i]);
}

/* vim: set noexpandtab tabstop=8: */
