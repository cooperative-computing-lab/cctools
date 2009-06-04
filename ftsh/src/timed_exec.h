/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef TIMED_EXEC_H
#define TIMED_EXEC_H

#include <time.h>
#include <sys/types.h>

typedef enum {
	TIMED_EXEC_SUCCESS,
	TIMED_EXEC_FAILURE,
	TIMED_EXEC_TIMEOUT,
	TIMED_EXEC_NOEXEC
} timed_exec_t;

timed_exec_t timed_exec( int line, const char *path, char **argv, int fds[3], pid_t *pid, int *status, time_t stoptime );

#endif
