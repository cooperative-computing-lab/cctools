/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef MULTI_FORK_H
#define MULTI_FORK_H

#include <time.h>
#include <unistd.h>
#include <sys/types.h>

struct multi_fork_status {
	pid_t pid;
	int status;
	enum {
		MULTI_FORK_STATE_CRADLE,
		MULTI_FORK_STATE_RUNNING,
		MULTI_FORK_STATE_GRAVE
	} state;
};

int multi_fork( int n, struct multi_fork_status *s, time_t stoptime, int line );

#define MULTI_FORK_SUCCESS -1
#define MULTI_FORK_FAILURE -2
#define MULTI_FORK_TIMEOUT -3

extern int multi_fork_kill_timeout;
extern int multi_fork_kill_mode;

#define MULTI_FORK_KILL_MODE_WEAK 0
#define MULTI_FORK_KILL_MODE_STRONG 1

#endif
