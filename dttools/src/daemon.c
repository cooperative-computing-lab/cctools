/*
 * Copyright (C) 2011- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "daemon.h"

#include "debug.h"
#include "fd.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void daemonize (int cdroot, const char *pidfile)
{
	/* Become session leader and lose controlling terminal */
	pid_t pid = fork();
	if (pid < 0) {
		debug(D_DEBUG, "could not fork: %s", strerror(errno));
		exit(EXIT_FAILURE);
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	pid_t group = setsid();
	if (group == (pid_t) -1) {
		debug(D_DEBUG, "could not create session: %s", strerror(errno));
		exit(EXIT_FAILURE);
	}

	/* Second fork ensures process cannot acquire controlling terminal */
	pid = fork();
	if (pid < 0) {
		debug(D_DEBUG, "could not fork: %s", strerror(errno));
		exit(EXIT_FAILURE);
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	if (pidfile) {
		FILE *file = fopen(pidfile, "w");
		fprintf(file, "%ld", (long)getpid());
		fclose(file);
	}

	if (cdroot){
		int status = chdir("/");
		if (status == -1) {
			debug(D_DEBUG, "could not chdir to `/': %s", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	umask(0);

    fd_nonstd_close();

    FILE *file0 = freopen("/dev/null", "r", stdin);
    if (file0 == NULL) {
        debug(D_DEBUG, "could not reopen stdin: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    FILE *file1 = freopen("/dev/null", "w", stdout);
    if (file1 == NULL) {
        debug(D_DEBUG, "could not reopen stdout: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    FILE *file2 = freopen("/dev/null", "w", stderr);
    if (file2 == NULL) {
        debug(D_DEBUG, "could not reopen stderr: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
}

/* vim: set noexpandtab tabstop=4: */
