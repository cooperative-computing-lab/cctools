/*
 * Copyright (C) 2022 The University of Notre Dame
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
		fatal("could not fork: %s", strerror(errno));
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	pid_t group = setsid();
	if (group == (pid_t) -1) {
		fatal("could not create session: %s", strerror(errno));
	}

	/* Second fork ensures process cannot acquire controlling terminal */
	pid = fork();
	if (pid < 0) {
		fatal("could not fork: %s", strerror(errno));
	} else if (pid > 0) {
		exit(EXIT_SUCCESS); /* exit parent */
	}

	if (pidfile && strlen(pidfile)) {
		FILE *file = fopen(pidfile, "w");
		if (file) {
			fprintf(file, "%ld", (long)getpid());
			fclose(file);
		} else {
			fatal("could not open `%s' for writing: %s", pidfile, strerror(errno));
		}
	}

	if (cdroot){
		int status = chdir("/");
		if (status == -1) {
			fatal("could not chdir to `/': %s", strerror(errno));
		}
	}

	umask(0);

	fd_nonstd_close();

	FILE *file0 = freopen("/dev/null", "r", stdin);
	if (file0 == NULL) {
		fatal("could not reopen stdin: %s", strerror(errno));
	}
	FILE *file1 = freopen("/dev/null", "w", stdout);
	if (file1 == NULL) {
		fatal("could not reopen stdout: %s", strerror(errno));
	}
	FILE *file2 = freopen("/dev/null", "w", stderr);
	if (file2 == NULL) {
		fatal("could not reopen stderr: %s", strerror(errno));
	}

	debug_reopen();
}

/* vim: set noexpandtab tabstop=8: */
