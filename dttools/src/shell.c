/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "catch.h"
#include "debug.h"
#include "full_io.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

static int execute (const char *cmd, const char * const env[], int out[2], int err[2])
{
	int rc;
	int i;

	CATCHUNIX(close(out[0]));
	CATCHUNIX(close(err[0]));

	CATCHUNIX(dup2(out[1], STDOUT_FILENO));
	CATCHUNIX(dup2(err[1], STDERR_FILENO));

	CATCHUNIX(close(out[1]));
	CATCHUNIX(close(err[1]));

	for(i = 0; env[i]; i++) {
		CATCHUNIX(putenv((char *)env[i]));
	}

	CATCHUNIX(execlp("sh", "sh", "-c", cmd, NULL));

	rc = 0;
	goto out;
out:
	fatal("shellcode execute failure: %s", strerror(errno));
	abort();
}

int shellcode(const char *cmd, const char * const env[], buffer_t * Bout, buffer_t * Berr, int *status)
{
	int rc;
	int out[2] = {-1, -1};
	int err[2] = {-1, -1};
	pid_t child = 0;
	const char * const _env[] = {NULL};
	struct timeval start, stop;

	gettimeofday(&start, NULL);

	if (env == NULL)
		env = _env;

	CATCHUNIX(pipe(out));
	CATCHUNIX(pipe(err));

	CATCHUNIX(child = fork());

	if(child == 0) {
		return execute(cmd, env, out, err);
	}

	CATCHUNIX(fcntl(out[0], F_GETFL));
	rc |= O_NONBLOCK;
	CATCHUNIX(fcntl(out[0], F_SETFL, rc));

	CATCHUNIX(fcntl(err[0], F_GETFL));
	rc |= O_NONBLOCK;
	CATCHUNIX(fcntl(err[0], F_SETFL, rc));

	while (1) {
		char b[1<<16];
		pid_t w;
		ssize_t result;

		CATCHUNIX(w = waitpid(child, status, WNOHANG));

		result = full_read(out[0], b, sizeof(b));
		if (result == -1 && errno != EAGAIN) {
			CATCH(errno);
		} else if (result > 0 && Bout) {
			buffer_putlstring(Bout, b, (size_t)result);
		}

		result = full_read(err[0], b, sizeof(b));
		if (result == -1 && errno != EAGAIN) {
			CATCH(errno);
		} else if (result > 0 && Berr) {
			buffer_putlstring(Berr, b, (size_t)result);
		}

		if (w == child)
			break;
	}

	rc = 0;
	goto out;
out:
	if (child) {
		kill(child, SIGKILL);
		waitpid(child, NULL, 0);
	}
	close(out[0]);
	close(out[1]);
	close(err[0]);
	close(err[1]);
	gettimeofday(&stop, NULL);
	debug(D_DEBUG, "shellcode finished in %.2fs", (double)(stop.tv_sec-start.tv_sec) + (stop.tv_usec-start.tv_usec)*1e-6);
	return RCUNIX(rc);
}

/* vim: set noexpandtab tabstop=4: */
