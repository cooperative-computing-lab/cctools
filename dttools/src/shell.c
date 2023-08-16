/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "catch.h"
#include "debug.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

static int execute (const char *cmd, const char * const env[], int in[2], int out[2], int err[2])
{
	int rc;
	int i;

	CATCHUNIX(close(in[1]));
	CATCHUNIX(close(out[0]));
	CATCHUNIX(close(err[0]));

	CATCHUNIX(dup2(in[0], STDIN_FILENO));
	CATCHUNIX(dup2(out[1], STDOUT_FILENO));
	CATCHUNIX(dup2(err[1], STDERR_FILENO));

	CATCHUNIX(close(in[0]));
	CATCHUNIX(close(out[1]));
	CATCHUNIX(close(err[1]));

	for(i = 0; env[i]; i++) {
		CATCHUNIX(putenv((char *)env[i]));
	}

	CATCHUNIX(execlp("/bin/sh", "sh", "-c", cmd, NULL));

	rc = 0;
	goto out;
out:
	fatal("shellcode execute failure: %s", strerror(errno));
	abort();
}

int shellcode(const char *cmd, const char * const env[], const char *input, size_t len, buffer_t *Bout, buffer_t *Berr, int *status)
{
	int rc;
	int in[2] = {-1, -1};
	int out[2] = {-1, -1};
	int err[2] = {-1, -1};
	pid_t child = 0;
	const char * const _env[] = {NULL};
	struct timeval start, stop;

	gettimeofday(&start, NULL);

	if (env == NULL)
		env = _env;

	CATCHUNIX(pipe(in));
	CATCHUNIX(pipe(out));
	CATCHUNIX(pipe(err));

	CATCHUNIX(child = fork());

	if(child == 0) {
		return execute(cmd, env, in, out, err);
	}

	CATCHUNIX(close(in[0]));
	in[0] = -1;
	CATCHUNIX(close(out[1]));
	out[1] = -1;
	CATCHUNIX(close(err[1]));
	err[1] = -1;

	CATCHUNIX(fcntl(in[1], F_GETFL));
	CATCHUNIX(fcntl(in[1], F_SETFL, rc|O_NONBLOCK));

	CATCHUNIX(fcntl(out[0], F_GETFL));
	CATCHUNIX(fcntl(out[0], F_SETFL, rc|O_NONBLOCK));

	CATCHUNIX(fcntl(err[0], F_GETFL));
	CATCHUNIX(fcntl(err[0], F_SETFL, rc|O_NONBLOCK));

	while (1) {
		char b[1<<16];
		pid_t w;
		ssize_t result;

		CATCHUNIX(w = waitpid(child, status, WNOHANG));

		if (len) {
			result = write(in[1], input, len);
			if (result == -1 && errno != EAGAIN && errno != EINTR) {
				CATCH(errno);
			} else if (result > 0) {
				input += result;
				len -= (size_t)result;
			}
		} else if (in[1] >= 0) {
			close(in[1]);
			in[1] = -1;
		}

		result = read(out[0], b, sizeof(b));
		if (result == -1 && errno != EAGAIN && errno != EINTR) {
			CATCH(errno);
		} else if (result > 0 && Bout) {
			buffer_putlstring(Bout, b, (size_t)result);
		}

		result = read(err[0], b, sizeof(b));
		if (result == -1 && errno != EAGAIN && errno != EINTR) {
			CATCH(errno);
		} else if (result > 0 && Berr) {
			buffer_putlstring(Berr, b, (size_t)result);
		}

		if (w == child)
			break;
	}
	child = 0;

	rc = 0;
	goto out;
out:
	if (child > 0) {
		kill(child, SIGKILL);
		waitpid(child, NULL, 0);
	}
	if (in[0] >= 0) close(in[0]);
	if (in[1] >= 0) close(in[1]);
	if (out[0] >= 0) close(out[0]);
	if (out[1] >= 0) close(out[1]);
	if (err[0] >= 0) close(err[0]);
	if (err[1] >= 0) close(err[1]);
	gettimeofday(&stop, NULL);
	debug(D_DEBUG, "shellcode finished in %.2fs", (double)(stop.tv_sec-start.tv_sec) + (stop.tv_usec-start.tv_usec)*1e-6);
	return RCUNIX(rc);
}

/* vim: set noexpandtab tabstop=8: */
