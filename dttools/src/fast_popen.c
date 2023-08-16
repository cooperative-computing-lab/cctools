/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable.h"
#include "stringtools.h"
#include "debug.h"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <errno.h>

static struct itable *process_table = 0;

FILE *fast_popen(const char *command)
{
	pid_t pid;
	int argc;
	char **argv;
	int fds[2];
	char cmd[4096];
	int result;

	strcpy(cmd, command);

	if(string_split_quotes(cmd, &argc, &argv) < 1)
		return 0;

	if(argc < 1)
		return 0;

	result = pipe(fds);
	if(result < 0) {
		free(argv);
		return 0;
	}

	pid = fork();
	if(pid > 0) {
		free(argv);
		close(fds[1]);

		if(!process_table)
			process_table = itable_create(0);

		itable_insert(process_table, fds[0], (void *) (PTRINT_T) pid);
		return fdopen(fds[0], "r");

	} else if(pid == 0) {

		int i;

		close(0);
		dup2(fds[1], 1);
		dup2(fds[1], 2);
		close(fds[1]);
		close(fds[0]);
		for(i = 3; i < 10; i++)
			close(i);

		execv(argv[0], argv);
		_exit(1);

	} else {
		free(argv);
		return 0;
	}
}

int fast_pclose(FILE * file)
{
	pid_t pid;
	int result;
	int status;

	pid = (PTRINT_T) itable_remove(process_table, fileno(file));

	fclose(file);

	while(1) {
		result = waitpid(pid, &status, 0);
		if(result == pid) {
			return 0;
		} else {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		}
	}

	errno = ECHILD;
	return -1;
}

/* vim: set noexpandtab tabstop=8: */
