/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_info.h"

#include <stddef.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

int gpu_info_get()
{
	pid_t pid;
	char *args[] = {"gpu_autodetect", NULL};
	int status = 0;

	switch (pid = fork() ) {
	case -1:
		return 0;
	case 0:
		execvp("gpu_autodetect", args);
		return 0;
	default:
		waitpid(pid, &status, 0);
		if(WIFEXITED(status)) return WEXITSTATUS(status);
		return 0;
	}
}

/* vim: set noexpandtab tabstop=4: */
