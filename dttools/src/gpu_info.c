/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_info.h"
#include "stringtools.h"
#include "get_line.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>



#define GPU_AUTODETECT "cctools_gpu_autodetect"

int gpu_info_get()
{
	int pipefd[2];
	pipe(pipefd);

	pid_t pid = fork();

	if(pid<0) {
		return 0;
	} else if(pid==0) {
		close(pipefd[0]);
		dup2(pipefd[1], fileno(stdout));
		char *args[] = {GPU_AUTODETECT, NULL};
		if(!access(GPU_AUTODETECT, R_OK|X_OK)){
			execv(GPU_AUTODETECT, args);
		} else {
			execvp(GPU_AUTODETECT, args);
		}
		_exit(0);
	} else {
		close(pipefd[1]);
		int status = 0;
		int gpu_count = 0;
		char buffer[10]; /* Enough characters to hold a decimal representation of a 32 bit int. */
		if(read(pipefd[0], buffer, 10)){
			waitpid(pid, &status, 0);
			gpu_count = atoi(buffer);
		}

		close(pipefd[0]);
		return gpu_count;
	}
}

char *gpu_name_get()
{
	char *nvidia_cmd = "/bin/nvidia-smi";

	if(access(nvidia_cmd, X_OK) != 0) {
		return NULL;
	}

	FILE *pipe = popen("/bin/nvidia-smi --query-gpu=gpu_name --format=csv,noheader", "r");
	if(!pipe) {
		return NULL;
	}

	char *gpu_name = get_line(pipe);
	string_chomp(gpu_name);

	pclose(pipe);

	return gpu_name;
}

/* vim: set noexpandtab tabstop=4: */
