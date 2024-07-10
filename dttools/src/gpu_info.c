/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_info.h"
#include "get_line.h"
#include "stringtools.h"
#include "debug.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define GPU_EXECUTABLE "/bin/nvidia-smi"
#define GPU_COUNT_COMMAND GPU_EXECUTABLE " --query-gpu=count --format=csv,noheader"
#define GPU_NAME_COMMAND GPU_EXECUTABLE " --query-gpu=name --format=csv,noheader"

int gpu_count_get()
{
	if (access(GPU_EXECUTABLE, X_OK) != 0)
		return 0;

	debug(D_DEBUG, "gpu_count_get: running \"%s\"\n", GPU_COUNT_COMMAND);

	FILE *pipe = popen(GPU_COUNT_COMMAND, "r");
	if (!pipe)
		return 0;

	int gpus;
	int fields = fscanf(pipe, "%d", &gpus);
	int status = pclose(pipe);

	/*
	An error in GPU detection will be indicated by
	non-zero exit status accompanied by some unpredictable output,
	so we must check the exit status before declaring success.
	*/

	if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
		if (fields == 1) {
			return gpus;
		} else {
			return 0;
		}
	} else {
		debug(D_DEBUG, "gpu_count_get: failed with status %d", WEXITSTATUS(status));
		return 0;
	}
}

char *gpu_name_get()
{
	if (access(GPU_EXECUTABLE, X_OK) != 0)
		return 0;

	debug(D_DEBUG, "gpu_name_get: running \"%s\"\n", GPU_NAME_COMMAND);

	FILE *pipe = popen(GPU_NAME_COMMAND, "r");
	if (!pipe)
		return 0;

	char *gpu_name = get_line(pipe);

	string_chomp(gpu_name);

	int status = pclose(pipe);

	/*
	An error in GPU detection will be indicated by
	non-zero exit status accompanied by some unpredictable output,
	so we must check the exit status before declaring success.
	*/

	if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
		return gpu_name;
	} else {
		debug(D_DEBUG, "gpu_name_get: failed with status %d", WEXITSTATUS(status));
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
