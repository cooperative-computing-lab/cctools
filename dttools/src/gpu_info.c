/*
Copyright (C) 2022 The University of Notre Dame
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

#define GPU_EXECUTABLE "/bin/nvidia-smi"
#define GPU_COUNT_COMMAND GPU_EXECUTABLE " --query-gpu=count --format=csv,noheader"
#define GPU_NAME_COMMAND GPU_EXECUTABLE " --query-gpu=name --format=csv,noheader"

int gpu_count_get()
{
	if(access(GPU_EXECUTABLE, X_OK) != 0) return 0;
	
	FILE *pipe = popen(GPU_COUNT_COMMAND,"r");
	if (!pipe) return 0;
	
	int gpus;
	int fields = fscanf(pipe, "%d", &gpus);	
	pclose(pipe);	

	if(fields==1) {
		return gpus;
	} else {
		return 0;
	}
}

char *gpu_name_get()
{
	if(access(GPU_EXECUTABLE, X_OK) != 0) return 0;

	FILE *pipe = popen(GPU_NAME_COMMAND,"r");
	if(!pipe) return 0;

	char *gpu_name = get_line(pipe);

	string_chomp(gpu_name);

	pclose(pipe);

	return gpu_name;
}

/* vim: set noexpandtab tabstop=8: */
