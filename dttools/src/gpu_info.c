/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_info.h"
#include "stringtools.h"
#include "get_line.h"

#include <string.h>
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
	char* nvidia_cmd= "/bin/nvidia-smi -q";
	if(access(nvidia_cmd, X_OK) != 0){	
			return 0;
	}
	
	FILE *pipe = popen(nvidia_cmd, "r");

	if(!pipe){
		return 0;
	}
	
	char buffer[BUFSIZ];
	char *target;
	while(fgets(buffer, BUFSIZ, pipe)){
		if(strstr(buffer, "Attached GPUs")){
			target = strtok(buffer, " :");
			target = strtok(NULL, " :");
			target = strtok(NULL, " :");
			int gpus = atoi(target);
			return gpus;
		}
	}
	
	return 0;

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
	fclose(pipe);

	return gpu_name;
}

/* vim: set noexpandtab tabstop=4: */
