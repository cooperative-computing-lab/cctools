/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "xmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int find_executable(const char *exe_name, const char *env_path_var, char *exe_path, int max_length)
{
	char *env_paths;
	char *cur_path;

	if(access(exe_name, R_OK | X_OK) == 0) {
		snprintf(exe_path, max_length, "%s", exe_name);
		return 1;
	}

	if(!getenv(env_path_var))
		return 0;

	env_paths = xstrdup(getenv(env_path_var));

	for(cur_path = strtok(env_paths, ":"); cur_path; cur_path = strtok(NULL, ":")) {
		snprintf(exe_path, max_length, "%s/%s", cur_path, exe_name);
		if(access(exe_path, R_OK | X_OK) == 0)
			goto fe_cleanup;
	}

      fe_cleanup:
	free(env_paths);

	return cur_path != NULL;
}
