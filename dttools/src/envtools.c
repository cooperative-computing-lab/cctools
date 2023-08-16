/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <ctype.h>
#include <errno.h>
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

	env_paths = xxstrdup(getenv(env_path_var));

	for(cur_path = strtok(env_paths, ":"); cur_path; cur_path = strtok(NULL, ":")) {
		snprintf(exe_path, max_length, "%s/%s", cur_path, exe_name);
		if(access(exe_path, R_OK | X_OK) == 0)
			goto fe_cleanup;
	}

	  fe_cleanup:
	free(env_paths);

	return cur_path != NULL;
}

int env_replace( const char *infile, const char *outfile ){
	FILE *INPUT = fopen(infile, "r");
	if (INPUT == NULL) {
		debug(D_ERROR, "unable to open %s: %s", infile, strerror(errno));
		return 1;
	}

	FILE *OUTPUT = fopen(outfile, "w");
	if (OUTPUT == NULL) {
		debug(D_ERROR, "unable to open %s: %s", outfile, strerror(errno));
		return 1;
	}
	
	char variable[1024];
	int var_index = 0;
	int valid_var = 0;

	char c = fgetc(INPUT);
	while (c != EOF)
	{
		if (c == '$') {
			valid_var = 1;
		} else if (valid_var && (isalpha(c) || (c == '_') || (var_index > 1 && isdigit(c)))) {
			variable[var_index] = c;
			var_index++;
		} else if (valid_var && var_index > 0) {
			variable[var_index] = '\0';
			const char *var = getenv(variable);
			if (var) {
				fprintf(OUTPUT, "%s", var);
			} else {
				debug(D_NOTICE, "failed to resolve %s environment variable, restoring string", variable);
			}
			valid_var = 0;
			var_index = 0;
		} else {
			if (valid_var) {
				fprintf(OUTPUT, "$");
			}
			valid_var = 0;
			var_index = 0;
		}

		if (!valid_var) {
			fprintf(OUTPUT, "%c", c);
		}
		c = fgetc(INPUT);
	}
	fclose(OUTPUT);
	fclose(INPUT);

	return 0;
}

const char *system_tmp_dir(const char *override_tmp_dir)
{
	const char *tmp_dir;
	if(override_tmp_dir) {
		return override_tmp_dir;
	}
	else if((tmp_dir = getenv("CCTOOLS_TEMP")) && access(tmp_dir, R_OK|W_OK|X_OK) == 0){
		return tmp_dir;
	}
	else if((tmp_dir = getenv("_CONDOR_SCRATCH_DIR")) && access(tmp_dir, R_OK|W_OK|X_OK) == 0){
		return tmp_dir;
	}
	else if((tmp_dir = getenv("TMPDIR")) && access(tmp_dir, R_OK|W_OK|X_OK) == 0){
		return tmp_dir;
	}
	else if((tmp_dir = getenv("TEMP")) && access(tmp_dir, R_OK|W_OK|X_OK) == 0){
		return tmp_dir;
	}
	else if((tmp_dir = getenv("TMP")) && access(tmp_dir, R_OK|W_OK|X_OK) == 0){
		return tmp_dir;
	}
	else {
		return "/tmp";
	}
}

/* vim: set noexpandtab tabstop=8: */
