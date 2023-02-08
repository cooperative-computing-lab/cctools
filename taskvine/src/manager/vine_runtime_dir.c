/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>

#include "taskvine.h"
#include "vine_manager.h"
#include "assert.h"
#include "create_dir.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"

static char *vine_runtime_info_path = "vine-runtime";


char *vine_runtime_directory_create() {
    /* runtime directories are created at vine_runtime_info_path, which defaults
     * to "vine-runtime" of the current working directory.
     * Each workflow run has its own directory of the form: %Y-%m-%dT%H:%M:%S,
     * but this can be changed with VINE_RUNTIME_INFO_DIR.
     *
     * If VINE_RUNTIME_INFO_DIR is not an absolute path, then it is
     * interpreted as a suffix to vine_runtime_info_path.
     *
     * VINE_RUNTIME_INFO_DIR has the subdirectories: logs and staging
     */

	char *runtime_dir = NULL;
	if(getenv("VINE_RUNTIME_INFO_DIR")) {
		runtime_dir = xxstrdup(getenv("VINE_RUNTIME_INFO_DIR"));
	} else {
		char buf[20];
		time_t now = time(NULL);
		struct tm *tm_info = localtime(&now);
		strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", tm_info);
		runtime_dir = xxstrdup(buf);
	}

	if(strncmp(runtime_dir, "/", 1)) {
        char *tmp = path_concat(vine_runtime_info_path, runtime_dir);
		free(runtime_dir);
		runtime_dir = tmp;
	}

	setenv("VINE_RUNTIME_INFO_DIR", runtime_dir, 1);
	if(!create_dir(runtime_dir, 755)) {
        return NULL;
    }

	char pabs[PATH_MAX];
	path_absolute(runtime_dir, pabs, 0);
	free(runtime_dir);
	runtime_dir = xxstrdup(pabs);

	char *tmp = string_format("%s/logs", runtime_dir);
	if(!create_dir(tmp, 755)) {
        return NULL;
    }
	free(tmp);

	tmp = string_format("%s/staging", runtime_dir);
	if(!create_dir(tmp, 755)) {
        return NULL;
    }
	free(tmp);

    return runtime_dir;
}

char *vine_get_runtime_path_log(struct vine_manager *m, const char *path) {
    return string_format("%s/logs/%s", m->runtime_directory, path ? path : "");
}

char *vine_get_path_runtime_staging(struct vine_manager *m, const char *path) {
    return string_format("%s/staging/%s", m->runtime_directory, path ? path : "");
}

void vine_set_runtime_info_path(const char *path) {
    assert(path);
    vine_runtime_info_path = xxstrdup(path);
}
