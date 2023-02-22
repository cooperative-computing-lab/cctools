/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "taskvine.h"
#include "vine_manager.h"
#include "assert.h"
#include "create_dir.h"
#include "list.h"
#include "path.h"
#include "stringtools.h"
#include "unlink_recursive.h"
#include "xxmalloc.h"

static char *vine_runtime_info_path = "vine-run-info";

static struct list *known_staging_dirs = NULL;

void cleanup_staging_dirs() {
    if(!known_staging_dirs) {
        return;
    }

    char *path = NULL;
    LIST_ITERATE(known_staging_dirs, path) {
        if(!access(path, F_OK)) {
            unlink_recursive(path);
        }
    }

    list_free(known_staging_dirs);
    list_delete(known_staging_dirs);

    known_staging_dirs = NULL;
}


void register_staging_dir(const char *path) {
    if(!known_staging_dirs) {
        known_staging_dirs = list_create();
        atexit(cleanup_staging_dirs);
    }

    list_push_head(known_staging_dirs, xxstrdup(path));
}


char *vine_runtime_directory_create() {
    /* runtime directories are created at vine_runtime_info_path, which defaults
     * to "vine-run-info" of the current working directory.
     * Each workflow run has its own directory of the form: %Y-%m-%dT%H:%M:%S,
     * but this can be changed with VINE_RUNTIME_INFO_DIR.
     *
     * If VINE_RUNTIME_INFO_DIR is not an absolute path, then it is
     * interpreted as a suffix to vine_runtime_info_path.
     *
     * VINE_RUNTIME_INFO_DIR has the subdirectories: logs and staging
     */

	char *runtime_dir = NULL;
    int symlink_most_recent = 0;
	if(getenv("VINE_RUNTIME_INFO_DIR")) {
		runtime_dir = xxstrdup(getenv("VINE_RUNTIME_INFO_DIR"));
	} else {
		char buf[20];
		time_t now = time(NULL);
		struct tm *tm_info = localtime(&now);
		strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", tm_info);
		runtime_dir = xxstrdup(buf);

        symlink_most_recent = 1;
	}

	if(strncmp(runtime_dir, "/", 1)) {
        char *tmp = path_concat(vine_runtime_info_path, runtime_dir);
		free(runtime_dir);
		runtime_dir = tmp;
	}

	setenv("VINE_RUNTIME_INFO_DIR", runtime_dir, 1);
	if(!create_dir(runtime_dir, 0755)) {
        return NULL;
    }

	char pabs[PATH_MAX];
	path_absolute(runtime_dir, pabs, 0);
	free(runtime_dir);
	runtime_dir = xxstrdup(pabs);

	char *tmp = string_format("%s/vine-logs", runtime_dir);
	if(!create_dir(tmp, 0755)) {
        return NULL;
    }
	free(tmp);

	tmp = string_format("%s/staging", runtime_dir);
	if(!create_dir(tmp, 0755)) {
        return NULL;
    }
    register_staging_dir(tmp);
	free(tmp);

    if(symlink_most_recent) {
        char *tmp = path_concat(vine_runtime_info_path, "most-recent");
        unlink(tmp);
        symlink(runtime_dir, tmp);
        free(tmp);
    }

    return runtime_dir;
}

char *vine_get_runtime_path_log(struct vine_manager *m, const char *path) {
    return string_format("%s/vine-logs/%s", m->runtime_directory, path ? path : "");
}

char *vine_get_runtime_path_staging(struct vine_manager *m, const char *path) {
    return string_format("%s/staging/%s", m->runtime_directory, path ? path : "");
}

void vine_set_runtime_info_path(const char *path) {
    assert(path);
    vine_runtime_info_path = xxstrdup(path);
}

