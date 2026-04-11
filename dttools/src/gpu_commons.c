/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_commons.h"
#include "debug.h"


const char * LIBRARY_SEARCH_COMMAND = "ldconfig -p | grep %s | awk '{print $NF}'";

struct library_search_result find_library_by_name(const char *lib_name) {
    char command[256];
    char result[512];
    struct library_search_result lib_result = {{0}, false};

    snprintf(command, sizeof(command), LIBRARY_SEARCH_COMMAND, lib_name);

    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        debug(D_ERROR,"popen failed to run ldconfig command");
        return lib_result;
    }

    if (fgets(result, sizeof(result), fp) != NULL) {
        result[strcspn(result, "\n")] = 0;
        strncpy(lib_result.path, result, sizeof(lib_result.path) - 1);
        lib_result.path[sizeof(lib_result.path) - 1] = '\0';
        lib_result.found = true;
        debug(D_DEBUG, "GPU Library found at path %s\n", lib_result.path);
    } else {
        debug(D_DEBUG,"GPU Library %s not found in system search paths.\n", lib_name);
    }

    pclose(fp);
    return lib_result;
}
