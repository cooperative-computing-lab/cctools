/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu_commons.h"
#include "debug.h"
#include <string.h>

// nvidia libray name as macro
const char *NVIDIA_LIBRARY_SEARCH_COMMAND = "ldconfig -p | grep libnvidia-ml.so | awk '{print $NF}'";
// AMD library need to check for now we are only intrested in Nvidia
// the below library is not verified but referenced for future use case
const char *AMD_LIBRARY_SEARCH_COMMAND = "ldconfig -p | grep libhsa-runtime64.so | awk '{print $NF}'";

struct library_search_result find_library_by_name(enum gpu_vendor vendor)
{
	char command[256];
	char result[512];
	struct library_search_result lib_result = {{0}, false};
	if (vendor == NVIDIA) {
		strcpy(command, NVIDIA_LIBRARY_SEARCH_COMMAND);
	} else if (vendor == AMD) {
		strcpy(command, AMD_LIBRARY_SEARCH_COMMAND);
	} else {
		debug(D_ERROR, "we are not supporting any other GPU's apart from Nvidia or AMD");
		return lib_result;
	}

	FILE *fp = popen(command, "r");
	if (fp == NULL) {
		debug(D_ERROR, "popen failed to run ldconfig command");
		return lib_result;
	}

	if (fgets(result, sizeof(result), fp) != NULL) {
		result[strcspn(result, "\n")] = 0;
		strncpy(lib_result.path, result, sizeof(lib_result.path) - 1);
		lib_result.path[sizeof(lib_result.path) - 1] = '\0';
		lib_result.found = true;
		debug(D_DEBUG, "GPU Library found at path %s", lib_result.path);
	} else {
		debug(D_DEBUG, "GPU Library not found in system search paths.");
	}

	pclose(fp);
	return lib_result;
}
