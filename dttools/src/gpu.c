/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu.h"
#include "debug.h"
#include "gpu_commons.h"
#include "nvidia_nvml_library.h"
#include <string.h>

struct gpu_library *gpu_lib_init()
{
	struct gpu_library *gpu_lib = calloc(1, sizeof(*gpu_lib));
	if (!gpu_lib) {
		debug(D_DEBUG, "Out of memory allocating gpu_library");
		return NULL;
	}

	enum gpu_vendor vendor = NVIDIA;

	// TODO: write a logic to figure out the vendor
	// for now stick with nvidia
	gpu_lib->vendor = vendor;
	struct library_search_result gpu_lib_path = find_library_by_name(vendor);
	if (gpu_lib_path.found) {
		if (vendor == NVIDIA) {
			gpu_lib->nvidia_lib = nvml_library_open(gpu_lib_path);
			return gpu_lib;
		} else if (vendor == AMD) {
			// work around that we are not supporting the GPU for now
			free(gpu_lib);
		} else {
			free(gpu_lib);
		}
	}
	return NULL;
}

void gpu_lib_close(struct gpu_library *gpu_lib)
{
	if (gpu_lib != NULL) {
		if (gpu_lib->vendor == NVIDIA) {
			// call the close function from nvidia_library
			nvml_library_close(gpu_lib->nvidia_lib);
			free(gpu_lib);
		} else if (gpu_lib->vendor == AMD) {
			// call the close function from amd_library
			free(gpu_lib);
		}
	}
}

char *gpu_name_get_new(struct gpu_library *gpu_lib)
{
	if (gpu_lib->vendor == NVIDIA) {
		return nvml_gpu_name(gpu_lib->nvidia_lib);
	} else if (gpu_lib->vendor == AMD) {
		// todo work in future
		char *name = strdup("AMD GPU Not Supported");
		if (name == NULL) {
			return NULL;
		}
		return name;
	}
	return NULL;
}
