/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvidia_nvml_library.h"
#include "debug.h"
#include <string.h>

#define LOAD_SYMBOL(handle, field, type, name) \
	do { \
		dlerror(); \
		(field) = (type)dlsym((handle), (name)); \
		const char *sym_err = dlerror(); \
		if (sym_err) { \
			debug(D_ERROR, "dlsym failed for %s: %s", (name), sym_err); \
			goto fail; \
		} \
	} while (0)

struct nvml_library *nvml_library_open(struct library_search_result res)
{
	struct nvml_library *nvml = calloc(1, sizeof(*nvml));
	if (!nvml) {
		debug(D_ERROR, "Out of memory allocating nvml_library");
		return NULL;
	}

	if (!res.found || res.path[0] == '\0') {
		debug(D_ERROR, "Error: GPU Library path was not found.");
		free(nvml);
		return NULL;
	}

	debug(D_DEBUG, "GPU library opening from path %s", res.path);
	nvml->lib_handle = dlopen(res.path, RTLD_NOW | RTLD_LOCAL);
	if (!nvml->lib_handle) {
		debug(D_ERROR, "dlopen failed: %s", dlerror());
		free(nvml);
		return NULL;
	}

	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlInit, nvmlReturn_t (*)(void), "nvmlInit");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlShutdown, nvmlReturn_t (*)(void), "nvmlShutdown");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlDeviceGetHandleByIndex, nvmlReturn_t (*)(int, nvmlDevice_t *), "nvmlDeviceGetHandleByIndex");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlDeviceGetMemoryInfo, nvmlReturn_t (*)(nvmlDevice_t, nvmlMemory_t *), "nvmlDeviceGetMemoryInfo");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlDeviceGetUtilizationRates, nvmlReturn_t (*)(nvmlDevice_t, nvmlUtilization_t *), "nvmlDeviceGetUtilizationRates");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlDeviceGetCount, nvmlReturn_t (*)(int *), "nvmlDeviceGetCount");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlDeviceGetName, nvmlReturn_t (*)(nvmlDevice_t, char *, unsigned int), "nvmlDeviceGetName");
	LOAD_SYMBOL(nvml->lib_handle, nvml->nvmlErrorString, char *(*)(nvmlReturn_t), "nvmlErrorString");

	debug(D_DEBUG, "library nvml load completed!");

	nvmlReturn_t init_result = nvml->nvmlInit();
	debug(D_DEBUG, "initialization result %d", init_result);
	if (init_result != NVML_SUCCESS) {
		debug(D_ERROR, "GPU initialization failed with error code %i", init_result);
		goto fail;
	}
	return nvml;

fail:
	nvml_library_close(nvml);
	return NULL;
}

void nvml_library_close(struct nvml_library *lib)
{
	if (!lib) {
		return;
	}
	// now call the shutdown function to properly close the nvml lib
	if (lib->nvmlShutdown) {
		nvmlReturn_t shutdown_result = lib->nvmlShutdown();
		if (shutdown_result != NVML_SUCCESS) {
			debug(D_ERROR, "GPU shutdown failed with error code %i", shutdown_result);
			// doesnt matter now close the lib anyways
		}
	}
	if (lib->lib_handle) {
		dlclose(lib->lib_handle);
	}
	free(lib);
}

char *nvml_gpu_name(struct nvml_library *nvml_lib)
{
	if (!nvml_lib) {
		debug(D_ERROR, "nvml_gpu_name called with NULL nvml_lib");
		return NULL;
	}

	int device_count = 0;
	nvmlReturn_t count_result = nvml_lib->nvmlDeviceGetCount(&device_count);
	if (count_result != NVML_SUCCESS) {
		debug(D_ERROR, "Failed to get device count: %s", nvml_lib->nvmlErrorString(count_result));
		return NULL;
	}

	if (device_count <= 0) {
		debug(D_ERROR, "No GPU devices found");
		return NULL;
	}

	char *name = malloc(NVML_DEVICE_NAME_BUFFER_SIZE);
	if (!name) {
		debug(D_ERROR, "Out of memory allocating GPU name buffer");
		return NULL;
	}

	nvmlDevice_t device;
	nvmlReturn_t result = nvml_lib->nvmlDeviceGetHandleByIndex(0, &device);
	if (result != NVML_SUCCESS) {
		debug(D_ERROR, "Failed to get device handle: %s", nvml_lib->nvmlErrorString(result));
		free(name);
		return NULL;
	}

	result = nvml_lib->nvmlDeviceGetName(device, name, NVML_DEVICE_NAME_BUFFER_SIZE);
	if (result != NVML_SUCCESS) {
		debug(D_ERROR, "Failed to get device name: %s", nvml_lib->nvmlErrorString(result));
		free(name);
		return NULL;
	}

	debug(D_DEBUG, "GPU Name: %s", name);
	return name;
}
