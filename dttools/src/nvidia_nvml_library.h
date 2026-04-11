/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef NVIDIA_NVML_LIBRARY
#define NVIDIA_NVML_LIBRARY

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>
#include "gpu_commons.h"

// ========================= NVML Header content =================

// buffer size is large enough to store the GPU name
// this is how the NVML library defines the buffer size for device names
#define NVML_DEVICE_NAME_BUFFER_SIZE 64

typedef enum {
    NVML_SUCCESS = 0,
    NVML_ERROR_UNINITIALIZED = 1,
    NVML_ERROR_INVALID_ARGUMENT = 2,
    NVML_ERROR_NOT_SUPPORTED = 3,
    NVML_ERROR_NO_PERMISSION = 4,
    NVML_ERROR_ALREADY_INITIALIZED = 5,
    NVML_ERROR_NOT_FOUND = 6,
    NVML_ERROR_INSUFFICIENT_SIZE = 7,
    NVML_ERROR_INSUFFICIENT_POWER = 8,
    NVML_ERROR_DRIVER_NOT_LOADED = 9,
    NVML_ERROR_TIMEOUT = 10,
    NVML_ERROR_IRQ_ISSUE = 11,
    NVML_ERROR_LIBRARY_NOT_FOUND = 12,
    NVML_ERROR_FUNCTION_NOT_FOUND = 13,
    NVML_ERROR_CORRUPTED_INFOROM = 14,
    NVML_ERROR_GPU_IS_LOST = 15,
    NVML_ERROR_RESET_REQUIRED = 16,
    NVML_ERROR_OPERATING_SYSTEM = 17,
    NVML_ERROR_LIB_RM_VERSION_MISMATCH = 18,
    NVML_ERROR_IN_USE = 19,
    NVML_ERROR_MEMORY = 20,
    NVML_ERROR_NO_DATA = 21,
    NVML_ERROR_VGPU_ECC_NOT_SUPPORTED = 22,
    NVML_ERROR_INSUFFICIENT_RESOURCES = 23,
    NVML_ERROR_FREQ_NOT_SUPPORTED = 24,
    NVML_ERROR_ARGUMENT_VERSION_MISMATCH = 25,
    NVML_ERROR_DEPRECATED = 26,
    NVML_ERROR_NOT_READY = 27,
    NVML_ERROR_GPU_NOT_FOUND = 28,
    NVML_ERROR_INVALID_STATE = 29,
    NVML_ERROR_RESET_TYPE_NOT_SUPPORTED = 30,
    NVML_ERROR_UNKNOWN = 999
} nvmlReturn_t;

// type definition representing GPU device
typedef void * nvmlDevice_t;

// type definition representing memory utilization by GPU
typedef struct {
    unsigned long long total;
    unsigned long long free;
    unsigned long long used;
} nvmlMemory_t;

// type definition representing the gpu utilization
typedef struct {
    unsigned int gpu;
    unsigned int memory;
}  nvmlUtilization_t;

// library methods that we trap calls for Nvidia GPU metrics
struct nvml_library {
    // dlopen handle of libnvidia-ml.so being part of struct
    // so that we can close and free the memory for clean destruction
    void *lib_handle;
    // all nvml function calls trapped from libnvidia-ml.so
    // reason for trapping in future we can bring AMD or any GPU support
    // below is continous and prefetch is better for CPU
    nvmlReturn_t (*nvmlInit)(void);
    nvmlReturn_t (*nvmlShutdown)(void);
    nvmlReturn_t (*nvmlDeviceGetCount)(int *);
    nvmlReturn_t (*nvmlDeviceGetHandleByIndex)(int, nvmlDevice_t *);
    nvmlReturn_t (*nvmlDeviceGetName)(nvmlDevice_t, char *, unsigned int);
    nvmlReturn_t (*nvmlDeviceGetMemoryInfo)(nvmlDevice_t, nvmlMemory_t *);
    nvmlReturn_t (*nvmlDeviceGetUtilizationRates)(nvmlDevice_t, nvmlUtilization_t *);
    char *(*nvmlErrorString)(nvmlReturn_t);
};

struct nvml_library *nvml_library_open(struct library_search_result res);
void nvml_library_close(struct nvml_library *lib);
char * nvml_gpu_name(struct nvml_library *nvml_lib);

#endif
