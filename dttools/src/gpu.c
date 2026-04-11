/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "gpu.h"
#include "debug.h"
#include "gpu_commons.h"
#include "nvidia_nvml_library.h"

// nvidia libray name as macro
const char * NVIDIA_LIBRARY_NAME = "libnvidia-ml.so";
// AMD library need to check for now we are only intrested in Nvidia
// the below library is not verified but referenced for future use case
const char * AMD_LIBRARY_NAME = "libhsa-runtime64.so";


struct gpu_library * gpu_lib_init(){
    struct gpu_library * gpu_lib = calloc(1, sizeof(*gpu_lib));
    if (!gpu_lib) {
        debug(D_DEBUG,"Out of memory allocating gpu_library");
        return NULL;
    }

    enum gpu_vendor vendor = NVIDIA;

    // TODO: write a logic to figure out the vendor
    // for now stick with nvidia

    if(vendor == NVIDIA) {
        gpu_lib->vendor = NVIDIA;
        struct library_search_result gpu_lib_path = find_library_by_name(NVIDIA_LIBRARY_NAME);
        if(gpu_lib_path.found){
         gpu_lib->nvidia_lib = nvml_library_open(gpu_lib_path);   
        } else {
            // not found so lets return null
            free(gpu_lib);
            return NULL;
        }

    } else if(vendor == AMD) {
        gpu_lib->vendor = AMD;
        struct library_search_result gpu_lib_path = find_library_by_name(AMD_LIBRARY_NAME);
        if(gpu_lib_path.found){
            // work around that we are not supporting the GPU for now
            free(gpu_lib);
            return NULL;
        } else {
            // not found so lets return null
            free(gpu_lib);
            return NULL;
        }
    }
    return gpu_lib;
}


void gpu_lib_close(struct gpu_library * gpu_lib){
    if(gpu_lib->vendor == NVIDIA){
        // call the close function from nvidia_library
        nvml_library_close(gpu_lib->nvidia_lib);
        free(gpu_lib);
    } else if(gpu_lib->vendor == AMD){
        // call the close function from amd_library
        free(gpu_lib);
    }
}

char *gpu_name_get_new(struct gpu_library * gpu_lib){
    char * name = malloc(64);
    if(gpu_lib->vendor == NVIDIA){
        name = nvml_gpu_name(gpu_lib->nvidia_lib);
    } else if(gpu_lib->vendor == AMD){
        // todo work in future
        name = "AMD GPU Not Available";
    }
    return name;
}
