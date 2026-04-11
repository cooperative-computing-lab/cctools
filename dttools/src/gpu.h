/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GPU
#define GPU

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>

// enum for the GPU vendor
enum gpu_vendor {
    NVIDIA = 0,
    AMD = 1,
};

// generic gpu options which can be used everywhere
struct gpu_library {
  enum gpu_vendor vendor;
  struct nvml_library * nvidia_lib;
  struct amd_rocm_hsa_library * amd_lib;
  // ... add more if we decide to support more
};

struct gpu_library * gpu_lib_init();
void gpu_lib_close(struct gpu_library * gpu_lib);
char *gpu_name_get_new(struct gpu_library * gpu_lib);

#endif
