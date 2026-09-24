/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GPU_COMMONS_H
#define GPU_COMMONS_H

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

// enum for the GPU vendor
enum gpu_vendor {
    NVIDIA = 0,
    AMD = 1,
};

// this says whether the searched library exists or not
struct library_search_result {
    char path[512];
    bool found;
};

// AMD place holder library
struct amd_rocm_hsa_library {};

struct library_search_result find_library_by_name(enum gpu_vendor vendor);

#endif
