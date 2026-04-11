/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GPU_COMMONS
#define GPU_COMMONS

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

// this says whether the searched library exists or not
struct library_search_result {
    char path[512];
    bool found;
};

// AMD place holder library
struct amd_rocm_hsa_library {};

struct library_search_result find_library_by_name(const char *lib_name);

#endif
