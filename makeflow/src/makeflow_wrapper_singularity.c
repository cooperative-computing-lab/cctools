/*
 * Copyright (C) 2016- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_singularity.h"

#include <string.h>
#include <stdlib.h>

/* 1) create a global script for running docker container
 * 2) add this script to the global wrapper list
 * 3) reformat each task command
 */
void makeflow_wrapper_singularity_init(struct makeflow_wrapper *w, char *container_image) {
    FILE *wrapper_fn;

    wrapper_fn = fopen(CONTAINER_SINGULARITY_SH, "w");

    char* filedata;
    //$@ passes in everything, thus this should be awesome and just say "singularity exect <contimg> ALL THE THINGS
    filedata = string_format("%s\n%s\n",
                             "#!/bin/sh",
                             "singularity exec %s \"$@\"");
    
    fprintf(wrapper_fn, filedata, container_image);

    fclose(wrapper_fn);

    chmod(CONTAINER_SINGULARITY_SH, 0755);

    makeflow_wrapper_add_input_file(w, CONTAINER_SINGULARITY_SH);

    char *global_cmd = string_format("sh %s", CONTAINER_SINGULARITY_SH);
    makeflow_wrapper_add_command(w, global_cmd);
    
    free(filedata);
}