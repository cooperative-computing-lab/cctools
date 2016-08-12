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
    char* file_sans_compress = string_format("%s",container_image);

    char* filedata;
    //$@ passes in everything, thus this should be awesome and just say "singularity exect <contimg> ALL THE THINGS
    if(strstr(container_image,".gz") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xzf $s",
                                 "singularity exec %s \"$@\"");
        file_sans_compress[strlen(container_image)-3] = '\0';
        fprintf(wrapper_fn, filedata, container_image, file_sans_compress);
    }else if(strstr(container_image,".xz") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xf %s",
                                 "singularity exec %s \"$@\"");
        file_sans_compress[strlen(container_image)-3] = '\0';
        fprintf(wrapper_fn, filedata, container_image, file_sans_compress);
    }else if(strstr(container_image,".bz2") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xjf %s",
                                 "singularity exec %s \"$@\"");
        file_sans_compress[strlen(container_image)-4] = '\0';
        fprintf(wrapper_fn, filedata, container_image, file_sans_compress);
    }else{
        filedata = string_format("%s\n%s\n",
                                 "#!/bin/sh",
                                 "singularity exec %s \"$@\"");
        fprintf(wrapper_fn, filedata, container_image);
    }
    
    //fprintf(wrapper_fn, filedata, container_image);

    fclose(wrapper_fn);

    makeflow_wrapper_add_input_file(w, container_image);
    
    chmod(CONTAINER_SINGULARITY_SH, 0755);

    makeflow_wrapper_add_input_file(w, CONTAINER_SINGULARITY_SH);

    char *global_cmd = string_format("sh %s", CONTAINER_SINGULARITY_SH);
    makeflow_wrapper_add_command(w, global_cmd);
    
    free(filedata);
    free(file_sans_compress);
}