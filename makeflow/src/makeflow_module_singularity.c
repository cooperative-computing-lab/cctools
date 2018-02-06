/*
 Copyright (C) 2018- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_gc.h"
#include "makeflow_hook.h"

#include <string.h>
#include <stdlib.h>

#define CONTAINER_SINGULARITY_SH "singularity.wrapper.sh"

char *singularity_global_command = NULL;

char *singularity_image = NULL;

char *singularity_tar = NULL;

static int create( struct jx *hook_args )
{
    if(jx_lookup_string(hook_args, "singularity_container_image"))
        singularity_image = xxstrdup(jx_lookup_string(hook_args, "singularity_container_image"));	

    if(jx_lookup_string(hook_args, "singularity_image_tar"))
        singularity_tar = xxstrdup(jx_lookup_string(hook_args, "singularity_image_tar"));	

	return MAKEFLOW_HOOK_SUCCESS;
}

static int destroy( struct dag *d )
{
	if(makeflow_clean_file(d, makeflow_get_remote_queue(), dag_file_lookup_or_create(d, CONTAINER_SINGULARITY_SH), 1))
		return MAKEFLOW_HOOK_SUCCESS;
	return MAKEFLOW_HOOK_FAILURE;
}

static int dag_start(struct dag *d){
	FILE *wrapper_fn = fopen(CONTAINER_SINGULARITY_SH, "w");

    char* file_sans_compress = string_format("%s",singularity_image);

    char* filedata;
    //$@ passes in everything, thus this should be awesome and just say "singularity exect <contimg> ALL THE THINGS

    if(strstr(singularity_image,".gz") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xzf $s",
                                "singularity exec --home `pwd` %s \"$@\"");
        file_sans_compress[strlen(singularity_image)-3] = '\0';
        fprintf(wrapper_fn, filedata, singularity_image, file_sans_compress);
    }else if(strstr(singularity_image,".xz") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xf %s",
                                 "singularity exec --home `pwd` %s \"$@\"");
        file_sans_compress[strlen(singularity_image)-3] = '\0';
        fprintf(wrapper_fn, filedata, singularity_image, file_sans_compress);
    }else if(strstr(singularity_image,".bz2") != NULL){
        filedata = string_format("%s\n%s\n%s\n",
                                 "#!/bin/sh",
                                 "tar -xjf %s",
                                 "singularity exec --home `pwd` %s \"$@\"");
        file_sans_compress[strlen(singularity_image)-4] = '\0';
        fprintf(wrapper_fn, filedata, singularity_image, file_sans_compress);
    }else{
        filedata = string_format("%s\n%s\n",
                                 "#!/bin/sh",
                                 "singularity exec --home `pwd` %s \"$@\"");
        fprintf(wrapper_fn, filedata, singularity_image);
    }
	fclose(wrapper_fn);

    chmod(CONTAINER_SINGULARITY_SH, 0755);

    free(filedata);
    free(file_sans_compress);

	dag_file_lookup_or_create(d, singularity_image);
	dag_file_lookup_or_create(d, CONTAINER_SINGULARITY_SH);

	singularity_global_command = string_format("sh %s", CONTAINER_SINGULARITY_SH);

	return MAKEFLOW_HOOK_SUCCESS;
}

static int node_submit(struct dag_node *n, struct batch_task *t){

	makeflow_hook_add_input_file(n->d, t, singularity_image, NULL);
	makeflow_hook_add_input_file(n->d, t, CONTAINER_SINGULARITY_SH, NULL);

	batch_task_wrap_command(t, singularity_global_command);

	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_singularity = {
	.module_name = "Singularity",
	.create = create,
	.destroy = destroy,

	.dag_start = dag_start,

	.node_submit = node_submit,
};


