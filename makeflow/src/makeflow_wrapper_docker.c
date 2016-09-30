/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_docker.h"

#include <string.h>
#include <stdlib.h>

/* 1) create a global script for running docker container
 * 2) add this script to the global wrapper list
 * 3) reformat each task command
 */
void makeflow_wrapper_docker_init( struct makeflow_wrapper *w, char *container_image, char *image_tar )
{
	FILE *wrapper_fn;

	wrapper_fn = fopen(CONTAINER_DOCKER_SH, "w");

	if (image_tar == NULL) {

		fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker pull %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", container_image, container_image);

	} else {

		fprintf(wrapper_fn, "#!/bin/sh\n\
curr_dir=`pwd`\n\
default_dir=/root/worker\n\
flock /tmp/lockfile /usr/bin/docker load < %s\n\
docker run --rm -m 1g -v $curr_dir:$default_dir -w $default_dir %s \"$@\"\n", image_tar, container_image);

		makeflow_wrapper_add_input_file(w, image_tar);
	}

	fclose(wrapper_fn);

	chmod(CONTAINER_DOCKER_SH, 0755);

	makeflow_wrapper_add_input_file(w, CONTAINER_DOCKER_SH);

	char *global_cmd = string_format("sh %s", CONTAINER_DOCKER_SH);
	makeflow_wrapper_add_command(w, global_cmd);
}
