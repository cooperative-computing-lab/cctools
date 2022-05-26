/*
 Copyright (C) 2008- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

#include "makeflow_hook.h"

#include <stdio.h>

/*
 * Simple create function that prints out a message.
 */
static int makeflow_hook_example_create(void ** instance_struct, struct jx *args){
	printf("Hello from module: EXAMPLE.\n");
	return MAKEFLOW_HOOK_SUCCESS;
}

static int makeflow_hook_example_destroy(void * instance_struct, struct dag *d){
	printf("Goodbye from module: EXAMPLE.\n");
	return MAKEFLOW_HOOK_SUCCESS;
}
	
struct makeflow_hook makeflow_hook_example = {
	.module_name = "MAKEFLOW_EXAMPLE",
	.create = makeflow_hook_example_create,
	.destroy = makeflow_hook_example_destroy,
};


