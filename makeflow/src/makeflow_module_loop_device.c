
#include "create_dir.h"
#include "debug.h"
#include "jx.h"
#include "path.h"
#include "rmonitor.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "dag.h"
#include "dag_file.h"
#include "dag_node.h"
#include "makeflow_hook.h"
#include "makeflow_log.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int register_hook(struct makeflow_hook *hook, struct list *makeflow_hooks) { return MAKEFLOW_HOOK_SUCCESS; }

static int node_fail(struct dag_node *n, struct batch_task *task)
{
	if (task->info->disk_allocation_exhausted) {
		fprintf(stderr, "\nrule %d failed because it exceeded its loop device allocation capacity.\n",
				n->nodeid);
		if (n->resources_measured) {
			rmsummary_print(stderr, n->resources_measured, /* pprint */ 0, /* extra fields */ NULL);
			fprintf(stderr, "\n");
		}
		return MAKEFLOW_HOOK_FAILURE;
	}
	return MAKEFLOW_HOOK_SUCCESS;
}

struct makeflow_hook makeflow_hook_loop_device = {
		.module_name = "Loop Device",

		.register_hook = register_hook,

		.node_fail = node_fail,
};
