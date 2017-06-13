
/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_ALLOC_H
#define MAKEFLOW_ALLOC_H

#include "dag_node.h"

/*
This module implements resource allocations.
*/

typedef enum {
	MAKEFLOW_ALLOC_RELEASE_COMMIT,		/* Clean nothing, default. */
	MAKEFLOW_ALLOC_RELEASE_USED			/* Clean nothing, default. */
} makeflow_alloc_release;

typedef enum {
	MAKEFLOW_ALLOC_TYPE_MAX = 0,		/* Clean nothing, default. */
	MAKEFLOW_ALLOC_TYPE_MIN,			/* Clean nothing, default. */
	MAKEFLOW_ALLOC_TYPE_OUT,			/* Clean nothing, default. */
	MAKEFLOW_ALLOC_TYPE_OFF 		   	/* Clean nothing, default. */
} makeflow_alloc_type;

struct makeflow_alloc_unit {
	uint64_t total;
	uint64_t used;
	uint64_t greedy;
	uint64_t commit;
	uint64_t free;
};

struct makeflow_alloc {
	int nodeid;
	struct makeflow_alloc_unit *storage;
	struct makeflow_alloc *parent;
	struct list *residuals;
	int locked;
	int ordered;
	makeflow_alloc_type enabled;
};

struct makeflow_alloc * makeflow_alloc_create(int nodeid, struct makeflow_alloc *parent, uint64_t size, int locked, makeflow_alloc_type type);

void makeflow_alloc_print( struct makeflow_alloc *a, struct dag_node *n);

int makeflow_alloc_check_space( struct makeflow_alloc *a, struct dag_node *n);
int makeflow_alloc_commit_space( struct makeflow_alloc *a, struct dag_node *n);
int makeflow_alloc_use_space( struct makeflow_alloc *a, struct dag_node *n);
int makeflow_alloc_release_space( struct makeflow_alloc *a, struct dag_node *n, uint64_t size, makeflow_alloc_release release);
uint64_t makeflow_alloc_get_dynamic_alloc_time();
#endif
