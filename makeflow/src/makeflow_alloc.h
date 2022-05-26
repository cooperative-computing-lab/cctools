
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
	MAKEFLOW_ALLOC_RELEASE_COMMIT,      /* Intended to denote release of committed space, not used */
	MAKEFLOW_ALLOC_RELEASE_USED         /* Denotes release of used space */
} makeflow_alloc_release;

typedef enum {
	MAKEFLOW_ALLOC_TYPE_MAX = 0,        /* Allocation events are logged, limit set to max size for concurrency */
	MAKEFLOW_ALLOC_TYPE_MIN,            /* Allocation events are logged, limit is imposed on footprint of nodes */
	MAKEFLOW_ALLOC_TYPE_OUT,            /* Allocation events are logged, limit tracks only on output of active nodes */
	MAKEFLOW_ALLOC_TYPE_OFF,            /* Allocation events are logged, but doesn't limit storage */
	MAKEFLOW_ALLOC_TYPE_NOT_ENABLED     /* Allocation monitoring is not enabled. */
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
