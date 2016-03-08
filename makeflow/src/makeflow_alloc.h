
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

struct makeflow_alloc_unit {
	uint64_t total;
	uint64_t commit;
	uint64_t free;
};

struct makeflow_alloc {
	int nodeid;
	struct makeflow_alloc_unit *storage;
	struct makeflow_alloc *parent;
	struct list *residuals;
	int locked;
};

struct makeflow_alloc * makeflow_alloc_create(int nodeid, struct makeflow_alloc *parent, uint64_t size, int locked);

void makeflow_alloc_print( struct makeflow_alloc *a, struct dag_node *n);

int makeflow_alloc_check_node_size( struct makeflow_alloc *a, struct dag_node *n);
int makeflow_alloc_commit_space( struct makeflow_alloc *a, struct dag_node *n);
int makeflow_alloc_release_space( struct makeflow_alloc *a, struct dag_node *n, uint64_t size, int free);
#endif
