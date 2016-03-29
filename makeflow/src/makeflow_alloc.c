/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"

#include "dag_file.h"
#include "dag_node.h"
#include "makeflow_alloc.h"

#include "list.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

uint64_t dynamic_alloc = 0;

struct makeflow_alloc_unit * makeflow_alloc_unit_create(uint64_t size)
{
	struct makeflow_alloc_unit *u = malloc(sizeof(*u));
	u->total	= size;
	u->used		= 0;
	u->greedy	= 0;
	u->commit	= 0;
	u->free		= size;

	return u;
}

void makeflow_alloc_unit_delete( struct makeflow_alloc_unit *u)
{
	free(u);
	return;
}

struct makeflow_alloc * makeflow_alloc_create(int nodeid, struct makeflow_alloc *parent, uint64_t size, int locked, makeflow_alloc_type type)
{
	struct makeflow_alloc *a = malloc(sizeof(*a));
	a->nodeid = nodeid;
	a->storage  = makeflow_alloc_unit_create(size);
	a->parent = parent;
	a->residuals = list_create();
	a->locked = locked;
	a->ordered = 1;
	a->enabled = type;

	return a;
}

void makeflow_alloc_delete(struct makeflow_alloc *a)
{
	makeflow_alloc_unit_delete(a->storage);
	if(a->parent){
		list_remove(a->parent->residuals, a);
	}
	list_delete(a->residuals);
	free(a);
	return;
}

struct makeflow_alloc * makeflow_alloc_traverse_to_node(struct makeflow_alloc *a, struct dag_node *n)
{
	struct dag_node *node;
	struct makeflow_alloc *alloc1, *alloc2, *tmp;

	alloc1 = a;
	list_first_item(n->residual_nodes);
	while((node = list_peek_current(n->residual_nodes))){
		tmp = NULL;
		list_first_item(alloc1->residuals);
		while((alloc2 = list_next_item(alloc1->residuals))){
			if(alloc2->nodeid == node->nodeid){
				tmp = alloc2;
				break;
			}
		}
		if(tmp){
			alloc1 = tmp;
		}else{
			break;
		}

		list_next_item(n->residual_nodes);
	}
	return alloc1;
}

int makeflow_alloc_try_grow_alloc( struct makeflow_alloc *a, uint64_t inc)
{
	if(!a) // Case that we are at parent and we will now try to grow
		return 0;

	struct makeflow_alloc *tmp = a->parent;
	if(a->storage->free >= inc){ // Fits in the already free space
		return 1;
	} else if(a->nodeid == -1 && !a->locked){ // At top and we can use more space
		return 1;
	} else if(tmp) { // Within an existing alloc
		uint64_t needed = inc - a->storage->free;
		if(tmp->storage->free >= needed || // Fits in the parents free space
				(makeflow_alloc_try_grow_alloc(tmp, needed))){  // Parent can grow to fit us
			return 1;
		}
	}


	return 0;
}

uint64_t makeflow_alloc_node_size( struct makeflow_alloc *a, struct dag_node *cur_node, struct dag_node *n)
{
	uint64_t alloc_size;
	uint64_t freed_space = 0;
	struct dag_file *f;
	if(n->footprint_min_type != DAG_NODE_FOOTPRINT_RUN){
		list_first_item(n->source_files);
		while((f = list_next_item(n->source_files))){
			if(f->reference_count == 1){
				freed_space += dag_file_size(f);
			}
		}
	}

	switch(a->enabled){
		case MAKEFLOW_ALLOC_TYPE_SIZE:
			alloc_size = n->target_size;
			break;
		case MAKEFLOW_ALLOC_TYPE_MIN:
			alloc_size = cur_node->footprint_min_size - freed_space;
			break;
		case MAKEFLOW_ALLOC_TYPE_MAX:
			alloc_size = cur_node->footprint_max_size - freed_space;
			break;
		default:
			alloc_size = 0;
	}
	return alloc_size;
}

int makeflow_alloc_check_space( struct makeflow_alloc *a, struct dag_node *n)
{
	uint64_t start = timestamp_get();
	if(!a){
		dynamic_alloc += timestamp_get() - start;
		return 0;
	}

	if(a->enabled == MAKEFLOW_ALLOC_TYPE_OFF){
		if(a->storage->used >= a->storage->total){
			dynamic_alloc += timestamp_get() - start;
			return 0;
		}
		dynamic_alloc += timestamp_get() - start;
		return 1;
	}

	struct dag_node *node1;
	struct makeflow_alloc *alloc1, *alloc2;

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid == n->nodeid){
		if(alloc1->storage->free < n->target_size){
			dynamic_alloc += timestamp_get() - start;
			return 0;
		}
		dynamic_alloc += timestamp_get() - start;
		return 1;
	}

	while((node1 = list_peek_current(n->residual_nodes))){
		alloc2 = makeflow_alloc_create(node1->nodeid, alloc1, 0, 0, a->enabled);
		if(!(makeflow_alloc_try_grow_alloc(alloc2, makeflow_alloc_node_size(a, node1, n))
			|| (n == node1 && set_size(n->descendants) < 2 &&
				makeflow_alloc_try_grow_alloc(alloc2, node1->self_res)))){
			dynamic_alloc += timestamp_get() - start;
			return 0;
		}

		makeflow_alloc_delete(alloc2);
		list_next_item(n->residual_nodes);
	}

	dynamic_alloc += timestamp_get() - start;
	return 1;
}



int makeflow_alloc_grow_alloc( struct makeflow_alloc *a, uint64_t inc)
{
	if(!a) // Case that we are at parent and we will now try to grow
		return 0;

	struct makeflow_alloc *tmp = a->parent;
	if(a->storage->free >= inc){ // Fits in the already free space
		return 1;
	} else if(a->nodeid == -1 && !a->locked){ // At top and we can use more space
		a->storage->total    += inc;
		a->storage->free     += inc;
		return 1;
	} else if(tmp) { // Within an existing alloc
		uint64_t needed = inc - a->storage->free;
		if(tmp->storage->free >= needed || // Fits in the parents free space
				(makeflow_alloc_grow_alloc(tmp, needed))){  // Parent can grow to fit us
			tmp->storage->commit += needed;
			tmp->storage->free   -= needed;
			a->storage->total    += needed;
			a->storage->free     += needed;
			return 1;
		}
	}


	return 0;
}

int makeflow_alloc_commit_space( struct makeflow_alloc *a, struct dag_node *n)
{
	uint64_t start = timestamp_get();
	if(!a)
		return 0;

	if(a->enabled == MAKEFLOW_ALLOC_TYPE_OFF)
		return 1;

	struct dag_node *node1;
	struct makeflow_alloc *alloc1, *alloc2;

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid == n->nodeid){
		if(alloc1->storage->free < n->target_size){
			dynamic_alloc += timestamp_get() - start;
			return 0;
		}
	} else {
		while((node1 = list_peek_current(n->residual_nodes))){
			alloc2 = makeflow_alloc_create(node1->nodeid, alloc1, 0, 0, a->enabled);
			if(!(makeflow_alloc_grow_alloc(alloc2, makeflow_alloc_node_size(a, node1, n))
				|| (n == node1 && set_size(n->descendants) < 2 &&
					makeflow_alloc_grow_alloc(alloc2, node1->self_res)))){
				dynamic_alloc += timestamp_get() - start;
				return 0;
			}
			list_push_tail(alloc1->residuals, alloc2);
			alloc1 = alloc2;

			list_next_item(n->residual_nodes);
		}
	}

	alloc1->storage->greedy += n->target_size;
	alloc1->storage->free   -= n->target_size;
	alloc1 = alloc1->parent;
	while(alloc1){
		alloc1->storage->greedy += n->target_size;
		alloc1->storage->commit -= n->target_size;
		alloc1 = alloc1->parent;
	}

	dynamic_alloc += timestamp_get() - start;
	return 1;
}

int makeflow_alloc_use_space( struct makeflow_alloc *a, struct dag_node *n)
{
	uint64_t start = timestamp_get();
	uint64_t inc = dag_node_file_list_size(n->target_files);
	if(a->enabled == MAKEFLOW_ALLOC_TYPE_OFF){
		a->storage->used   += inc;
		dynamic_alloc += timestamp_get() - start;
		return 1;
	}

	a = makeflow_alloc_traverse_to_node(a, n);

	uint64_t needed;
	if(inc > a->storage->greedy){
		needed = inc - a->storage->greedy;

		if(needed > a->storage->commit){
			uint64_t grow = needed - a->storage->commit;
			if(!makeflow_alloc_grow_alloc(a, grow)){
				dynamic_alloc += timestamp_get() - start;
				return 0;
			}
			a->storage->commit += grow;

		}
		a->storage->greedy += needed;
		struct makeflow_alloc *a2 = a->parent;
		while(a2){
			a2->storage->greedy += needed;
			a2->storage->commit -= needed;
			a2 = a2->parent;
		}
	}

	if(inc < a->storage->greedy){
		needed = a->storage->greedy - inc;
	}

	while(a){
		a->storage->used   += inc;
		a->storage->greedy -= (inc + needed);
		a = a->parent;
	}

	dynamic_alloc += timestamp_get() - start;
	return 1;
}

int makeflow_alloc_shrink_alloc( struct makeflow_alloc *a, uint64_t dec, makeflow_alloc_release release)
{
	if(!a)
		return 0;

	if(release == MAKEFLOW_ALLOC_RELEASE_USED){
		a->storage->used  -= dec;
		a->storage->total -= dec;
		if(a->parent){
			a = a->parent;
			a->storage->used -= dec;
			a->storage->free += dec;
		}
		while(a->parent){
			a = a->parent;
			a->storage->used   -= dec;
			a->storage->commit += dec;
		}
	} else {
		dec = a->storage->commit;
		a->storage->commit = 0;
		dec += a->storage->free;
		a->storage->free   = 0;
		a->storage->total -= dec;

		if(a->parent){
			a->parent->storage->commit -= dec;
			a->parent->storage->free   += dec;
		}


	}
	return 1;
}


int makeflow_alloc_release_space( struct makeflow_alloc *a, struct dag_node *n, uint64_t size, makeflow_alloc_release release)
{
	uint64_t start = timestamp_get();
	struct makeflow_alloc *alloc1;

	if(a->enabled == MAKEFLOW_ALLOC_TYPE_OFF){
		a->storage->used -= size;
		dynamic_alloc += timestamp_get() - start;
		return 1;
	}

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid != n->nodeid){
		dynamic_alloc += timestamp_get() - start;
		return 0;
	}

	makeflow_alloc_shrink_alloc(alloc1, size, release);

	if(alloc1->storage->total == 0){
		makeflow_alloc_delete(alloc1);
	}

	dynamic_alloc += timestamp_get() - start;
	return 1;
}

uint64_t makeflow_alloc_get_dynamic_alloc_time()
{
	return dynamic_alloc;
}
