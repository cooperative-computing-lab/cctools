/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"

#include "dag_node.h"
#include "makeflow_alloc.h"

#include "list.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

struct makeflow_alloc_unit * makeflow_alloc_unit_create(uint64_t size)
{
	struct makeflow_alloc_unit *u = malloc(sizeof(*u));
	u->total	= size;
	u->used		= 0;
	u->commit	= 0;
	u->free		= size;

	return u;
}

struct makeflow_alloc * makeflow_alloc_create(int nodeid, struct makeflow_alloc *parent, uint64_t size, int locked)
{
	struct makeflow_alloc *a = malloc(sizeof(*a));
	a->nodeid = nodeid;
	a->storage  = makeflow_alloc_unit_create(size);
	a->parent = parent;
	a->residuals = list_create();
	a->locked = locked;

	printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" CREATE \n",
		a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);

	return a;
}

struct makeflow_alloc * makeflow_alloc_traverse_to_node(struct makeflow_alloc *a, struct dag_node *n)
{
	struct dag_node *node;
	struct makeflow_alloc *alloc1, *alloc2, *tmp;

	alloc1 = a;
	printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\n",
		a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);
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
			printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\n",
				alloc1->nodeid, alloc1->storage->total, alloc1->storage->commit,
				alloc1->storage->used, alloc1->storage->free);
		}else{
			break;
		}

		list_next_item(n->residual_nodes);
	}
	return alloc1;
}
/*
int makeflow_alloc_viable_space( struct makeflow_alloc *a, struct dag_node *n)
{
	if(!a)
		return 0;

	struct makeflow_alloc *alloc1, *alloc2;
	list_first_item(n->residual_nodes);
	struct dag_node *node = list_next_item(n->residual_nodes);
	uint64_t space;
	if(!node)
		return a->storage->free;

	alloc1 = a;
	space = alloc1->storage->free;
	list_first_item(alloc1->residuals);
	while((alloc2 = list_next_item(alloc1->residuals)) && node){
		if(alloc2->nodeid == node->nodeid){
			alloc1 = alloc2;
			list_first_item(alloc1->residuals);
			node = list_next_item(n->residual_nodes);
			space += alloc1->storage->free;
		}
	}

	return space;
}

int makeflow_alloc_res_accounted( struct makeflow_alloc *a, struct dag_node *n)
{
	if(!a)
		return 0;

	list_first_item(n->residual_nodes);
	struct dag_node *node = list_next_item(n->residual_nodes);
	struct makeflow_alloc *alloc1, *alloc2;

	alloc1 = a;
	list_first_item(alloc1->residuals);
	while(node && (alloc2 = list_next_item(alloc1->residuals))){
		if(n->nodeid == alloc2->nodeid)
			break;
		if(alloc2->nodeid == node->nodeid){
			alloc1 = alloc2;
			list_first_item(alloc1->residuals);
			node = list_next_item(n->residual_nodes);
			continue;
		}
	}

	struct dag_node *tmp = list_pop_tail(n->residual_nodes);
	node = list_peek_tail(n->residual_nodes);
	list_push_tail(n->residual_nodes, tmp);
	if ((alloc1->nodeid == -1 && !node) ||
		(alloc1 && node && (alloc1->nodeid == node->nodeid)))
			return 1;
	return 0;
}



int makeflow_alloc_check_node_size( struct makeflow_alloc *a, struct dag_node *n)
{
	if(!a->locked){
		return 1;
	}

	struct makeflow_alloc *alloc1 = makeflow_alloc_traverse_to_node(a, n);
	if(alloc1 && alloc1->nodeid == n->nodeid)
		return 1;

	uint64_t result = makeflow_alloc_viable_space(a, n);
	struct dag_node *node = list_peek_tail(n->residual_nodes);
	if(result >= node->footprint_min_size){
		return 1;
	}

	int res_accounted = makeflow_alloc_res_accounted(a, n);
	if(res_accounted && (result >= n->target_size)){
		return 1;
	}
	return 0;
}
*/

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
		printf("\t%"PRIu64"\t%"PRIu64"\n", inc, needed);
		if(tmp->storage->free >= needed || // Fits in the parents free space
				(makeflow_alloc_try_grow_alloc(tmp, needed))){  // Parent can grow to fit us
			return 1;
		}
	}


	return 0;
}

int makeflow_alloc_check_space( struct makeflow_alloc *a, struct dag_node *n)
{
	if(!a)
		return 0;
	printf("\n\n\nCheck\n");

	struct dag_node *node1;
	struct makeflow_alloc *alloc1, *alloc2;

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid == n->nodeid){
		if(alloc1->storage->free < n->target_size)
			return 0;

		return 1;
	}

	while((node1 = list_peek_current(n->residual_nodes))){
		alloc2 = makeflow_alloc_create(node1->nodeid, alloc1, 0, 0);
		if(!makeflow_alloc_try_grow_alloc(alloc2, node1->footprint_min_size))
			return 0;

		list_next_item(n->residual_nodes);
	}

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
		printf("\t%"PRIu64"\t%"PRIu64"\n", inc, needed);
		if(tmp->storage->free >= needed || // Fits in the parents free space
				(makeflow_alloc_grow_alloc(tmp, needed))){  // Parent can grow to fit us
			tmp->storage->commit += needed;
			tmp->storage->free   -= needed;
			a->storage->total    += needed;
			a->storage->free     += needed;
			printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" GROW\n",
				tmp->nodeid, tmp->storage->total, tmp->storage->commit, tmp->storage->used, tmp->storage->free);
			printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" GROW\n\n",
				a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);
			return 1;
		}
	}


	return 0;
}

int makeflow_alloc_commit_space( struct makeflow_alloc *a, struct dag_node *n)
{
	if(!a)
		return 0;
	printf("\n\n\nCommit\n");

	struct dag_node *node1;
	struct makeflow_alloc *alloc1, *alloc2;

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid == n->nodeid){
		if(alloc1->storage->free < n->target_size)
			return 0;
		alloc1->storage->commit += n->target_size;
		alloc1->storage->free   -= n->target_size;

		printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" COM\n",
			alloc1->nodeid, alloc1->storage->total, alloc1->storage->commit,
			alloc1->storage->used, alloc1->storage->free);

		return 1;
	}

	while((node1 = list_peek_current(n->residual_nodes))){
		alloc2 = makeflow_alloc_create(node1->nodeid, alloc1, 0, 0);
		if(!makeflow_alloc_grow_alloc(alloc2, node1->footprint_min_size))
			return 0;
		list_push_tail(alloc1->residuals, alloc2);
		alloc1 = alloc2;

		list_next_item(n->residual_nodes);
	}

	alloc1->storage->commit += n->target_size;
	alloc1->storage->free   -= n->target_size;

	printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" COM\n",
		alloc1->nodeid, alloc1->storage->total, alloc1->storage->commit,
		alloc1->storage->used, alloc1->storage->free);

	printf("\n\n");

	return 1;
}

int makeflow_alloc_use_space( struct makeflow_alloc *a, struct dag_node *n)
{

	printf("\n\n\nUse\n");

	uint64_t inc = dag_node_file_list_size(n->target_files);
	a = makeflow_alloc_traverse_to_node(a, n);

	if(inc > a->storage->commit){
		uint64_t needed = inc - a->storage->commit;
		if(!makeflow_alloc_grow_alloc(a, needed))
			return 0;
		a->storage->commit += needed;
	}

	while(a){
		a->storage->used   += inc;
		a->storage->commit -= inc;
		printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" USE\n",
			a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);
		a = a->parent;
	}

	return 1;
}

int makeflow_alloc_shrink_alloc( struct makeflow_alloc *a, uint64_t dec, makeflow_alloc_release release)
{
	if(!a)
		return 0;

	if(release == MAKEFLOW_ALLOC_USED){
		a->storage->used  -= dec;
		a->storage->total -= dec;
		printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" S USE\n",
			a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);
		while(a->parent){
			a = a->parent;
			a->storage->used   -= dec;
			a->storage->commit += dec;
			printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" S USE\n",
				a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);
		}
	} else {
		dec = a->storage->commit;
		a->storage->commit = 0;
		dec += a->storage->free;
		a->storage->free   = 0;
		a->storage->total -= dec;
		printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" S COM\n",
			a->nodeid, a->storage->total, a->storage->commit, a->storage->used, a->storage->free);

		if(a->parent){
			a->parent->storage->commit -= dec;
			a->parent->storage->free   += dec;
			printf("%d\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64"\t: %"PRIu64" S COM\n",
				a->parent->nodeid, a->parent->storage->total, a->parent->storage->commit,
				a->parent->storage->used, a->parent->storage->free);
		}
	}
	return 1;
}


int makeflow_alloc_release_space( struct makeflow_alloc *a, struct dag_node *n, uint64_t size, makeflow_alloc_release release)
{
	struct makeflow_alloc *alloc1;

	printf("\n\n\nShrink\n");

	alloc1 = makeflow_alloc_traverse_to_node(a, n);

	if(alloc1->nodeid != n->nodeid)
		return 0;

	makeflow_alloc_shrink_alloc(alloc1, size, release);

	if(alloc1->storage->total == 0){
		// Delete allocations that are empty
	}

	return 1;
}
