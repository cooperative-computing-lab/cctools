/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_NODE_FOOTPRINT_H
#define DAG_NODE_FOOTPRINT_H

#include "batch_queue.h"
#include "dag_node.h"
#include "category.h"
#include "set.h"
#include "hash_table.h"
#include "itable.h"


typedef enum {
	DAG_NODE_FOOTPRINT_RUN = 0,
	DAG_NODE_FOOTPRINT_DELETE,
	DAG_NODE_FOOTPRINT_DESC
} dag_node_footprint_t;

/* struct dag_node_footprint is a used to measure and record the footprint 
 * associated with a dag_node. The footprint comes in different forms based
 * on the type of "look ahead"
*/

struct dag_node_footprint {
	struct set *direct_children;	/* The nodes of which this node is an immediate ancestor
										and no descendant of mine is also its parent */
	struct set *accounted;			/* The nodes of which this node is an immediate descendant */

	uint64_t source_size;			/* size of dag_files of the node's requirements */
	uint64_t target_size;			/* size of dag_files of the node's productions */

	struct set *terminal_files;		/* set of dag_files that exist until the end of the Makeflow */
	struct set *coexist_files;		/* set of dag_files that exist until the end of the Makeflow */

	struct list *residual_nodes;	/* list of dag_nodes that describe residual sybc */
	struct set *residual_files;		/* set of dag_files of the node's residual */
	uint64_t residual_size;			/* Size of current residual, changes depending on
										context of requesting node. */

	struct set *run_files;			/* size of dag_files of my output's and my parents' */
	uint64_t run_footprint;			/* size of dag_files of my output's and my parents' */

	struct set *delete_files;		/* size of dag_files of my output's and my child's */
	uint64_t delete_footprint;		/* size of dag_files of my output's and my child's */
	struct list *delete_run_order;			/* list of child and the order to maintain committed size */

	struct set *prog_min_files;		/* Set of nodes that define the min footprint */
	uint64_t prog_min_footprint;	/* Size of the minimum defined footprint */

	struct set *prog_max_files;		/* Set of nodes that define the max footprint */
	uint64_t prog_max_footprint;	/* Size of the largest defined footprint */
	struct list *prog_run_order;			/* list of child and the order to maintain committed size */

	struct set *footprint_min_files;/* Set of nodes that define the min footprint */
	uint64_t footprint_min_size;	/* Size of the minimum defined footprint */
	dag_node_footprint_t footprint_min_type; /* Type that defines which footprint was chosen */

	struct set *footprint_max_files;/* Set of nodes that define the max footprint */
	uint64_t footprint_max_size;	/* Size of the largest defined footprint */
	dag_node_footprint_t footprint_max_type; /* Type that defines which footprint was chosen */

	uint64_t footprint_size;		/* Size decided upon by the user as the footprint between min and max */
	dag_node_footprint_t footprint_type; /* Type that defines which footprint was chosen */

	uint64_t self_res;
	uint64_t res;
	struct set *res_files;
	uint64_t wgt;
	struct set *wgt_files;
	uint64_t max_wgt;
	struct set *max_wgt_files;
	uint64_t diff;

	struct list *run_order;			/* list of child and the order to maintain committed size */
	struct set *dependencies;		/* Set of nodes that need to be active prior to execution for footprint */

	int children_updated;			/* Int indicating this node has updated its direct_children */
	int size_updated;				/* Int indicating this node has updated its size */
	int footprint_updated;			/* Int indicating this node has updated its footprint */
	int terminal_updated;			/* Int indicating this node has updated its terminal_files */
};

/* Defines the set of functions needed to compute a dags footprint.
 * struct dag_node n : should be a node -1 that is a artifical node
 *  that "creates" all starting nodes. Provides a base from which to
 *  measure. */
void dag_node_footprint_calculate(struct dag_node *n);

struct dag_node_footprint *dag_node_footprint_create();
void dag_node_footprint_delete(struct dag_node_footprint *f);

int dag_node_footprint_dependencies_active(struct dag_node *n);

void dag_node_footprint_determine_children(struct dag_node *n);
void dag_node_footprint_prepare_node_terminal_files(struct dag_node *n);
void dag_node_footprint_prepare_node_size(struct dag_node *n);
void dag_node_footprint_find_largest_residual(struct dag_node *n, struct dag_node *limit);
void dag_node_footprint_measure(struct dag_node *n);
void dag_node_footprint_print(struct dag *d, struct dag_node *base, char *output);
void dag_node_footprint_reset(struct dag_node *n);

#endif
