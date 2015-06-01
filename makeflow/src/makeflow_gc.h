/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_GC_H
#define MAKEFLOW_GC_H

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
*/

typedef enum {
	MAKEFLOW_GC_NONE,		/* Do no garbage collection. */
	MAKEFLOW_GC_REF_COUNT,	/* Remove files as soon as the reference count falls to zero. */
	MAKEFLOW_GC_ON_DEMAND,	/* Remove files when available storage is low. */
	MAKEFLOW_GC_FORCE		/* Remove all collectable files right now. */
} makeflow_gc_method_t;

void makeflow_parse_input_outputs( struct dag *d );
void makeflow_gc( struct dag *d, struct batch_queue *queue, makeflow_gc_method_t method, int count );
void makeflow_clean_node( struct dag *d, struct batch_queue *queue, struct dag_node *n);
void makeflow_clean( struct dag *d, struct batch_queue *queue);

#endif
