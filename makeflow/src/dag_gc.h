/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_GC_H
#define DAG_GC_H

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
*/

typedef enum {
	DAG_GC_NONE,		/* Do no garbage collection. */
	DAG_GC_REF_COUNT,	/* Remove files as soon as the reference count falls to zero. */
	DAG_GC_ON_DEMAND,	/* Remove files when available storage is low. */
	DAG_GC_FORCE		/* Remove all collectable files right now. */
} dag_gc_method_t;

void dag_gc_prepare( struct dag *d );
void dag_gc( struct dag *d, dag_gc_method_t method, int count );

#endif

