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

typedef enum {
	MAKEFLOW_CLEAN_NONE,          /* Clean nothing, default. */
	MAKEFLOW_CLEAN_INTERMEDIATES, /* Clean only intermediate files. */
	MAKEFLOW_CLEAN_OUTPUTS,       /* Clean only output files. */
	MAKEFLOW_CLEAN_ALL            /* Clean all created files and logs. */
} makeflow_clean_depth;

void makeflow_parse_input_outputs( struct dag *d );
void makeflow_gc( struct dag *d, struct batch_queue *queue, makeflow_gc_method_t method, int count );
int makeflow_file_clean( struct dag *d, struct batch_queue *queue, struct dag_file *f, int silent );
void makeflow_sandbox_delete( struct dag *d, const char *sandbox_name );
void makeflow_clean_node( struct dag *d, struct batch_queue *queue, struct dag_node *n, int silent );
void makeflow_clean( struct dag *d, struct batch_queue *queue, makeflow_clean_depth clean_depth);

#endif
