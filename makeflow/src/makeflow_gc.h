/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_GC_H
#define MAKEFLOW_GC_H

#include "makeflow_hook.h"

/*
This module implements garbage collection on the dag.
Files that are no longer needed as inputs to any rules
may be removed, according to a variety of criteria.
*/

typedef enum {
	MAKEFLOW_GC_NONE,       /* Do no garbage collection. */
	MAKEFLOW_GC_COUNT,      /* If existing files > count, remove all available files as soon as the reference count falls to zero. */
	MAKEFLOW_GC_ON_DEMAND,  /* Remove COUNT files as soon as the reference count falls to zero. */
	MAKEFLOW_GC_SIZE,       /* Remove COUNT files when available storage is below SIZE. */
	MAKEFLOW_GC_ALL         /* Remove all collectable files right now. */
} makeflow_gc_method_t;

typedef enum {
	MAKEFLOW_CLEAN_NONE,          /* Clean nothing, default. */
	MAKEFLOW_CLEAN_INTERMEDIATES, /* Clean only intermediate files. */
	MAKEFLOW_CLEAN_OUTPUTS,       /* Clean only output files. */
	MAKEFLOW_CLEAN_CACHE,         /* Clean the dependency cache and the links pointing to it. */
	MAKEFLOW_CLEAN_ALL            /* Clean all created files and logs. */
} makeflow_clean_depth;

void makeflow_parse_input_outputs( struct dag *d );
void makeflow_gc( struct dag *d, struct batch_queue *queue, makeflow_gc_method_t method, uint64_t size, int count );
int  makeflow_clean_file( struct dag *d, struct batch_queue *queue, struct dag_file *f );
void makeflow_clean_node( struct dag *d, struct batch_queue *queue, struct dag_node *n );

/* return 0 on success; return non-zero on failure. */
int makeflow_clean( struct dag *d, struct batch_queue *queue, makeflow_clean_depth clean_depth );

/* makeflow_clean_mount_target removes the target.
 * @param target: a file path
 * return 0 on success, -1 on failure.
 */
int makeflow_clean_mount_target(const char *target);

#endif
