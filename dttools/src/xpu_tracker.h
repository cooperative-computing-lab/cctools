/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This module tracks how and array of processing units (cpus, gpus, dpus)
is assigned to specific tasks, so that we can indicate to running tasks
exactly which processing units should be used.
*/

#ifndef XPU_TRACKER_H
#define XPU_TRACKER_H

struct xpu_tracker * xpu_tracker_create( const char *name, int count );
void xpu_tracker_debug( struct xpu_tracker *x );
void xpu_tracker_alloc( struct xpu_tracker *x, int n, int taskid );
void xpu_tracker_free( struct xpu_tracker *x, int taskid );
char * xpu_tracker_to_string( struct xpu_tracker *x, int taskid );

#endif
