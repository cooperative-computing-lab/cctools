/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This module tracks how and array of processing units (cpus, gpus, dpus)
is assigned to specific tasks, so that we can indicate to running tasks
exactly which processing units should be used.
*/

#ifndef VINE_XPU_TRACKER_H
#define VINE_XPU_TRACKER_H

struct vine_xpu_tracker * vine_xpu_tracker_create( const char *name, int count );
void vine_xpu_tracker_debug( struct vine_xpu_tracker *x );
void vine_xpu_tracker_alloc( struct vine_xpu_tracker *x, int n, int taskid );
void vine_xpu_tracker_free( struct vine_xpu_tracker *x, int taskid );
char * vine_xpu_tracker_to_string( struct vine_xpu_tracker *x, int taskid );

#endif
