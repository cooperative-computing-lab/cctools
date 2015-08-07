/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MAKEFLOW_LOG_H
#define MAKEFLOW_LOG_H

#include "dag.h"
#include "timestamp.h"

/*
Each dag is associated with a log file that records each operation that
moves the workload forward.  As a node changes state, an event is written
to the log.  Upon recovery from a crash, makeflow_log_recover plays back
the state to recover the dag.
*/

void makeflow_log_started_event( struct dag *d );
void makeflow_log_aborted_event( struct dag *d );
void makeflow_log_failed_event( struct dag *d );
void makeflow_log_completed_event( struct dag *d );
void makeflow_log_state_change( struct dag *d, struct dag_node *n, int newstate );
void makeflow_log_file_state_change( struct dag *d, struct dag_file *f, int newstate );
void makeflow_log_gc_event( struct dag *d, int collected, timestamp_t elapsed, int total_collected );
void makeflow_log_recover( struct dag *d, const char *filename, int verbose_mode, struct batch_queue *queue );

#endif
