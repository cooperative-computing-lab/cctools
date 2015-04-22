/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_LOG_H
#define DAG_LOG_H

#include "dag.h"

/*
Each dag is associated with a log file that records each operation that
moves the workload forward.  As a node changes state, an event is written
to the log.  Upon recovery from a crash, dag_log_recover plays back
the state to recover the dag.
*/

void dag_log_state_change( struct dag *d, struct dag_node *n, int newstate );

void dag_log_recover( struct dag *d, const char *filename, int verbose_mode );

#endif

