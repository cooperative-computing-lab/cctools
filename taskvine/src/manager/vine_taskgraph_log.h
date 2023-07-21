/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TASKGRAPH_LOG_H
#define VINE_TASKGRAPH_LOG_H

/*
Implementation of the manager's graph log, which records numbers
of tasks, workers, etc present over time.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"

void vine_taskgraph_log_write_header( struct vine_manager *q );
void vine_taskgraph_log_write_task( struct vine_manager *q, struct vine_task *t );
void vine_taskgraph_log_write_file( struct vine_manager *q, struct vine_file *f );
void vine_taskgraph_log_write_footer( struct vine_manager *q );

#endif

