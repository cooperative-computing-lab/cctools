/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_PERF_LOG_H
#define VINE_PERF_LOG_H

/*
Implementation of the manager's performance log, which records numbers
of tasks, workers, etc present over time.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"

#define VINE_PERF_LOG_INTERVAL 5

void vine_perf_log_write_header( struct vine_manager *q );
void vine_perf_log_write_update( struct vine_manager *q, int force );

#endif

