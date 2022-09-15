/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_PERF_LOG_H
#define DS_PERF_LOG_H

/*
Implementation of the manager's performance log, which records numbers
of tasks, workers, etc present over time.
This module is private to the manager and should not be invoked by the end user.
*/

#include "ds_manager.h"

void ds_perf_log_write_header( struct ds_manager *q );
void ds_perf_log_write_update( struct ds_manager *q, int force );

#endif

