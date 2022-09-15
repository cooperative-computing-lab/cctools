/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_MANAGER_GET_H
#define DS_MANAGER_GET_H

/*
Provides the recursive transfer of files and directories
from the worker back to the manager at task completion.
This is the counterpart of worker/ds_transfer.c on the worker side.
This module is private to the manager and should not be invoked by the end user.
*/

#include "ds_manager.h"

ds_result_code_t ds_manager_get_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f );
ds_result_code_t ds_manager_get_output_files( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t );
ds_result_code_t ds_manager_get_monitor_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t );

#endif

