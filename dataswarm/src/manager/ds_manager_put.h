/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_MANAGER_PUT_H
#define DS_MANAGER_PUT_H

/*
Provides the recursive transfer of files and directories
from the manager to the worker prior to task execution.
This is the counterpart of worker/ds_transfer.c on the worker side.
This module is private to the manager and should not be invoked by the end user.
*/

#include "ds_manager.h"

ds_result_code_t ds_manager_put_input_files( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t );

#endif

