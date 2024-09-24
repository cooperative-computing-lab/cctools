/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_GET_H
#define VINE_MANAGER_GET_H

/*
Provides the recursive transfer of files and directories
from the worker back to the manager at task completion.
This is the counterpart of worker/vine_transfer.c on the worker side.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_mount.h"

vine_result_code_t vine_manager_get_single_file( struct vine_manager *q, struct vine_worker_info *w, struct vine_file *f );
vine_result_code_t vine_manager_get_output_file( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_mount *m, struct vine_file *f );
vine_result_code_t vine_manager_get_output_files( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t );
vine_result_code_t vine_manager_get_stdout(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
vine_result_code_t vine_manager_get_monitor_output_file( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t );

#endif

