/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_PUT_H
#define VINE_MANAGER_PUT_H

/*
Provides the recursive transfer of files and directories
from the manager to the worker prior to task execution.
This is the counterpart of worker/vine_transfer.c on the worker side.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"

vine_result_code_t vine_manager_put_input_files( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t );
vine_result_code_t vine_manager_put_task( struct vine_manager *m, struct vine_worker_info *w, struct vine_task *t, const char *command_line, struct rmsummary *limits, struct vine_file *target );
vine_result_code_t vine_manager_put_url_now( struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, struct vine_file *f );

#endif

