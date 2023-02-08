/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TXN_LOG_H
#define VINE_TXN_LOG_H

/*
Implementation of the manager's transaction log,
which records details of every major event in the system:
task execution, file transfer, etc. for later analysis.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"

void vine_txn_log_write_header( struct vine_manager *q );
void vine_txn_log_write_manager(struct vine_manager *q, const char *str);
void vine_txn_log_write_task(struct vine_manager *q, struct vine_task *t);
void vine_txn_log_write_category(struct vine_manager *q, struct category *c);
void vine_txn_log_write_worker(struct vine_manager *q, struct vine_worker_info *w, int leaving, vine_worker_disconnect_reason_t reason_leaving);
void vine_txn_log_write_transfer(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *f, size_t size_in_bytes, int time_in_usecs, int is_input );
void vine_txn_log_write_cache_update(struct vine_manager *q, struct vine_worker_info *w, size_t size_in_bytes, int time_in_usecs, const char *name );
void vine_txn_log_write_worker_resources(struct vine_manager *q, struct vine_worker_info *w);

#endif

