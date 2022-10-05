/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_TXN_LOG_H
#define DS_TXN_LOG_H

/*
Implementation of the manager's transaction log,
which records details of every major event in the system:
task execution, file transfer, etc. for later analysis.
This module is private to the manager and should not be invoked by the end user.
*/

#include "ds_manager.h"

void ds_txn_log_write_header( struct ds_manager *q );
void ds_txn_log_write(struct ds_manager *q, const char *str);
void ds_txn_log_write_task(struct ds_manager *q, struct ds_task *t);
void ds_txn_log_write_category(struct ds_manager *q, struct category *c);
void ds_txn_log_write_worker(struct ds_manager *q, struct ds_worker_info *w, int leaving, ds_worker_disconnect_reason_t reason_leaving);
void ds_txn_log_write_transfer(struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f, size_t size_in_bytes, int time_in_usecs, int is_input );
void ds_txn_log_write_worker_resources(struct ds_manager *q, struct ds_worker_info *w);

#endif

