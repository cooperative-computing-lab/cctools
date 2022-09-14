
#ifndef DS_TRANSACTION_H
#define DS_TRANSACTION_H

#include "ds_manager.h"

void ds_transaction_write_header( struct ds_manager *q );
void ds_transaction_write(struct ds_manager *q, const char *str);
void ds_transaction_write_task(struct ds_manager *q, struct ds_task *t);
void ds_transaction_write_category(struct ds_manager *q, struct category *c);
void ds_transaction_write_worker(struct ds_manager *q, struct ds_worker_info *w, int leaving, ds_worker_disconnect_reason_t reason_leaving);
void ds_transaction_write_transfer(struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f, size_t size_in_bytes, int time_in_usecs, ds_file_type_t type);
void ds_transaction_write_worker_resources(struct ds_manager *q, struct ds_worker_info *w);

#endif

