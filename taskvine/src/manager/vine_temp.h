#ifndef vine_temp_H
#define vine_temp_H

#include "vine_manager.h"

int is_checkpoint_worker(struct vine_manager *q, struct vine_worker_info *w);
int vine_temp_replicate_file_now(struct vine_manager *q, struct vine_file *f);
int vine_temp_replicate_file_later(struct vine_manager *q, struct vine_file *f);
int vine_temp_checkpoint_file_later(struct vine_manager *q, struct vine_file *f);
int vine_temp_handle_file_lost(struct vine_manager *q, char *cachename);
int vine_temp_start_replication(struct vine_manager *q);
int vine_temp_start_checkpointing(struct vine_manager *q);
void vine_temp_start_peer_transfer(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source_worker, struct vine_worker_info *dest_worker);
int file_has_been_checkpointed(struct vine_manager *q, struct vine_file *f);

#endif