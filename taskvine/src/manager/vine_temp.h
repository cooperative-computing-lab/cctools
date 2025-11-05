#ifndef vine_temp_H
#define vine_temp_H

#include "vine_manager.h"

/** Replication related functions */
int vine_temp_queue_for_replication(struct vine_manager *q, struct vine_file *f);
int vine_temp_start_replication(struct vine_manager *q);
int vine_temp_handle_lost_replica(struct vine_manager *q, char *cachename);

/** Storage management functions */
void vine_temp_clean_redundant_replicas(struct vine_manager *q, struct vine_file *f);
void vine_temp_shift_disk_load(struct vine_manager *q, struct vine_worker_info *source_worker, struct vine_file *f);

#endif