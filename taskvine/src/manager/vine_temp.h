#ifndef vine_temp_H
#define vine_temp_H

#include "vine_manager.h"

int vine_temp_replicate_file_later(struct vine_manager *q, struct vine_file *f);
int vine_temp_rescue_lost_replica(struct vine_manager *q, char *cachename);
int vine_temp_start_replication(struct vine_manager *q);

void vine_temp_clean_redundant_replicas(struct vine_manager *q, struct vine_file *f);
void vine_temp_shift_disk_load(struct vine_manager *q, struct vine_worker_info *source_worker, struct vine_file *f);

#endif