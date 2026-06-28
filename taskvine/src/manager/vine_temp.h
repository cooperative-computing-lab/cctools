#ifndef vine_temp_H
#define vine_temp_H

#include "vine_manager.h"

/** Check if a temporary file exists somewhere in all workers. */
int vine_temp_exists_somewhere(struct vine_manager *q, struct vine_file *f);

/** Queue a temporary file for replication. */
int vine_temp_queue_for_replication(struct vine_manager *q, struct vine_file *f);

/** Start replication for temporary files. */
int vine_temp_start_replication(struct vine_manager *q);

/** Clean redundant replicas of a temporary file. */
void vine_temp_clean_redundant_replicas(struct vine_manager *q, struct vine_file *f);

/** Shift a temp file replica away from the worker using the most cache space. */
void vine_temp_shift_disk_load(struct vine_manager *q, struct vine_worker_info *source_worker, struct vine_file *f);

#endif