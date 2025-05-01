#ifndef VINE_REDUNDANCY_H
#define VINE_REDUNDANCY_H

#include "debug.h"
#include "macros.h"
#include "vine_file.h"
#include "vine_manager.h"
#include "priority_queue.h"
#include "vine_worker_info.h"
#include "stringtools.h"
#include "vine_worker_info.h"
#include "vine_manager_put.h"
#include "vine_mount.h"
#include "vine_file_replica_table.h"
#include "vine_task.h"

#include <float.h>
#include <assert.h>

int vine_redundancy_handle_file_pruning(struct vine_manager *q, struct vine_file *f);
int vine_redundancy_handle_worker_removal(struct vine_manager *q, struct vine_worker_info *w);
int vine_redundancy_handle_task_completion(struct vine_manager *q, struct vine_task *t);
int vine_redundancy_handle_cache_update(struct vine_manager *q, struct vine_file *f);
int vine_redundancy_process_temp_files(struct vine_manager *q);

#endif /* VINE_REDUNDANCY_H */
