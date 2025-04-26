#ifndef VINE_CHECKPOINT_H
#define VINE_CHECKPOINT_H

#include "debug.h"
#include "macros.h"
#include "vine_file.h"
#include "vine_manager.h"
#include "priority_queue.h"
#include "vine_worker_info.h"
#include "priority_queue.h"
#include "stringtools.h"
#include "vine_worker_info.h"
#include "vine_manager_put.h"
#include "vine_mount.h"
#include "vine_file_replica_table.h"

#include <float.h>
#include <assert.h>


int vine_redundancy_process_temp_files(struct vine_manager *q);

#endif /* VINE_CHECKPOINT_H */
