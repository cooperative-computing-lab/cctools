#ifndef WORK_QUEUE_SANDBOX_H
#define WORK_QUEUE_SANDBOX_H

#include "work_queue_process.h"
#include "work_queue_cache.h"

int work_queue_sandbox_stagein( struct work_queue_process *p, struct work_queue_cache *c );
int work_queue_sandbox_stageout( struct work_queue_process *p, struct work_queue_cache *c );

#endif
