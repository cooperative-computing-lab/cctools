#ifndef DS_PERF_LOG_H
#define DS_PERF_LOG_H

#include "ds_manager.h"

void ds_perf_log_write_header( struct ds_manager *q );
void ds_perf_log_write_update( struct ds_manager *q, int force );

#endif

