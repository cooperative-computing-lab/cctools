#ifndef DS_MANAGER_GET_H
#define DS_MANAGER_GET_H

#include "ds_manager.h"

ds_result_code_t ds_manager_get_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f );
ds_result_code_t ds_manager_get_output_files( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t );
ds_result_code_t ds_manager_get_monitor_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t );

#endif

