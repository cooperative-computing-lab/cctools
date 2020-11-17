#ifndef DS_DB_H
#define DS_DB_H

#include "ds_manager.h"

void ds_db_commit_task( struct ds_manager *m, const char *taskid );
void ds_db_commit_file( struct ds_manager *m, const char *fileid );

void ds_db_recover_all( struct ds_manager *m );

#endif
