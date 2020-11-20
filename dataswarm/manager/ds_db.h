#ifndef DS_DB_H
#define DS_DB_H

#include "ds_manager.h"

/*
Implements a trivial persistent database for tasks and files.
At startup, all tasks and files are read into the manager hash tables.
When a task or file is modified, call ds_db_commit_{task|file} to
force its storage to local disk.
*/


void ds_db_commit_task( struct ds_manager *m, const char *taskid );
void ds_db_commit_file( struct ds_manager *m, const char *fileid );

void ds_db_recover_all( struct ds_manager *m );

#endif
