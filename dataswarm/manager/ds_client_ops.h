#ifndef DATASWARM_CLIENT_OPS_H
#define DATASWARM_CLIENT_OPS_H

#include "jx.h"
#include "common/ds_task.h"
#include "ds_file.h"
#include "ds_manager.h"


//task operations
char *ds_submit_task(struct jx *task, struct ds_manager *m);
struct ds_task *ds_delete_task(const char *uuid, struct ds_manager *m);
struct jx *ds_retrieve_task(const char *uuid, struct ds_manager *m);

//file operations
char *ds_declare_file(struct jx *json, struct ds_manager *m);
struct ds_file *ds_commit_file(const char *uuid, struct ds_manager *m);
struct ds_file *ds_delete_file(const char *uuid, struct ds_manager *m);
struct ds_file *ds_copy_file(const char *uuid, struct ds_manager *m);

//service operations
char *ds_submit_service(struct jx *service);
struct jx *ds_delete_service(char *uuid);

//project operations
char *ds_create_project(char *project_name);
struct jx *ds_delete_project(char *uuid);

//other operations
struct jx *ds_wait();
int ds_queue_empty();
struct jx *ds_status(char *uuid);

#endif
