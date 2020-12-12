#ifndef DATASWARM_CLIENT_OPS_H
#define DATASWARM_CLIENT_OPS_H

#include "ds_task.h"
#include "ds_file.h"
#include "ds_manager.h"
#include "ds_client_rep.h"

//task operations
ds_result_t ds_client_task_submit(struct ds_manager *m,  struct ds_client_rep *c, struct jx *task, struct jx **result);
struct ds_task *ds_client_task_delete(struct ds_manager *m, const char *uuid);
struct jx *ds_client_task_retrieve(struct ds_manager *m, const char *uuid);

//file operations
struct ds_file *ds_client_file_declare(struct ds_manager *m, struct jx *file);
struct ds_file *ds_client_file_commit(struct ds_manager *m, const char *uuid);
struct ds_file *ds_client_file_delete(struct ds_manager *m, const char *uuid);
struct ds_file *ds_client_file_copy(struct ds_manager *m, const char *uuid);

//service operations
char *ds_client_service_submit(struct ds_manager *m, struct jx *service);
struct jx *ds_client_service_delete(struct ds_manager *m, struct jx *service);

//project operations
char *ds_client_project_create(struct ds_manager *m, struct jx *project);
struct jx *ds_client_project_delete(struct ds_manager *m, struct jx *project);

//other operations
void ds_client_wait(struct ds_manager *m, struct ds_client_rep *c, jx_int_t msgid, struct jx *params);
int ds_client_queue_empty(struct ds_manager *m, struct jx *params);
struct jx *ds_client_status(struct ds_manager *m, struct jx *params);

#endif
