#ifndef DATASWARM_CLIENT_OPS_H
#define DATASWARM_CLIENT_OPS_H

#include "jx.h"

//task operations
char *dataswarm_submit_task(struct jx *task);
struct jx *dataswarm_delete_task(char *uuid);
struct jx *dataswarm_retrieve_task(char *uuid);

//file operations
char *dataswarm_declare_file(struct jx *json);
struct jx *dataswarm_commit_file(char *uuid);
struct jx *dataswarm_delete_file(char *uuid);
struct jx *dataswarm_copy_file(char *uuid);

//service operations
char *dataswarm_submit_service(struct jx *service);
struct jx *dataswarm_delete_service(char *uuid);

//project operations
char *dataswarm_create_project(char *project_name);
struct jx *dataswarm_delete_project(char *uuid);

//other operations
struct jx *dataswarm_wait();
bool dataswarm_queue_empty();
struct jx *dataswarm_status(char *uuid);

#endif
