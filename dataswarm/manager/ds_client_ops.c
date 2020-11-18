/*
   Copyright (C) 2020- The University of Notre Dame
   This software is distributed under the GNU General Public License.
   See the file COPYING for details.
   */

#include "ds_client_ops.h"
#include "uuid.h"
#include "ds_validate.h"
#include "ds_ids.h"
#include "ds_db.h"

#include <string.h>

char *ds_client_task_submit(struct ds_manager *m, struct jx *task)
{
    if(!validate_json(task, SUBMIT_TASK)){
        return NULL;
    }

    char *taskid = ds_create_taskid(m);

    jx_insert_string(task, "task-id", taskid);

    struct ds_task *t = ds_task_create(task);

    hash_table_insert(m->task_table, taskid, t);
    ds_db_commit_task(m,taskid);

    return taskid;
}

struct ds_task *ds_client_task_delete(struct ds_manager *m, const char *uuid)
{
	struct ds_task *t = hash_table_lookup(m->task_table, uuid);
	t->state = DS_TASK_DELETING;
	ds_db_commit_task(m,uuid);
	return t;
}

struct jx *ds_client_task_retrieve(struct ds_manager *m, const char *uuid)
{
	struct ds_task *t = hash_table_lookup(m->task_table, uuid);
	return ds_task_to_jx(t);
}

struct ds_file *ds_client_file_declare(struct ds_manager *m, struct jx *params)
{
    if(!validate_json(params, DECLARE_FILE)){
        return NULL;
    }

    char *fileid = ds_create_fileid(m);

    struct ds_file *f = ds_file_create(
            fileid,
            jx_lookup_string(params, "project"),
            jx_lookup_integer(params, "size"),
            jx_lookup(params, "metadata"));

    hash_table_insert(m->file_table, fileid, f);
    ds_db_commit_file(m,fileid);

    return f;
}

struct ds_file *ds_client_file_commit(struct ds_manager *m, const char *uuid) {

    //get file metadata from mapping
    struct ds_file *f = hash_table_lookup(m->file_table, uuid);

    //TODO: change all blobs to RO

    //TODO: change state to immutable

    return f;

}

struct ds_file *ds_client_file_delete(struct ds_manager *m, const char *uuid)
{
	struct ds_file *f = hash_table_lookup(m->file_table,uuid);
	f->state = DS_FILE_DELETING;
	ds_db_commit_file(m,uuid);
	return f; // Why are we returning the file here?
}

struct ds_file *ds_client_file_copy(struct ds_manager *m, const char *uuid)
{
    //get file data from mapping
    struct ds_file *f = hash_table_lookup(m->file_table, uuid);

    //TODO: replicate file data to mapping
    return f;
}

char *ds_client_service_submit(struct ds_manager *m, struct jx *service){

    if(!validate_json(service, SUBMIT_SERVICE)){
        return NULL;
    }

    char *serviceid = ds_create_serviceid(m);

    jx_insert_string(service, "uuid", serviceid);

    //TODO: save UUID to file mapping in memory

    return serviceid;;
}

struct jx *ds_client_service_delete(struct ds_manager *m, struct jx *params) {

    //TODO: get service data from mapping

    //TODO: remove service data from mapping
    return NULL;
}

char *ds_client_project_create(struct ds_manager *m, struct jx *params) {

    // assign a UUID to the project
    cctools_uuid_t *uuid = 0;
    cctools_uuid_create(uuid);

    char * uuid_str = strdup(uuid->str);

    //TODO: create jx with project_name and uuid

    //TODO: save UUID to file mapping in memory

    //return task UUID
    return uuid_str;

}

struct jx *ds_client_project_delete(struct ds_manager *m, struct jx *params) {

    //TODO: get project data from mapping

    //TODO: remove project data from mapping

    return NULL;

}

struct jx *ds_client_wait(struct ds_manager *m, struct jx *params) {

    //TODO: block until something happens

    return NULL;

}

int ds_client_queue_empty(struct ds_manager *m, struct jx *params) {

    //TODO: return true if the queue of tasks is empty

    return 0;

}

struct jx *ds_client_status(struct ds_manager *m, struct jx *params) {

    return NULL;

}
