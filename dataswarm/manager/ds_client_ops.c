/*
   Copyright (C) 2020- The University of Notre Dame
   This software is distributed under the GNU General Public License.
   See the file COPYING for details.
   */

#include "ds_client_ops.h"
#include "uuid.h"
#include "ds_validate.h"

#include <assert.h>
#include <string.h>

ds_result_t ds_client_task_submit(struct ds_manager *m, struct ds_client_rep *c, struct jx *task, struct jx **result) {
    /*
    if(!validate_json(task, SUBMIT_TASK)){
        return DS_RESULT_BAD_PARAMS;
    }
    */

    // assign a UUID to the task
    cctools_uuid_t uuid;
    cctools_uuid_create(&uuid);

    char *uuid_str = strdup(uuid.str);
    *result = jx_string(uuid_str);

    //add state and uuid to task
    jx_insert_string(task, "task-id", uuid_str);

    struct ds_task *t = ds_task_create(task);
    set_insert(t->subscribers, c);

    //save UUID to task mapping in memory
    hash_table_insert(m->task_table, uuid_str, t);

    return DS_RESULT_SUCCESS;
}

struct ds_task *ds_client_task_delete(struct ds_manager *m, const char *uuid) {

    return hash_table_remove(m->task_table, uuid);

}

struct jx *ds_client_task_retrieve(struct ds_manager *m, const char *uuid) {

    struct ds_task *t = hash_table_lookup(m->task_table, uuid);
    return ds_task_to_jx(t);

}

struct ds_file *ds_client_file_declare(struct ds_manager *m, struct jx *params) {
    //validate json
    if(!validate_json(params, DECLARE_FILE)){
        return NULL;
    }

    // assign a UUID to the file
    cctools_uuid_t uuid;
    cctools_uuid_create(&uuid);

    struct ds_file *f = ds_file_create(
            uuid.str,
            jx_lookup_string(params, "project"),
            jx_lookup_integer(params, "size"),
            jx_lookup(params, "metadata"));

    //save UUID to file mapping in memory
    hash_table_insert(m->file_table, uuid.str, f);

    return f;
}

struct ds_file *ds_client_file_commit(struct ds_manager *m, const char *uuid) {

    //get file metadata from mapping
    struct ds_file *f = hash_table_lookup(m->file_table, uuid);

    //TODO: change all blobs to RO

    //TODO: change state to immutable

    return f;

}

struct ds_file *ds_client_file_delete(struct ds_manager *m, const char *uuid) {

    return hash_table_remove(m->file_table, uuid);

}

struct ds_file *ds_client_file_copy(struct ds_manager *m, const char *uuid) {

    //get file data from mapping
    struct ds_file *f = hash_table_lookup(m->file_table, uuid);

    //TODO: replicate file data to mapping
    return f;
}

char *ds_client_service_submit(struct ds_manager *m, struct jx *service){

    if(!validate_json(service, SUBMIT_SERVICE)){
        return NULL;
    }

    // assign a UUID to the service
    cctools_uuid_t *uuid = 0;
    cctools_uuid_create(uuid);

    char * uuid_str = strdup(uuid->str);

    //add state and uuid to service
    jx_insert_string(service, "uuid", uuid_str);

    //TODO: save UUID to file mapping in memory

    return uuid_str;
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

void ds_client_wait(struct ds_manager *m, struct ds_client_rep *c, jx_int_t msgid, struct jx *params) {
    assert(!c->waiting); // not sure what to do here??
    c->waiting = true;
    c->wait_id = msgid;
    ds_client_rep_flush_notifications(c);
}

int ds_client_queue_empty(struct ds_manager *m, struct jx *params) {

    //TODO: return true if the queue of tasks is empty

    return 0;

}

struct jx *ds_client_status(struct ds_manager *m, struct jx *params) {

    return NULL;

}
