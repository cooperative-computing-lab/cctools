/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_client_ops.h"
#include "helpers.h"

#include <string.h>

char *ds_submit_task(struct jx *task){

    if(validate_json(task, SUBMIT_TASK)){
        return NULL;
    }

    char *type = jx_lookup_string(task, "type");
    char *service = jx_lookup_string(task, "service");
    char *project = jx_lookup_string(task, "project");
    struct jx *namespace = jx_lookup(task, "namespace");
    struct jx *resources = jx_lookup(task, "resources");
    struct jx *event = jx_lookup(task, "event");

    //TODO: check values from above
    //check_values(task)

    // assign a UUID to the task
    cctools_uuid_t *uuid;
    cctools_uuid_create(uuid);

    char uuid_str[UUID_LEN+1];
    strcpy(uuid_str, uuid->str);
 
    //add state and uuid to task
    jx_insert_string(task, "uuid", uuid_str);

    //TODO: save UUID to file mapping in memory
   
    //return task UUID
    return uuid_str;

}

struct jx *ds_delete_task(char *uuid){

    //TODO: get task data from mapping

    //TODO: remove task data from mapping

}

struct jx *ds_retrieve_task(char *uuid){

    //TODO: get task data from mapping

    //TODO: return task data

}

char *ds_declare_file(struct jx *file){
    
    char *type = NULL;
    int project = 0;
    char *metadata = NULL;
    
    //validate json
    if(validate_json(file, DECLARE_FILE)){
        return NULL;
    }

    //get file info from jx struct --> type, project, metadata
    void *i = NULL;
    void *j = NULL;
    const char *key = jx_iterate_keys(file, &j);
    struct jx *value = jx_iterate_values(file, &i);

    while(key != NULL){
        
        if(!strcmp(key, "type")){
            type = value->u.string_value;
        } else if(!strcmp(key, "project")){
            project = value->u.integer_value;
        } else if(!strcmp(key, "metadata")){
            metadata = value->u.string_value;
        }

    }

    //TODO:validate values of 'type' and 'project'
    //check_values(file);

    // assign a UUID to the file
    cctools_uuid_t *uuid;
    cctools_uuid_create(uuid);

    char uuid_str[UUID_LEN+1];
    strcpy(uuid_str, uuid->str);
 
    //add state and uuid to json
    jx_insert_string(file, "uuid", uuid_str);
    jx_insert_string(file, "state", "mutable");

    //TODO: save UUID to file mapping in memory
   
    //return file UUID
    return uuid_str;

}

struct jx *ds_commit_file(char *uuid){

    int i;
    int length = 2;

    //TODO: get file metadata from mapping
    
    //TODO: change all blobs to RO
    for(i=0;i<length;i++){
        ds_blob_commit(blobid);
    }

    //TODO: change state to immutable 

}

struct jx *ds_delete_file(char *uuid){

    //TODO: get file data from mapping

    //TODO: remove file data from mapping

}

struct jx *ds_copy_file(char *uuid){

    //TODO: get file data from mapping

    //TODO: replicate file data to mapping

}

char *ds_submit_service(struct jx *service){

    if(validate_json(service, SUBMIT_SERVICE)){
        return NULL;
    }

    char *type = jx_lookup_string(service, "type");
    char *project = jx_lookup_string(service, "project");
    struct jx *namespace = jx_lookup(service, "namespace");
    struct jx *resources = jx_lookup(service, "resources");
    struct jx *environment = jx_lookup(service, "environment");

    //TODO: check values from above
    //check_values(service)

    // assign a UUID to the service
    cctools_uuid_t *uuid;
    cctools_uuid_create(uuid);

    char uuid_str[UUID_LEN+1];
    strcpy(uuid_str, uuid->str);
 
    //add state and uuid to service
    jx_insert_string(service, "uuid", uuid_str);

    //TODO: save UUID to file mapping in memory
   
    //return task UUID
    return uuid_str;

}

struct jx *ds_delete_service(char *uuid){

    //TODO: get service data from mapping

    //TODO: remove service data from mapping

}

char *ds_create_project(char *project_name){

    // assign a UUID to the project
    cctools_uuid_t *uuid;
    cctools_uuid_create(uuid);

    char uuid_str[UUID_LEN+1];
    strcpy(uuid_str, uuid->str);
 
    //TODO: create jx with project_name and uuid

    //TODO: save UUID to file mapping in memory
   
    //return task UUID
    return uuid_str;

}

struct jx *ds_delete_project(char *uuid){

    //TODO: get project data from mapping

    //TODO: remove project data from mapping

}

struct jx *ds_wait(){

    //TODO: block until something happens     

}

bool ds_queue_empty(){

    //TODO: return true if the queue of tasks is empty

    return false;

}

struct jx *ds_status(char *uuid){



}
