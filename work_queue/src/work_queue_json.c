#ifndef WORK_QUEUE_JSON_H
#define WORK_QUEUE_JSON_H

#include "work_queue_json.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "work_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char* wq[] = {"name", "port", "priority", "num_tasks_left", "next_taskid", "workingdir", "master_link", 
            "poll_table", "poll_table_size", "tasks", "task_state_map", "ready_list", "worker_table",
            "worker_blacklist", "worker_task_map", "categories", "workers_with_available_results",
            "stats", "stats_measure", "stats_disconnected_workers", "time_last_wait", 
            "worker_selection_algorithm", "task_ordering", "process_pending_check", "short_timeout",
            "long_timeout", "task_reports", "asynchrony_multiplier", "asynchrony_modifier", 
            "minimum_transfer_timeout", "foreman_transfer_timeout", "transfer_outlier_factor",
            "default_transfer_rate", "catalog_hosts", "catalog_last_update_time", 
            "resources_last_update_time", "busy_waiting_flag", "allocation_default_mode", "logfile",
            "transactions_logfile", "keepalive_interval", "keepalive_timeout", "link_poll_end",
            "master_preferred_connection", "monitor_mode", "monitor_file", "monitor_output_directory", 
            "monitor_summary_filename", "monitor_exe", "measured_local_resources",
            "current_max_worker", "password", "bandwidth"};

char* t[] = { "tag", "command_line", "worker_selection_algorithm", "output", "input_files", 
            "output_files", "env_list", "taskid", "return_status", "result", "host", "hostname",
            "category", "resource_request", "priority", "max_retries", "try_count", 
            "exhausted_attempts", "time_when_submitted", "time_when_done", 
            "disk_allocation_exhausted", "time_when_commit_start", "time_when_commit_end",
            "time_when_retrieval", "time_workers_execute_last", "time_workers_execute_all", 
            "time_workers_execute_exhaustion", "time_workers_execute_failure", "bytes_received",
            "bytes_sent", "bytes_transferred", "resources_allocated", "resources_measured", 
            "resources_requested", "monitor_output_directory", "monitor_snapshot_file", "features",
            "time_task_submit", "time_task_finish", "time_committed", "time_send_input_start",
            "time_send_input_finish", "time_receive_result_start", "time_receive_result_finish",
            "time_receive_output_start", "time_receive_output_finish", "time_execute_cmd_start",
            "time_execute_cmd_finish", "total_transfer_time", "cmd_execution_time", 
            "total_cmd_execution_time", "total_cmd_exhausted_execute_time", 
            "total_time_until_worker_failure", "total_bytes_received", "total_bytes_sent", 
            "total_bytes_transferred", "time_app_delay"};


int is_in(const char* str, char* a[])
{

    int length = sizeof(a) / sizeof(char);

    int i;
    for (i=0; i < length; i++){
        
        if (!strcmp(a[i], str)) { return 1; }

    }

    return 0;

}

int validate_json(struct jx *json, char** a){

    //iterate over the keys in a JX_OBJECT
    void *j = NULL;
    const char* key = jx_iterate_keys(json, &j);

    while (key != NULL) {

        if(!is_in(key, a)){
            return 1;
        }

        key = jx_iterate_keys(json, &j);

    }

    return 0;

}

int specify_files(int input, struct jx *files, struct work_queue_task *task){

    void *i = NULL;
    struct jx *arr = jx_iterate_array(files, &i);

    while (arr != NULL){
        
        char *local, *remote;
        int flags;
        void *k = NULL;
        void *v = NULL;

        const char *key = jx_iterate_keys(arr, &k);
        struct jx *value = jx_iterate_values(arr, &v);

        while (key != NULL){


            if (!strcmp(key, "local")){
                local = value->u.string_value;
            }
            else if (!strcmp(key, "remote")){
                remote = value->u.string_value;
            }
            else if (!strcmp(key, "flags")){
                flags = value->u.integer_value;
            }
            else{
                printf("%s\n",value->u.string_value);
            }

            key = jx_iterate_keys(arr, &k);
            value = jx_iterate_values(arr, &v);

        }

        if (input){
            work_queue_task_specify_file(task, local, remote, WORK_QUEUE_INPUT, flags);
        }
        else{
            work_queue_task_specify_file(task, local, remote, WORK_QUEUE_OUTPUT, flags);
        }

        arr = jx_iterate_array(files, &i);
    
    }

    return 0;

}


/* WQTCREATE
 * takes in a JSON string and returns a work_queue struct
 */
struct work_queue_task* work_queue_task_json_create(char* str){

    char *command_line;
    struct jx *input_files, *output_files;

    struct jx *json = jx_parse_string(str); 

    //validate json
    if(validate_json(json, t)){
        return NULL;
    }

    //get command from json
    void *j = NULL;
    void *i = NULL;
    const char* key = jx_iterate_keys(json, &j);
    struct jx *value = jx_iterate_values(json, &i);

    while (key != NULL) {

        if(!strcmp(key, "command_line")){
            command_line = value->u.string_value;
        }
        else if (!strcmp(key, "input_files")){
            input_files = value;
        }
        else if (!strcmp(key, "output_files")){
            output_files = value;
        }
        else{
            printf("%s\n",value->u.string_value);
        }

        key = jx_iterate_keys(json, &j);
        value = jx_iterate_values(json, &i);
    
    }    

    //call work_queue_task_create
    if(command_line){

        struct work_queue_task* task = work_queue_task_create(command_line);

        if (!task){
            return NULL;
        }

        if(input_files){            
            specify_files(1, input_files, task);
        }

        if(output_files){
            specify_files(0, output_files, task);
        }
    
        return task;

    }

    return NULL;

}

/* WQCREATE
 * takes in a JSON string and returns a work_queue struct
 */
struct work_queue* work_queue_json_create(char* str){


    int port=0, priority=0;
    char *name;

    struct jx* json = jx_parse_string(str);

    //validate json
    if(validate_json(json, wq)){
        return NULL;
    }

    void *i = NULL;
    void *j = NULL;
    const char* key = jx_iterate_keys(json, &j);
    struct jx* value = jx_iterate_values(json, &i);

    while (key != NULL) {

        if (!strcmp(key, "name")){
            name = value->u.string_value;
        }
        else if (!strcmp(key, "port")){
            port = value->u.integer_value;
        }
        else if (!strcmp(key, "priority")){
            priority = value->u.integer_value;
        }
        else{
            printf("Not necessary: %s\n", value->u.string_value);
        }

        key = jx_iterate_keys(json, &j);
        value = jx_iterate_values(json, &i);

    }    

    if(port){

        struct work_queue *workqueue = work_queue_create(port);

        if(!workqueue){ 
            return NULL; 
        }        

        if(name){
            work_queue_specify_name(workqueue, name);
        }
        if(priority){
            work_queue_specify_priority(workqueue, priority);
        }
    
        return workqueue;

    }

    return NULL;

}

/* SUBMIT
 *
 */
int work_queue_json_submit(struct work_queue *q, char* str){

    struct work_queue_task * task;

    task = work_queue_task_json_create(str);

    if(task){
        return work_queue_submit(q, task);
    }
    else{
        return -1;
    }

}

#endif
