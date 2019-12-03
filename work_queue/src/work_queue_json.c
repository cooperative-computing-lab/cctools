#include "work_queue_json.h"

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char* work_queue_properties[] = {"name", "port", "priority", "num_tasks_left", "next_taskid", "workingdir", "master_link", 
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

static const char* work_queue_task_properties[] = { "tag", "command_line", "worker_selection_algorithm", "output", "input_files", 
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


static int is_in(const char* str, const char** array)
{

    const char** ptr = array;

    while(*ptr != 0){

        if(!strcmp(*ptr, str)){
            return 1;
        }

        ptr++;

    }
    
    return 0;

}

static int validate_json(struct jx *json, const char** array){

    //iterate over the keys in a JX_OBJECT
    void *j = NULL;
    const char* key = jx_iterate_keys(json, &j);

    while (key != NULL) {

        if(!is_in(key, array)){
            return 1;
        }

        key = jx_iterate_keys(json, &j);

    }

    return 0;

}

static int specify_files(int input, struct jx *files, struct work_queue_task *task){

    void *i = NULL;
    struct jx *arr = jx_iterate_array(files, &i);

    while (arr != NULL){
        
        char *local, *remote;
        struct jx_pair* flag;
        void *k = NULL;
        void *v = NULL;
        int cache = 1;
        int nocache = 0;
        int watch = 16;
        int flags = 0;

        const char *key = jx_iterate_keys(arr, &k);
        struct jx *value = jx_iterate_values(arr, &v);

        while (key != NULL){


            if (!strcmp(key, "local_name")){
                local = value->u.string_value;
            }
            else if (!strcmp(key, "remote_name")){
                remote = value->u.string_value;
            }
            else if (!strcmp(key, "flags")){
                flag = value->u.pairs;
                while(flag){
                    char *flag_key = flag->key->u.string_value;
                    bool flag_value = flag->value->u.boolean_value;
                    if (!strcmp(flag_key, "WORK_QUEUE_NOCACHE")){
                        if (flag_value){
                            flags |= nocache;
                        }
                    }
                    else if (!strcmp(flag_key, "WORK_QUEUE_CACHE")){
                        if (flag_value){
                            flags |= cache;
                        }
                    }
                    else if (!strcmp(flag_key, "WORK_QUEUE_WATCH")){
                        if (flag_value){
                            flags |= watch;
                        }
                    }
                    else{
                        printf("KEY ERROR: %s not valid", flag_key);
                        return 1;
                    }
                    flag = flag->next;
                }
            }
            else{
                printf("KEY ERROR: %s not valid", key);
                return 1;
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


static struct work_queue_task* create_task(const char* str){

    char *command_line;
    struct jx *input_files, *output_files;

    struct jx *json = jx_parse_string(str); 
    if(!json){
        return NULL;
    }

    //validate json
    if(validate_json(json, work_queue_task_properties)){
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

struct work_queue* work_queue_json_create(const char* str){


    int port=0, priority=0;
    char *name;

    struct jx* json = jx_parse_string(str);
    if(!json){
        return NULL;
    }

    //validate json
    if(validate_json(json, work_queue_properties)){
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

int work_queue_json_submit(struct work_queue *q, const char* str){

    struct work_queue_task * task;

    task = create_task(str);

    if(task){
        return work_queue_submit(q, task);
    }
    else{
        return -1;
    }

}

char* work_queue_json_wait(struct work_queue *q, int timeout){

    char *task;
    struct jx *j;
    struct jx_pair *command_line, *taskid, *return_status, *tag, *output, *result;

    struct work_queue_task *t = work_queue_wait(q, timeout);

    command_line = jx_pair(jx_string("command_line"), jx_string(t->command_line), NULL);
    taskid = jx_pair(jx_string("taskid"), jx_integer(t->taskid), command_line);
    return_status = jx_pair(jx_string("return_status"), jx_integer(t->return_status), taskid);
    result = jx_pair(jx_string("result"), jx_integer(t->result), return_status);

    if (t->tag){
        tag = jx_pair(jx_string("tag"), jx_string(t->tag), result);
        output = jx_pair(jx_string("output"), jx_string(t->output), tag);
    }
    else {
        output = jx_pair(jx_string("output"), jx_string(t->output), result);
    }

    j = jx_object(output);

    task = jx_print_string(j);

    return task;    

}
