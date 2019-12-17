#include "work_queue_json.h"
#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <string.h>
#include <time.h>

char * workqueue = "{ \"name\" : \"server_wq\" , \"port\" : 1234 }";

void reply(struct link *client, char *method, char* message, int id){

    struct jx_pair *jsonrpc, *result, *idd;
    struct jx *j;
    char *response;
    time_t t;

    idd = jx_pair(jx_string("id"), jx_integer(id), NULL);

    if(!strcmp(method, "error")){
        result = jx_pair(jx_string("error"), jx_string(message), idd);
    }
    else {
        result = jx_pair(jx_string("result"), jx_string(message), idd);
    }
        
    jsonrpc = jx_pair(jx_string("jsonrpc"), jx_string("2.0"), result);

    j = jx_object(jsonrpc);

    response = jx_print_string(j);

    t = time(NULL);
    link_write(client, response, strlen(response), t + 10);

    jx_delete(j);

}

int main(){

    const char *key;
    char *error;
    char message[BUFSIZ];
    time_t t;
    void *k = NULL, *v = NULL;
    char *method, *task;
    struct jx *val, *value, *jsonrpc;
    ssize_t read;
    int timeout, taskid, id=-1;

    //create work queue
    struct work_queue* q = work_queue_json_create(workqueue);

    if (!q){
        printf("Could not create work_queue\n");
        return 1;
    }
    
    //wait for client to connect
    struct link* client = link_serve(2345);

    if (!client){
        printf("Could not serve on port 2345\n");
        return 1;
    }

    t = time(NULL);

    client = link_accept(client, t + 100);

    if (!client){
        printf("Could not accept connection\n");
        return 1;
    }

    printf("Connected to client. Waiting for messages..\n");

    while (true){

        //reset
        error = NULL;
        id = -1;

        //receive message
        t = time(NULL);

        read = link_readline(client, message, BUFSIZ, t + 10);

        if (!read){
            error = "Error reading from client";
            reply(client, "error", error, id);
            break;
        }

        jsonrpc = jx_parse_string(message);

        if (!jsonrpc){
            error = "Could not parse JSON string";
            reply(client, "error", error, id);
            continue;
        }

        //iterate over the object: get method and task description
        key = jx_iterate_keys(jsonrpc, &k);
        value = jx_iterate_values(jsonrpc, &v);

        while (key){
            
            if (!strcmp(key, "method")){
                method = value->u.string_value;
            }

            else if (!strcmp(key, "params")){
                val = value;
            }
            else if (!strcmp(key, "id")){
                id = value->u.integer_value;
            }
            else if (strcmp(key, "jsonrpc")){
                error = "unrecognized parameter";
                reply(client, "error", error, id);
            }

            key = jx_iterate_keys(jsonrpc, &k);
            value = jx_iterate_values(jsonrpc, &v);

        }
    
        //submit or wait
        if (!strcmp(method, "submit")){
            
            task = jx_print_string(val);

            taskid = work_queue_json_submit(q, task);

            if (taskid < 0){
                error = "Could not submit task";
                reply(client, "error", error, id);
                continue;
            }
            
            reply(client, method, "Task submitted successfully.", id);

        }
        else if (!strcmp(method, "wait")){

            timeout = val->u.integer_value;

            task = 0;
            task = work_queue_json_wait(q, timeout);

            if (!task){
                error = "timeout reached with no task returned";
                reply(client, "error", error, id);
                break;
            }

            reply(client, method, task, id);

        }
        else if (!strcmp(method, "remove")){
            taskid = val->u.integer_value;

            task = work_queue_json_remove(q, taskid);
            if (!task){
                error = "task not able to be removed from queue";
                reply(client, "error", error, id);
            }

            reply(client, method, "Task removed successfully.", id);

        }
        else{
            error = "Method not recognized";
            reply(client, "error", error, id);
        }

        //clean up
        jx_delete(value);
        jx_delete(jsonrpc);

    }

    return 0;

}
