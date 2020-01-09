/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_json.h"
#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SERVER_PORT 2345
#define WQ_PORT 1234

char * workqueue = "{ \"name\" : \"server_wq\" , \"port\" : 1234 }";
int timeout = 25;

void reply(struct link *client, char *method, char* message, int id){

    struct jx_pair *result;

    int len = strlen(message);
    char buffer[BUFSIZ];

    sprintf(buffer, "%d", len);

    struct jx_pair *idd = jx_pair(jx_string("id"), jx_integer(id), NULL);

    if(!strcmp(method, "error")){
        result = jx_pair(jx_string("error"), jx_string(message), idd);
    }
    else {
        result = jx_pair(jx_string("result"), jx_string(message), idd);
    }
        
    struct jx_pair *jsonrpc = jx_pair(jx_string("jsonrpc"), jx_string("2.0"), result);

    struct jx *j = jx_object(jsonrpc);

    char *response = jx_print_string(j);

    strcat(buffer, response);

    ssize_t written = link_write(client, buffer, strlen(buffer), time(NULL)+timeout);

    while (written < 0){
       written = link_write(client, buffer, strlen(buffer), time(NULL)+timeout); 
    }

    jx_delete(j);

}

int main(){

    char message[BUFSIZ];

        //create work queue
        struct work_queue* q = work_queue_json_create(workqueue);
    
        if (!q){
            printf("Could not create work_queue\n");
            return 1;
        }
 
    while(true){
    
        //clear work queue
        work_queue_cancel_all_tasks(q);       

        //wait for client to connect
        struct link* client = link_serve(SERVER_PORT);
    
        if (!client){
            printf("Could not serve on port %d\n", SERVER_PORT);
            return 1;
        }
    
        client = link_accept(client, time(NULL)+timeout);
    
        if (!client){
            printf("Could not accept connection\n");
        }
    
        printf("Connected to client. Waiting for messages..\n");
    
        while (true){
    
            //reset
            char *error = NULL;
            int id = -1;
    
            //receive message
            ssize_t read = link_readline(client, message, BUFSIZ, time(NULL)+timeout);
    
            //if server cannot read message from client, break connection
            if (!read){
                error = "Error reading from client";
                reply(client, "error", error, id);
                link_close(client);
                break;
            }
    
            struct jx *jsonrpc = jx_parse_string(message);
    
            //if server cannot parse JSON string, break connection
            if (!jsonrpc){
                error = "Could not parse JSON string";
                reply(client, "error", error, id);
                link_close(client);
                break;
            }
    
            //iterate over the object: get method and task description
            void *k = NULL, *v = NULL;
            const char *key = jx_iterate_keys(jsonrpc, &k);
            struct jx *value = jx_iterate_values(jsonrpc, &v);
    
            char *method;
            struct jx *val;

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
                    link_close(client);
                    break;
                }
    
                key = jx_iterate_keys(jsonrpc, &k);
                value = jx_iterate_values(jsonrpc, &v);
    
            }
        
            //submit or wait
            if (!strcmp(method, "submit")){
                
                char *task = val->u.string_value;
    
                int taskid = work_queue_json_submit(q, task);
    
                if (taskid < 0){
                    error = "Could not submit task";
                    reply(client, "error", error, id);
                }
                
                reply(client, method, "Task submitted successfully.", id);
    
            }
            else if (!strcmp(method, "wait")){
    
                int time_out = val->u.integer_value;
    
                char *task = work_queue_json_wait(q, time_out);
    
                if (!task){
                    error = "timeout reached with no task returned";
                    reply(client, "error", error, id);
                }
    
                reply(client, method, task, id);
    
            }
            else if (!strcmp(method, "remove")){
                int taskid = val->u.integer_value;
    
                char *task = work_queue_json_remove(q, taskid);
                if (!task){
                    error = "task not able to be removed from queue";
                    reply(client, "error", error, id);
                }
    
                reply(client, method, "Task removed successfully.", id);
    
            }
            else if (!strcmp(method, "disconnect")){
                reply(client, method, "Successfully disconnected.", id);
                break;
            }
            else{
                error = "Method not recognized";
                reply(client, "error", error, id);
            }
    
            //clean up
            jx_delete(value);
            jx_delete(jsonrpc);
    
        }
    
        link_close(client);
    
    }

    return 0;

}
