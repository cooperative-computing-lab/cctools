/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "timestamp.h"
#include "stringtools.h"
#include "list.h"
#include "makeflow_summary.h"
#include "../../dttools/src/catalog_query.h"
#include "../../dttools/src/json.h"
#include "../../dttools/src/json_aux.h"
#include "../../dttools/src/username.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>

//packet for start and stop, once per min, configurable time

//make sure to send: project name, owner, type

//waiting means: inputs not ready, running means: took job and submitted

int makeflow_catalog_summary(struct dag* d, char* name){
    struct dag_node *n;
    const char *fn = 0;
    dag_node_state_t state;
    
    struct list *failed_tasks; 
    failed_tasks = list_create();
    
    int tasks_completed = 0;
    int tasks_aborted   = 0;
    int tasks_waiting   = 0;
    int tasks_running   = 0;

    for (n = d->nodes; n; n = n->next) {
        state = n->state;
        if (state == DAG_NODE_STATE_FAILED && !list_find(failed_tasks, (int (*)(void *, const void *)) string_equal, (void *) fn))
            list_push_tail(failed_tasks, (void *) n->command);
        else if (state == DAG_NODE_STATE_ABORTED)
            tasks_aborted++;
        else if (state == DAG_NODE_STATE_COMPLETE) 
            tasks_completed++;
        else if(state == DAG_NODE_STATE_RUNNING)
            tasks_running++;
        else if(state == DAG_NODE_STATE_WAITING)
            tasks_waiting++;
    }
    
    //transmit report here
    //creates memory
    char* host = string_format("%s:%i",CATALOG_HOST,CATALOG_PORT);
    
    char username[USERNAME_MAX];
    username_get(username);
    
    //creates memory
    char* text = string_format("{\"type\":\"makeflow\",\"total\":%i,\"running\":%i,\"waiting\":%i,\"aborted\":%i,\"completed\":%i,\"failed\":%i,\"project\":\"%s\",\"owner\":\"%s\"}",
                         itable_size(d->node_table), tasks_running, tasks_waiting, tasks_aborted, tasks_completed, list_size(failed_tasks),name,username);
    
    int resp = catalog_query_send_update(host, text);
    
    free(host);
    free(text);
    
    list_free(failed_tasks);
    list_delete(failed_tasks);
    
    return resp;//all good
}
