
#include "dataswarm_task_table.h"
#include "dataswarm_worker.h"
#include "dataswarm_task.h"
#include "dataswarm_message.h"

#include "hash_table.h"
#include "jx.h"
#include "stringtools.h"

#include <string.h>

/*
Every time a task changes state, send an async update message.
*/

static void update_task_state( struct dataswarm_worker *w, struct dataswarm_task *task, dataswarm_task_state_t state )
{
	task->state = state;
	struct jx *msg = dataswarm_message_task_update( task->taskid, dataswarm_task_state_string(state) );
	dataswarm_json_send(w->manager_link,msg,time(0)+w->long_timeout);
	free(msg);
}

dataswarm_result_t dataswarm_task_table_submit( struct dataswarm_worker *w, const char *taskid, struct jx *jtask )
{
	struct dataswarm_task *task = dataswarm_task_create(jtask);
	if(task) {
		hash_table_insert(w->task_table, taskid, task);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_MALFORMED_PARAMETERS;
	}
}
		
dataswarm_result_t dataswarm_task_table_get( struct dataswarm_worker *w, const char *taskid, struct jx **jtask )
{
	struct dataswarm_task *task = hash_table_lookup(w->task_table, taskid);
	if(task) {
		*jtask = dataswarm_task_to_jx(task);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_NO_SUCH_TASKID;
	}
}
		
dataswarm_result_t dataswarm_task_table_remove( struct dataswarm_worker *w, const char *taskid )
{
	struct dataswarm_task *task = hash_table_lookup(w->task_table, taskid);
	if(task) {
		update_task_state(w,task,DATASWARM_TASK_DELETING);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_NO_SUCH_TASKID;
	}
}

/*
Consider each task currently in possession of the worker,
and act according to it's current state.
*/

void dataswarm_task_table_advance( struct dataswarm_worker *w )
{
	struct dataswarm_task *task;
	char *taskid;

	hash_table_firstkey(w->task_table);
	while(hash_table_nextkey(w->task_table,&taskid,(void**)&task)) {

		switch(task->state) {
			case DATASWARM_TASK_READY:
				// XXX only start tasks when resources available.
				task->process = dataswarm_process_create(task,w);
				if(task->process) {
					// XXX check for invalid mounts?
					if(dataswarm_process_start(task->process,w)) {
						update_task_state(w,task,DATASWARM_TASK_RUNNING);
					} else {
						update_task_state(w,task,DATASWARM_TASK_FAILED);
					}
				} else {
					update_task_state(w,task,DATASWARM_TASK_FAILED);
				}
				break;
			case DATASWARM_TASK_RUNNING:
				if(dataswarm_process_isdone(task->process)) {
					update_task_state(w,task,DATASWARM_TASK_DONE);
				}
				break;
			case DATASWARM_TASK_DONE:
				// Do nothing until removed.
				break;
			case DATASWARM_TASK_FAILED:
				// Do nothing until removed.
				break;
			case DATASWARM_TASK_DELETING:
				// Remove the local state assocated with the process.
				dataswarm_process_delete(task->process);
				task->process = 0;
				// Send the deleted message.
				update_task_state(w,task,DATASWARM_TASK_DELETED);
				// Now actually remove it from the data structures.
				hash_table_remove(w->task_table,taskid);
				dataswarm_task_delete(task);
				break;
			case DATASWARM_TASK_DELETED:
				break;
		}
	}

}
