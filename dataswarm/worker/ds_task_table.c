#include "ds_task_table.h"
#include "ds_worker.h"
#include "ds_process.h"
#include "common/ds_task.h"
#include "common/ds_message.h"

#include "hash_table.h"
#include "jx.h"
#include "stringtools.h"

#include <string.h>

/*
Every time a task changes state, send an async update message.
*/

static void update_task_state( struct ds_worker *w, struct ds_task *task, ds_task_state_t state )
{
	task->state = state;
	struct jx *msg = ds_message_task_update( task->taskid, ds_task_state_string(state) );
	ds_json_send(w->manager_link,msg,time(0)+w->long_timeout);
	free(msg);
}

ds_result_t ds_task_table_submit( struct ds_worker *w, const char *taskid, struct jx *jtask )
{
	struct ds_task *task = ds_task_create(jtask);
	if(task) {
		hash_table_insert(w->task_table, taskid, task);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_BAD_PARAMS;
	}
}

ds_result_t ds_task_table_get( struct ds_worker *w, const char *taskid, struct jx **jtask )
{
	struct ds_task *task = hash_table_lookup(w->task_table, taskid);
	if(task) {
		*jtask = ds_task_to_jx(task);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_NO_SUCH_TASKID;
	}
}

ds_result_t ds_task_table_remove( struct ds_worker *w, const char *taskid )
{
	struct ds_task *task = hash_table_lookup(w->task_table, taskid);
	if(task) {
		update_task_state(w,task,DS_TASK_DELETING);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_NO_SUCH_TASKID;
	}
}

/*
Consider each task currently in possession of the worker,
and act according to it's current state.
*/

void ds_task_table_advance( struct ds_worker *w )
{
	struct ds_task *task;
	struct ds_process *process;
	char *taskid;

	hash_table_firstkey(w->task_table);
	while(hash_table_nextkey(w->task_table,&taskid,(void**)&task)) {

		switch(task->state) {
			case DS_TASK_READY:
				// XXX only start tasks when resources available.
				process = ds_process_create(task,w->workspace);
				if(process) {
					hash_table_insert(w->process_table,taskid,process);
					// XXX check for invalid mounts?
					if(ds_process_start(process,w->workspace)) {
						update_task_state(w,task,DS_TASK_RUNNING);
					} else {
						update_task_state(w,task,DS_TASK_FAILED);
					}
				} else {
					update_task_state(w,task,DS_TASK_FAILED);
				}
				break;
			case DS_TASK_RUNNING:
				process = hash_table_lookup(w->process_table,taskid);
				if(ds_process_isdone(process)) {
					update_task_state(w,task,DS_TASK_DONE);
				}
				break;
			case DS_TASK_DONE:
				// Do nothing until removed.
				break;
			case DS_TASK_FAILED:
				// Do nothing until removed.
				break;
			case DS_TASK_DELETING:
				// Remove the local state assocated with the process.
				process = hash_table_remove(w->process_table,taskid);
				if(process) ds_process_delete(process);
				// Send the deleted message.
				update_task_state(w,task,DS_TASK_DELETED);
				// Now actually remove it from the data structures.
				hash_table_remove(w->task_table,taskid);
				ds_task_delete(task);
				break;
			case DS_TASK_DELETED:
				break;
		}
	}

}
