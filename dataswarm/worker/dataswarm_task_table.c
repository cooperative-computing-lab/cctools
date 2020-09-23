
#include "dataswarm_task_table.h"
#include "dataswarm_worker.h"
#include "dataswarm_task.h"
#include "dataswarm_message.h"

#include "hash_table.h"
#include "jx.h"
#include "stringtools.h"
#include "debug.h"
#include "delete_dir.h"

#include <dirent.h>
#include <string.h>

/*
Every time a task changes state, record the change on disk,
and then send an async update message if requested.
*/

static void update_task_state( struct dataswarm_worker *w, struct dataswarm_task *task, dataswarm_task_state_t state, int send_update_message )
{
	task->state = state;

	char * task_meta = string_format("%s/task/%s/meta",w->workspace,task->taskid);
	dataswarm_task_to_file(task,task_meta);
	free(task_meta);

	if(send_update_message) {
		struct jx *msg = dataswarm_message_task_update( task->taskid, dataswarm_task_state_string(state) );
		dataswarm_json_send(w->manager_link,msg,time(0)+w->long_timeout);
		free(msg);
	}
}

dataswarm_result_t dataswarm_task_table_submit( struct dataswarm_worker *w, const char *taskid, struct jx *jtask )
{
	struct dataswarm_task *task = dataswarm_task_create_from_jx(jtask);
	if(task) {
		update_task_state(w,task,DATASWARM_TASK_READY,0);
		hash_table_insert(w->task_table, taskid, task);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_BAD_PARAMS;
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
		update_task_state(w,task,DATASWARM_TASK_DELETING,0);
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
						update_task_state(w,task,DATASWARM_TASK_RUNNING,1);
					} else {
						update_task_state(w,task,DATASWARM_TASK_FAILED,1);
					}
				} else {
					update_task_state(w,task,DATASWARM_TASK_FAILED,1);
				}
				break;
			case DATASWARM_TASK_RUNNING:
				if(dataswarm_process_isdone(task->process)) {
					update_task_state(w,task,DATASWARM_TASK_DONE,1);
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
				update_task_state(w,task,DATASWARM_TASK_DELETED,1);
				// Now actually remove it from the data structures.
				hash_table_remove(w->task_table,taskid);
				dataswarm_task_delete(task);
				break;
			case DATASWARM_TASK_DELETED:
				break;
		}
	}

}

/*
After a restart, scan the tasks on disk to recover the table,
then cancel any tasks that were running and are now presumed dead.
Note that we are not connected to the master at this point,
so do not send any message.  A complete set up updates gets sent
when we reconnect.
*/

void dataswarm_task_table_recover( struct dataswarm_worker *w )
{
	char * task_dir = string_format("%s/task",w->workspace);

	DIR *dir;
	struct dirent *d;

	debug(D_DATASWARM,"checking %s for tasks to recover...",task_dir);

	dir = opendir(task_dir);
	if(!dir) return;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;
		if(!strcmp(d->d_name,"deleting")) continue;

		char * task_meta;
		struct dataswarm_task *task;

		debug(D_DATASWARM,"recovering task %s",d->d_name);

		task_meta = string_format("%s/task/%s/meta",w->workspace,d->d_name);

		task = dataswarm_task_create_from_file(task_meta);
		if(task) {
			hash_table_insert(w->task_table,task->taskid,task);
			if(task->state==DATASWARM_TASK_RUNNING) {
				update_task_state(w,task,DATASWARM_TASK_FAILED,0);
			}
		}
		free(task_meta);

	}

	debug(D_DATASWARM,"done recovering tasks");
	closedir(dir);
	free(task_dir);
}

void dataswarm_task_table_purge( struct dataswarm_worker *w )
{
	char *dirname = string_format("%s/task/deleting",w->workspace);

	debug(D_DATASWARM,"checking %s for stale tasks to delete:",dirname);

	DIR *dir = opendir(dirname);
	if(dir) {
		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;
			char *taskname = string_format("%s/%s",dirname,d->d_name);
			debug(D_DATASWARM,"deleting task: %s",taskname);
			delete_dir(taskname);
			free(taskname);
		}
	}

	debug(D_DATASWARM,"done checking for stale tasks");

	free(dirname);
}

