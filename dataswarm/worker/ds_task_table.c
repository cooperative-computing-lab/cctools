#include "ds_task_table.h"
#include "ds_worker.h"
#include "ds_process.h"
#include "common/ds_task.h"
#include "common/ds_message.h"

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

static void update_task_state( struct ds_worker *w, struct ds_task *task, ds_task_state_t state, int send_update_message )
{
	task->state = state;

	char * task_meta = string_format("%s/task/%s/meta",w->workspace,task->taskid);
	ds_task_to_file(task,task_meta);
	free(task_meta);

	if(send_update_message) {
		struct jx *msg = ds_message_task_update( task->taskid, ds_task_state_string(state) );
		ds_json_send(w->manager_link,msg,time(0)+w->long_timeout);
		free(msg);
	}
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
		update_task_state(w,task,DS_TASK_DELETING,0);
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
						update_task_state(w,task,DS_TASK_RUNNING,1);
					} else {
						update_task_state(w,task,DS_TASK_FAILED,1);
					}
				} else {
					update_task_state(w,task,DS_TASK_FAILED,1);
				}
				break;
			case DS_TASK_RUNNING:
				process = hash_table_lookup(w->process_table,taskid);
				if(ds_process_isdone(process)) {
					update_task_state(w,task,DS_TASK_DONE,1);
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
				update_task_state(w,task,DS_TASK_DELETED,1);
				// Now actually remove it from the data structures.
				hash_table_remove(w->task_table,taskid);
				ds_task_delete(task);
				break;
			case DS_TASK_DELETED:
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

void ds_task_table_recover( struct ds_worker *w )
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
		struct ds_task *task;

		debug(D_DATASWARM,"recovering task %s",d->d_name);

		task_meta = string_format("%s/task/%s/meta",w->workspace,d->d_name);

		task = ds_task_create_from_file(task_meta);
		if(task) {
			hash_table_insert(w->task_table,task->taskid,task);
			if(task->state==DS_TASK_RUNNING) {
				update_task_state(w,task,DS_TASK_FAILED,0);
			}
		}
		free(task_meta);

	}

	debug(D_DATASWARM,"done recovering tasks");
	closedir(dir);
	free(task_dir);
}

void ds_task_table_purge( struct ds_worker *w )
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


