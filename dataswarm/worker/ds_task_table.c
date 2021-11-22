#include "ds_task_table.h"
#include "ds_worker.h"
#include "ds_process.h"
#include "ds_task.h"
#include "ds_message.h"
#include "ds_task_attempt.h"

#include "hash_table.h"
#include "jx.h"
#include "stringtools.h"
#include "debug.h"
#include "unlink_recursive.h"
#include "macros.h"

#include <dirent.h>
#include <string.h>

/*
Every time a task changes state, record the change on disk,
and then send an async update message if requested.
*/

static void update_task_state( struct ds_worker *w, struct ds_task *task, ds_task_state_t state, ds_task_result_t result, int send_update_message )
{
	debug(D_DATASWARM,"task %s %s -> %s",
	      task->taskid,
	      ds_task_state_string(task->state),
	      ds_task_state_string(state));

	task->state = state;

    if(task->state == DS_TASK_DONE) {
        task->result = result;
    }

	char * task_meta = ds_worker_task_meta(w,task->taskid);
	ds_task_to_file(task,task_meta);
	free(task_meta);

	if(send_update_message) {
		struct jx *msg = ds_message_task_update(task);
		ds_json_send(w->manager_connection,msg);
		free(msg);
	}
}

ds_result_t ds_task_table_submit( struct ds_worker *w, const char *taskid, struct jx *jtask )
{
	struct ds_task *task = hash_table_lookup(w->task_table,taskid);
	if(task) {
		return DS_RESULT_TASKID_EXISTS;
	}

	task = ds_task_create(jtask);
	if(task) {
		ds_task_attempt_create(task);
		hash_table_insert(w->task_table, taskid, task);
		debug(D_DATASWARM,"task %s created",taskid);
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
		update_task_state(w, task, DS_TASK_DELETING, DS_TASK_RESULT_UNDEFINED, 0);
		return DS_RESULT_SUCCESS;
	} else {
		return DS_RESULT_NO_SUCH_TASKID;
	}
}

ds_result_t ds_task_table_list( struct ds_worker *w, struct jx **result )
{
	struct ds_task *task;
	char *taskid;

	*result = jx_object(0);

	hash_table_firstkey(w->task_table);
	while(hash_table_nextkey(w->task_table,&taskid,(void**)&task)) {
		jx_insert(*result,jx_string(taskid),ds_task_to_jx(task));
	}

	return DS_RESULT_SUCCESS;
}


void ds_task_try_advance(struct ds_worker *w, struct ds_task *task) {
    struct ds_process *process;

    switch(task->attempts->state) {
        case DS_TASK_TRY_NEW:
            if(!ds_worker_resources_avail(w,task->resources)) break;

            process = ds_process_create(task,w);
            if(process) {
                hash_table_insert(w->process_table,task->taskid,process);
                // XXX check for invalid mounts?
                if(ds_process_start(process,w)) {
                    update_task_state(w, task, DS_TASK_ACTIVE, DS_TASK_RESULT_UNDEFINED, 1);
					task->attempts->state = DS_TASK_TRY_PENDING;
                    ds_worker_resources_alloc(w,task->resources);
                } else {
                    update_task_state(w, task, DS_TASK_DONE, DS_TASK_RESULT_ERROR, 1);
                    // Mark disk as allocated to match free during delete.
                    ds_worker_disk_alloc(w,task->resources->disk);
                }
            } else {
                update_task_state(w, task, DS_TASK_DONE, DS_TASK_RESULT_ERROR, 1);
            }
            break;
        case DS_TASK_TRY_PENDING:
            process = hash_table_lookup(w->process_table,task->taskid);
            if(ds_process_isdone(process)) {
                ds_worker_resources_free_except_disk(w,task->resources);
                update_task_state(w, task, DS_TASK_DONE, DS_TASK_RESULT_SUCCESS, 1);
            }
            break;
        default:
            break;
    }
}

/*
Consider each task currently in possession of the worker,
and act according to it's current state.
*/

void ds_task_table_advance( struct ds_worker *w )
{
	struct ds_task *task;
	char *taskid;
    struct ds_process *process;

	hash_table_firstkey(w->task_table);
	while(hash_table_nextkey(w->task_table,&taskid,(void**)&task)) {

		switch(task->state) {
            case DS_TASK_ACTIVE:
                ds_task_try_advance(w, task);
                break;
			case DS_TASK_DELETING:
				{
				char *sandbox_dir = ds_worker_task_sandbox(w,task->taskid);
				char *task_dir = ds_worker_task_dir(w,task->taskid);

				// First delete the sandbox dir, which could be large and slow.
				unlink_recursive(sandbox_dir);

				// Now delete the task dir and metadata file, which should be quick.
				unlink_recursive(task_dir);

				free(sandbox_dir);
				free(task_dir);

				// Send the deleted message (need the task structure still)
				update_task_state(w, task, DS_TASK_DELETED, DS_TASK_RESULT_UNDEFINED, 1);

				// Now note that the storage has been reclaimed.
				ds_worker_disk_free(w,task->resources->disk);

				// Remove and delete the process and task structures.
				process = hash_table_remove(w->process_table,taskid);
				if(process) ds_process_delete(process);
				task = hash_table_remove(w->task_table,taskid);
				if(task) ds_task_delete(task);
				}
				break;
			case DS_TASK_DELETED:
				break;
			case DS_TASK_DONE:
                /* do nothing until removed */
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
	int64_t total_disk_used=0;

	debug(D_DATASWARM,"checking %s for tasks to recover...",task_dir);

	dir = opendir(task_dir);
	if(!dir) return;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;

		char * task_meta;
		struct ds_task *task;

		debug(D_DATASWARM,"recovering task %s",d->d_name);

		task_meta = ds_worker_task_meta(w,d->d_name);

		task = ds_task_create_from_file(task_meta);
		if(task) {
			hash_table_insert(w->task_table,task->taskid,task);
			if(task->attempts->state==DS_TASK_TRY_PENDING) {
				// If it was running, then it's not now.
				update_task_state(w, task, DS_TASK_DONE, DS_TASK_RESULT_ERROR, 0);
			}
			if(task->state != DS_TASK_ACTIVE) {
				// If it got past running, then the storage was allocated
				total_disk_used += task->resources->disk;
			}
			// Note that tasks still deleting will be handled in task_advance.
		}
		free(task_meta);

	}

	closedir(dir);
	free(task_dir);

	debug(D_DATASWARM,"done recovering tasks");
	debug(D_DATASWARM,"%d tasks recovered using %lld MB disk",
		hash_table_size(w->task_table),(long long)total_disk_used);

	// Account for the total allocated size of task sandboxes.
	ds_worker_disk_alloc(w,total_disk_used);
}
