#include "dataswarm_task.h"
#include "dataswarm_mount.h"
#include "dataswarm_resources.h"

#include <stdlib.h>
#include <string.h>

struct dataswarm_task * dataswarm_task_create( struct jx *jtask )
{
	struct dataswarm_task *t = malloc(sizeof(*t));
	memset(t,0,sizeof(*t));

	t->jtask = jtask;

	t->command = jx_lookup_string(jtask,"command");
	t->taskid = jx_lookup_string(jtask,"taskid");
	t->environment = jx_lookup(jtask,"environment");
	t->resources = dataswarm_resources_create(jx_lookup(jtask,"resources"));
	t->mounts = dataswarm_mounts_create(jx_lookup(jtask,"namespace"));

	return t;

}

const char * dataswarm_task_state_string( dataswarm_task_state_t state )
{
	switch(state) {
		case DATASWARM_TASK_READY: return "ready";
		case DATASWARM_TASK_RUNNING: return "running";
		case DATASWARM_TASK_DONE: return "done";
		case DATASWARM_TASK_FAILED: return "failed";
		case DATASWARM_TASK_DELETING: return "deleting";
		default: return "unknown";
	}
}

struct jx * dataswarm_task_to_jx( struct dataswarm_task *t )
{
	struct jx *jtask = jx_object(0);
	if(t->command) jx_insert_string(jtask,"command",t->command);
	if(t->taskid) jx_insert_string(jtask,"taskid",t->taskid);
	if(t->environment) jx_insert(jtask,jx_string("environment"),jx_copy(t->environment));
	if(t->resources) jx_insert(jtask,jx_string("resources"),dataswarm_resources_to_jx(t->resources));
	if(t->mounts) jx_insert(jtask,jx_string("namespace"),dataswarm_mounts_to_jx(t->mounts));
	jx_insert_string(jtask,"state",dataswarm_task_state_string(t->state));
	return jtask;
}


void dataswarm_task_delete( struct dataswarm_task *t )
{
	if(!t) return;
	dataswarm_resources_delete(t->resources);
	dataswarm_mount_delete(t->mounts);
	dataswarm_process_delete(t->process);
	jx_delete(t->jtask);
	free(t);
}
