#include "ds_task.h"
#include "ds_mount.h"
#include "ds_resources.h"

#include "jx_print.h"
#include "jx_parse.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct ds_task * ds_task_create( struct jx *jtask )
{
	struct ds_task *t = malloc(sizeof(*t));
	memset(t,0,sizeof(*t));

	t->command = jx_lookup_string_dup(jtask,"command");
	t->taskid = jx_lookup_string_dup(jtask,"task-id");

	t->environment = jx_lookup(jtask,"environment");
	if(t->environment) t->environment = jx_copy(t->environment);

	t->resources = ds_resources_create_from_jx(jx_lookup(jtask,"resources"));
	t->mounts = ds_mounts_create(jx_lookup(jtask,"namespace"));

	t->state = DS_TASK_ACTIVE;

	t->worker = NULL;
	t->attempts = NULL;

	t->subscribers = set_create(0);

	return t;

}

struct ds_task * ds_task_create_from_file( const char *filename )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct jx *jtask = jx_parse_stream(file);
	if(!jtask) {
		fclose(file);
		return 0;
	}

	struct ds_task *t = ds_task_create(jtask);

	jx_delete(jtask);
	fclose(file);

	return t;
}

const char * ds_task_state_string( ds_task_state_t state )
{
	switch(state) {
		case DS_TASK_ACTIVE: return "active";
		case DS_TASK_DONE: return "done";
		case DS_TASK_DELETING: return "deleting";
		case DS_TASK_DELETED: return "deleted";
		default: return "unknown";
	}
}

struct jx * ds_task_to_jx( struct ds_task *t )
{
	struct jx *jtask = jx_object(0);
	if(t->command) jx_insert_string(jtask,"command",t->command);
	if(t->taskid) jx_insert_string(jtask,"task-id",t->taskid);
	if(t->environment) jx_insert(jtask,jx_string("environment"),jx_copy(t->environment));
	if(t->resources) jx_insert(jtask,jx_string("resources"),ds_resources_to_jx(t->resources));
	if(t->mounts) jx_insert(jtask,jx_string("namespace"),ds_mounts_to_jx(t->mounts));
	jx_insert_string(jtask,"state",ds_task_state_string(t->state));
	return jtask;
}

int ds_task_to_file( struct ds_task *t, const char *filename )
{
	struct jx *jtask = ds_task_to_jx(t);
	if(!jtask) return 0;

	FILE *file = fopen(filename,"w");
	if(!file) {
		jx_delete(jtask);
		return 0;
	}

	jx_print_stream(jtask,file);

	jx_delete(jtask);
	fclose(file);

	return 1;
}

void ds_task_delete( struct ds_task *t )
{
	if(!t) return;
	if(t->command) free(t->command);
	if(t->taskid) free(t->taskid);
	ds_resources_delete(t->resources);
	ds_mount_delete(t->mounts);
	set_delete(t->subscribers);
	free(t);
}
