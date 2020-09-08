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

void dataswarm_task_delete( struct dataswarm_task *t )
{
	if(!t) return 0;
	dataswarm_resources_delete(t->resources);
	dataswarm_mounts_delete(t->mounts);
	jx_delete(t->jtask);
	free(t);
}
