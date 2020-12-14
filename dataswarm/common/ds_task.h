#ifndef DATASWARM_TASK_H
#define DATASWARM_TASK_H

#include "ds_mount.h"
#include "ds_resources.h"
#include "jx.h"
#include "set.h"

typedef enum {
	DS_TASK_ACTIVE,     /* Task definition has been completed and task can be dispatched/executed. */
	DS_TASK_DONE,       /* Task has either completed, or has a permanent error. */
	DS_TASK_DELETING,   /* Task is being deleted at workers. */
	DS_TASK_DELETED,    /* Task has been deleted at workers. */
} ds_task_state_t;

typedef enum {
	DS_TASK_RESULT_UNDEFINED, /* Task has not reached the DS_TASK_DONE state. */
	DS_TASK_RESULT_SUCCESS,   /* Task executed to completion. (Does not mean it executed succesfully.) */
	DS_TASK_RESULT_ERROR,     /* Task cannot be executed as defined or has a permanent error (e.g., missing inputs) */
} ds_task_result_t;


struct ds_task {
	char *command;
	char *taskid;

	ds_task_state_t state;
	ds_task_result_t result;

	/* 0 for task definitions, 1 for attempts at workers. */
	int is_try;

	struct ds_mount *mounts;
	struct ds_resources *resources;
	struct jx *environment;

	struct ds_task_attempt *attempts;

	// only used on the manager
	struct ds_worker_rep *worker;
	struct set *subscribers; // set of struct (ds_client_rep *)
};

struct ds_task * ds_task_create( struct jx *jtask );
struct ds_task * ds_task_create_from_file( const char *filename );

struct jx * ds_task_to_jx( struct ds_task *task );
int ds_task_to_file( struct ds_task *task, const char *filename );

const char *ds_task_state_string( ds_task_state_t state );
void ds_task_delete( struct ds_task *t );

#endif
